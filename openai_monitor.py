"""
OpenAI Status Monitor — Async, event-driven architecture.

WHY THIS APPROACH:
- The OpenAI status page (Instatus) does not expose a public SSE/WebSocket stream.
- The correct approach is async polling with change-detection:
  each poll result is treated as an "event" — output is only triggered on state change.
"""

import asyncio
import json
from datetime import datetime

import aiohttp

# ── Add more status pages here to monitor 100+ providers concurrently ──────────
MONITORS = [
    {
        "name": "OpenAI",
        "url": "https://status.openai.com/proxy/status.openai.com/",
        "interval": 30,  # seconds between polls
    },
]


def ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def build_name_map(summary: dict) -> dict:
    result = {}
    try:
        for item in summary.get("structure", {}).get("items", []):
            for comp in item.get("group", {}).get("components", []):
                cid = comp.get("component_id")
                if cid:
                    result[cid] = comp.get("name", "Unknown")
    except (KeyError, TypeError):
        pass
    for comp in summary.get("components", []):
        cid = comp.get("id")
        if cid and cid not in result:
            result[cid] = comp.get("name", "Unknown")
    return result


def resolve_name(component: dict, name_map: dict) -> str:
    cid = component.get("id") or component.get("component_id", "")
    return name_map.get(cid) or component.get("name", "Unknown")


async def fetch(session: aiohttp.ClientSession, url: str) -> dict:
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        resp.raise_for_status()
        return await resp.json(content_type=None)


async def monitor(provider: dict):
    """
    Async monitor for a single status page provider.
    Each call to fetch() is a lightweight coroutine — 100 of these
    run concurrently in the same event loop with no extra threads.
    """
    name = provider["name"]
    url = provider["url"]
    interval = provider["interval"]

    seen_incident_ids: set = set()
    seen_affected_ids: set = set()
    name_map: dict = {}
    first_run = True

    print(f"[{ts()}] [{name}] Monitor started. Polling every {interval}s.")

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                data = await fetch(session, url)
                summary = data.get("summary", {})
                affected = summary.get("affected_components", [])
                incidents = summary.get("ongoing_incidents", [])
                name_map = build_name_map(summary)

                # ── On first run: log full snapshot and seed existing state ──
                if first_run:
                    # print(f"[{ts()}] [{name}] STARTUP — full summary snapshot:")
                    # print(json.dumps(summary, indent=2))
                    # Seed so we don't re-alert on already-known active incidents
                    for inc in incidents:
                        if inc.get("id"):
                            seen_incident_ids.add(inc["id"])
                            iname = inc.get("name", "Unknown")
                            status = inc.get("status", "").replace("_", " ").title()
                            print(f"[{ts()}] [{name}] [EXISTING] Product: {iname} Status: {status}")
                    for comp in affected:
                        cid = comp.get("id") or comp.get("component_id")
                        if cid:
                            seen_affected_ids.add(cid)
                    first_run = False

                # ── EVENT: new affected component ────────────────────────────
                for comp in affected:
                    cid = comp.get("id") or comp.get("component_id")
                    if cid and cid not in seen_affected_ids:
                        seen_affected_ids.add(cid)
                        product = resolve_name(comp, name_map)
                        status = comp.get("status", "Degraded").replace("_", " ").title()
                        print(f"[{ts()}] Product: {product} Status: {status}")
                        print(f"[{ts()}] RAW affected_component: {json.dumps(comp, indent=2)}")

                # Clear recovered components so we re-alert if they degrade again
                current_affected_ids = {
                    c.get("id") or c.get("component_id") for c in affected
                }
                seen_affected_ids &= current_affected_ids

                # ── EVENT: new incident ──────────────────────────────────────
                for incident in incidents:
                    iid = incident.get("id")
                    if iid and iid not in seen_incident_ids:
                        seen_incident_ids.add(iid)
                        iname = incident.get("name", "Unknown")
                        status = incident.get("status", "Investigating").replace("_", " ").title()
                        print(f"[{ts()}] RAW ongoing_incident: {json.dumps(incident, indent=2)}")
                        components = incident.get("components", [])
                        if components:
                            for comp in components:
                                product = resolve_name(comp, name_map)
                                print(f"[{ts()}] Product: {product} Status: {status}")
                        else:
                            print(f"[{ts()}] Product: {iname} Status: {status}")

                # Clear resolved incidents
                current_incident_ids = {i.get("id") for i in incidents}
                seen_incident_ids &= current_incident_ids

            except aiohttp.ClientError as e:
                print(f"[{ts()}] [{name}] Network error: {e}")
            except Exception as e:
                print(f"[{ts()}] [{name}] Error: {e}")

            # Non-blocking sleep — other monitors keep running during this wait
            await asyncio.sleep(interval)


async def main():
    # Launch all monitors concurrently — adding 99 more pages costs almost nothing
    await asyncio.gather(*[monitor(p) for p in MONITORS])


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n[{ts()}] Monitor stopped.")