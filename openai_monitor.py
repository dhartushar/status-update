"""
OpenAI Status Monitor — Async, event-driven + HTTP server for Railway.

This script runs BOTH:
  1. The async status monitor (background task)
  2. A lightweight aiohttp web server on $PORT (Railway sets this automatically)

Visit your Railway public URL to see:
  GET /          → live HTML dashboard of all detected incidents
  GET /events    → SSE stream — browser/clients receive events in real time
  GET /health    → JSON health check
"""

import asyncio
import json
import os
from datetime import datetime

import aiohttp
from aiohttp import web

# ── Config ────────────────────────────────────────────────────────────────────
PORT = int(os.environ.get("PORT", 8080))  # Railway injects $PORT automatically

MONITORS = [
    {
        "name": "OpenAI",
        "url": "https://status.openai.com/proxy/status.openai.com/",
        "interval": 30,
    },
]

# ── Shared state (written by monitor, read by web server) ─────────────────────
app_state = {
    "events": [],          # list of dicts: {time, product, status, raw}
    "sse_queues": [],      # one asyncio.Queue per connected SSE client
    "last_poll": None,
    "monitor_status": "starting",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

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


def emit_event(product: str, status: str, raw: dict):
    """Log event to console, store in memory, and push to all SSE clients."""
    now = ts()
    print(f"[{now}] Product: {product} Status: {status}")
    # print(f"[{now}] RAW: {json.dumps(raw, indent=2)}")

    event = {"time": now, "product": product, "status": status, "raw": raw}
    app_state["events"].append(event)
    app_state["events"] = app_state["events"][-200:]  # keep last 200

    # Push to all connected SSE clients
    payload = json.dumps({"time": now, "product": product, "status": status})
    for q in list(app_state["sse_queues"]):
        q.put_nowait(payload)


# ── Monitor coroutine ─────────────────────────────────────────────────────────

async def fetch(session: aiohttp.ClientSession, url: str) -> dict:
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        resp.raise_for_status()
        return await resp.json(content_type=None)


async def monitor(provider: dict):
    name = provider["name"]
    url = provider["url"]
    interval = provider["interval"]

    seen_incident_ids: set = set()
    seen_affected_ids: set = set()
    first_run = True

    print(f"[{ts()}] [{name}] Monitor started. Polling every {interval}s.")
    app_state["monitor_status"] = "running"

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                data = await fetch(session, url)
                summary = data.get("summary", {})
                affected = summary.get("affected_components", [])
                incidents = summary.get("ongoing_incidents", [])
                name_map = build_name_map(summary)
                app_state["last_poll"] = ts()

                if first_run:
                    # print(f"[{ts()}] [{name}] STARTUP — full summary snapshot:")
                    # print(json.dumps(summary, indent=2))
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

                # New affected components
                for comp in affected:
                    cid = comp.get("id") or comp.get("component_id")
                    if cid and cid not in seen_affected_ids:
                        seen_affected_ids.add(cid)
                        product = resolve_name(comp, name_map)
                        status = comp.get("status", "Degraded").replace("_", " ").title()
                        emit_event(product, status, comp)

                current_affected_ids = {c.get("id") or c.get("component_id") for c in affected}
                seen_affected_ids &= current_affected_ids

                # New incidents
                for incident in incidents:
                    iid = incident.get("id")
                    if iid and iid not in seen_incident_ids:
                        seen_incident_ids.add(iid)
                        status = incident.get("status", "Investigating").replace("_", " ").title()
                        components = incident.get("components", [])
                        if components:
                            for comp in components:
                                product = resolve_name(comp, name_map)
                                emit_event(product, status, incident)
                        else:
                            emit_event(incident.get("name", "Unknown"), status, incident)

                current_incident_ids = {i.get("id") for i in incidents}
                seen_incident_ids &= current_incident_ids

            except aiohttp.ClientError as e:
                print(f"[{ts()}] [{name}] Network error: {e}")
            except Exception as e:
                print(f"[{ts()}] [{name}] Error: {e}")

            await asyncio.sleep(interval)


# ── Web handlers ──────────────────────────────────────────────────────────────

async def handle_health(request):
    return web.json_response({
        "status": "ok",
        "monitor": app_state["monitor_status"],
        "last_poll": app_state["last_poll"],
        "total_events": len(app_state["events"]),
    })


async def handle_dashboard(request):
    events_html = ""
    for e in reversed(app_state["events"]):
        events_html += f"""
        <tr>
            <td>{e['time']}</td>
            <td><strong>{e['product']}</strong></td>
            <td><span class="status">{e['status']}</span></td>
        </tr>"""

    if not events_html:
        events_html = "<tr><td colspan='3' style='text-align:center;color:#888'>No incidents detected yet — all systems operational ✅</td></tr>"

    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>OpenAI Status Monitor</title>
    <meta charset="utf-8">
    <style>
        body {{ font-family: monospace; background: #0d1117; color: #c9d1d9; margin: 40px; }}
        h1 {{ color: #58a6ff; }}
        .meta {{ color: #8b949e; margin-bottom: 24px; font-size: 13px; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th {{ text-align: left; padding: 10px; border-bottom: 1px solid #30363d; color: #8b949e; }}
        td {{ padding: 10px; border-bottom: 1px solid #21262d; }}
        .status {{ background: #da3633; padding: 2px 8px; border-radius: 4px; font-size: 12px; }}
        #live {{ color: #3fb950; font-size: 12px; }}
    </style>
</head>
<body>
    <h1>🔴 OpenAI Status Monitor</h1>
    <div class="meta">
        Last poll: {app_state['last_poll'] or 'pending...'} &nbsp;|&nbsp;
        Monitor: {app_state['monitor_status']} &nbsp;|&nbsp;
        <span id="live">● LIVE</span>
    </div>
    <table>
        <thead><tr><th>Time</th><th>Product</th><th>Status</th></tr></thead>
        <tbody id="tbody">{events_html}</tbody>
    </table>
    <script>
        // SSE: auto-prepend new events without page refresh
        const es = new EventSource('/events');
        es.onmessage = e => {{
            const d = JSON.parse(e.data);
            const row = `<tr><td>${{d.time}}</td><td><strong>${{d.product}}</strong></td><td><span class="status">${{d.status}}</span></td></tr>`;
            document.getElementById('tbody').insertAdjacentHTML('afterbegin', row);
        }};
    </script>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


async def handle_sse(request):
    """SSE endpoint — browser connects once, receives pushed events in real time."""
    response = web.StreamResponse()
    response.headers["Content-Type"] = "text/event-stream"
    response.headers["Cache-Control"] = "no-cache"
    response.headers["X-Accel-Buffering"] = "no"
    await response.prepare(request)

    q = asyncio.Queue()
    app_state["sse_queues"].append(q)
    print(f"[{ts()}] SSE client connected ({len(app_state['sse_queues'])} total)")

    try:
        while True:
            try:
                payload = await asyncio.wait_for(q.get(), timeout=25)
                await response.write(f"data: {payload}\n\n".encode())
            except asyncio.TimeoutError:
                await response.write(b": keepalive\n\n")
    except (ConnectionResetError, asyncio.CancelledError):
        pass
    finally:
        app_state["sse_queues"].remove(q)
        print(f"[{ts()}] SSE client disconnected ({len(app_state['sse_queues'])} remaining)")

    return response


# ── App setup ─────────────────────────────────────────────────────────────────

async def start_background_monitor(app):
    for provider in MONITORS:
        asyncio.create_task(monitor(provider))


def main():
    app = web.Application()
    app.router.add_get("/", handle_dashboard)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/events", handle_sse)
    app.on_startup.append(start_background_monitor)

    print(f"[{ts()}] Starting web server on port {PORT}")
    web.run_app(app, host="0.0.0.0", port=PORT, print=None)


if __name__ == "__main__":
    main()