import requests
import json
import time
from datetime import datetime

URL = "https://status.openai.com/proxy/status.openai.com/"

state = {
    "last_affected_components": [],
    "last_ongoing_incidents": [],
    "last_poll_time": None,
}


def get_product_name_map(data):
    """Build a map of component_id -> name from structure.items."""
    name_map = {}
    try:
        items = data["summary"]["structure"]["items"]
        for item in items:
            group = item.get("group", {})
            for comp in group.get("components", []):
                cid = comp.get("component_id")
                name = comp.get("name")
                if cid and name:
                    name_map[cid] = name
    except (KeyError, TypeError):
        pass

    # Also pull from top-level components list as fallback
    try:
        for comp in data["summary"]["components"]:
            cid = comp.get("id")
            name = comp.get("name")
            if cid and name and cid not in name_map:
                name_map[cid] = name
    except (KeyError, TypeError):
        pass

    return name_map


def resolve_name(component, name_map):
    """Get human-readable product name for a component."""
    cid = component.get("id") or component.get("component_id", "")
    return name_map.get(cid) or component.get("name", "Unknown")


def timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def poll():
    try:
        resp = requests.get(URL, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        print(f"[{timestamp()}] Network error: {e}")
        return
    except json.JSONDecodeError as e:
        print(f"[{timestamp()}] JSON parse error: {e}")
        return

    summary = data.get("summary", {})
    affected = summary.get("affected_components", [])
    incidents = summary.get("ongoing_incidents", [])
    name_map = get_product_name_map(data)

    # On first poll, dump the full summary so you can inspect the real structure
    # if state["last_poll_time"] is None:
        # print(f"[{timestamp()}] STARTUP — full summary snapshot:")
        # print(json.dumps(summary, indent=2))

    # --- Detect new/changed affected_components ---
    prev_affected_ids = {
        c.get("id") or c.get("component_id") for c in state["last_affected_components"]
    }
    curr_affected_ids = {
        c.get("id") or c.get("component_id") for c in affected
    }

    new_affected_ids = curr_affected_ids - prev_affected_ids

    for comp in affected:
        cid = comp.get("id") or comp.get("component_id")
        if cid in new_affected_ids:
            name = resolve_name(comp, name_map)
            status = comp.get("status", "Degraded performance").replace("_", " ").title()
            print(f"[{timestamp()}] Product: {name} Status: {status}")
            print(f"[{timestamp()}] RAW affected_component: {json.dumps(comp, indent=2)}")

    # --- Detect new ongoing incidents ---
    prev_incident_ids = {i.get("id") for i in state["last_ongoing_incidents"]}
    for incident in incidents:
        iid = incident.get("id")
        if iid not in prev_incident_ids:
            name = incident.get("name", "Unknown Incident")
            status = incident.get("status", "Investigating").replace("_", " ").title()
            print(f"[{timestamp()}] RAW ongoing_incident: {json.dumps(incident, indent=2)}")
            # Try to tie to affected components
            inc_components = incident.get("components", [])
            if inc_components:
                for comp in inc_components:
                    product = resolve_name(comp, name_map)
                    print(f"[{timestamp()}] Product: {product} Status: {status}")
            else:
                print(f"[{timestamp()}] Incident: {name} Status: {status}")

    # --- Update state ---
    state["last_affected_components"] = affected
    state["last_ongoing_incidents"] = incidents
    state["last_poll_time"] = datetime.now()


def main():
    print(f"[{timestamp()}] OpenAI Status Monitor started. Polling every 30s. Press Ctrl+C to stop.")
    while True:
        poll()
        time.sleep(30)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n[{timestamp()}] Monitor stopped.")
