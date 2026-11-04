#!/usr/bin/env python3
"""Eksportuje WSZYSTKIE dashboardy z działającej Grafany do grafana/dashboards/*.json.

Dashboardy tworzone w UI siedzą w bazie SQLite w wolumenie Dockera (grafana_data na vm5),
a NIE w repo. Ten skrypt kopiuje je do plików, które można zacommitować.

Użycie (na vm5 albo na dowolnej maszynie, która widzi Grafanę):
    python3 grafana/export_dashboards.py
    GRAFANA_URL=http://vm5:3000 GRAFANA_USER=admin GRAFANA_PASSWORD=admin python3 grafana/export_dashboards.py
Potem:  git add grafana/dashboards && git commit -m "dashboardy" && git push
"""
import base64
import json
import os
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path

BASE_URL = os.getenv("GRAFANA_URL", "http://localhost:3000").rstrip("/")
USER = os.getenv("GRAFANA_USER", "admin")
PASSWORD = os.getenv("GRAFANA_PASSWORD", "admin")
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", Path(__file__).parent / "dashboards"))


def api_get(path: str):
    request = urllib.request.Request(BASE_URL + path)
    token = base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()
    request.add_header("Authorization", f"Basic {token}")
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.load(response)


def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-") or "dashboard"


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    found = api_get("/api/search?type=dash-db&limit=5000")
    if not found:
        print("Grafana nie zwróciła żadnych dashboardów.")
        return 1
    for item in found:
        payload = api_get("/api/dashboards/uid/" + urllib.parse.quote(item["uid"]))
        dashboard = payload["dashboard"]
        dashboard["id"] = None            # id jest lokalne dla instancji; uid zostaje
        dashboard.pop("version", None)
        target = OUTPUT_DIR / f"{slugify(dashboard.get('title', item['uid']))}.json"
        target.write_text(json.dumps(dashboard, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"zapisano {target}  ({dashboard.get('title')})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
