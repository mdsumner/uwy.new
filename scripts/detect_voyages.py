#!/usr/bin/env python3
"""
Detect Nuyina voyages from underway parquet data.

Outputs voyages_draft.json with auto-detected port visits and voyage groupings.
"""

import json
import math
from datetime import datetime, timezone
from dataclasses import dataclass

try:
    import duckdb
except ImportError:
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "duckdb", "-q"])
    import duckdb


# ---- Port definitions ----
PORTS = [
    {"name": "Hobart", "lat": -42.88, "lon": 147.33, "radius_km": 15},
    {"name": "Burnie", "lat": -41.05, "lon": 145.91, "radius_km": 8},
    {"name": "Macquarie Island", "lat": -54.50, "lon": 158.94, "radius_km": 40},
    {"name": "Heard Island", "lat": -53.10, "lon": 73.51, "radius_km": 50},
    {"name": "Casey", "lat": -66.28, "lon": 110.53, "radius_km": 80},
    {"name": "Davis", "lat": -68.58, "lon": 77.97, "radius_km": 80},
    {"name": "Mawson", "lat": -67.60, "lon": 62.87, "radius_km": 80},
]

PARQUET_URL = "https://github.com/mdsumner/uwy.new/releases/download/v0.0.1/nuyina_underway.parquet"
MIN_DWELL_HOURS = 2  # ignore brief port touches


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate great-circle distance in km."""
    R = 6371
    to_rad = math.pi / 180
    d_lat = (lat2 - lat1) * to_rad
    d_lon = (lon2 - lon1) * to_rad
    a = (math.sin(d_lat / 2) ** 2 + 
         math.cos(lat1 * to_rad) * math.cos(lat2 * to_rad) * math.sin(d_lon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def detect_port(lat: float, lon: float) -> str | None:
    """Return port name if within radius, else None."""
    best_port = None
    best_dist = float("inf")
    
    for port in PORTS:
        dist = haversine_km(lat, lon, port["lat"], port["lon"])
        if dist < best_dist:
            best_dist = dist
            if dist <= port["radius_km"]:
                best_port = port["name"]
    
    return best_port


def load_data() -> list[dict]:
    """Load parquet and add port detection."""
    print(f"Loading data from {PARQUET_URL}...")
    
    con = duckdb.connect()
    df = con.sql(f"""
        SELECT gml_id, datetime, latitude, longitude
        FROM '{PARQUET_URL}'
        WHERE datetime >= '2020-01-01'
        ORDER BY datetime
    """).fetchdf()
    
    print(f"Loaded {len(df)} records")
    
    # Detect ports (vectorized would be faster but this is clear)
    print("Detecting port visits...")
    records = []
    for _, row in df.iterrows():
        records.append({
            "gml_id": row["gml_id"],
            "datetime": row["datetime"],
            "lat": row["latitude"],
            "lon": row["longitude"],
            "port": detect_port(row["latitude"], row["longitude"])
        })
    
    return records


def group_port_visits(records: list[dict]) -> list[dict]:
    """Group consecutive records at same port into visits."""
    if not records:
        return []
    
    visits = []
    current_port = None
    current_start = None
    current_records = []
    
    for rec in records:
        port = rec["port"]
        
        if port != current_port:
            # End previous visit if it was at a port
            if current_port is not None and current_records:
                visits.append({
                    "port": current_port,
                    "arrive": current_records[0]["datetime"],
                    "depart": current_records[-1]["datetime"],
                    "arrive_gml_id": current_records[0]["gml_id"],
                    "depart_gml_id": current_records[-1]["gml_id"],
                    "n_points": len(current_records),
                })
            
            # Start new segment
            current_port = port
            current_records = [rec] if port else []
        else:
            if port:
                current_records.append(rec)
    
    # Don't forget the last visit
    if current_port is not None and current_records:
        visits.append({
            "port": current_port,
            "arrive": current_records[0]["datetime"],
            "depart": current_records[-1]["datetime"],
            "arrive_gml_id": current_records[0]["gml_id"],
            "depart_gml_id": current_records[-1]["gml_id"],
            "n_points": len(current_records),
        })
    
    # Calculate dwell time and filter
    filtered = []
    for v in visits:
        dwell = (v["depart"] - v["arrive"]).total_seconds() / 3600
        if dwell >= MIN_DWELL_HOURS:
            v["dwell_hours"] = round(dwell, 1)
            filtered.append(v)
    
    print(f"Found {len(filtered)} port visits (>= {MIN_DWELL_HOURS}h dwell)")
    return filtered


def group_voyages(visits: list[dict]) -> list[dict]:
    """Group visits into voyages (split on Hobart departures)."""
    if not visits:
        return []
    
    voyages = []
    current_stops = []
    voyage_num = 0
    
    for i, visit in enumerate(visits):
        # New voyage starts when arriving at Hobart after being elsewhere
        is_new_voyage = (
            visit["port"] == "Hobart" and 
            i > 0 and 
            visits[i - 1]["port"] != "Hobart"
        )
        
        if is_new_voyage and current_stops:
            # Save previous voyage
            voyage_num += 1
            voyages.append({
                "id": f"V{voyage_num} {current_stops[0]['arrive'].strftime('%Y-%m')}",
                "note": "",
                "start": current_stops[0]["arrive"],
                "end": current_stops[-1]["depart"],
                "stops": current_stops,
            })
            current_stops = []
        
        current_stops.append(visit)
    
    # Don't forget last voyage (possibly in progress)
    if current_stops:
        voyage_num += 1
        voyages.append({
            "id": f"V{voyage_num} {current_stops[0]['arrive'].strftime('%Y-%m')}",
            "note": "",
            "start": current_stops[0]["arrive"],
            "end": current_stops[-1]["depart"],
            "stops": current_stops,
        })
    
    print(f"Grouped into {len(voyages)} voyages")
    return voyages


def format_datetime(dt) -> str:
    """Convert datetime to ISO string."""
    if hasattr(dt, "isoformat"):
        return dt.isoformat().replace("+00:00", "Z")
    return str(dt)


def build_output(voyages: list[dict]) -> dict:
    """Build the output JSON structure."""
    
    # Format ports
    ports_dict = {p["name"]: {"lat": p["lat"], "lon": p["lon"], "radius_km": p["radius_km"]} 
                  for p in PORTS}
    
    # Format voyages
    voyages_out = []
    for v in voyages:
        stops_out = []
        for s in v["stops"]:
            stops_out.append({
                "port": s["port"],
                "arrive": format_datetime(s["arrive"]),
                "depart": format_datetime(s["depart"]),
                "arrive_gml_id": s["arrive_gml_id"],
                "depart_gml_id": s["depart_gml_id"],
                "dwell_hours": s["dwell_hours"],
            })
        
        voyages_out.append({
            "id": v["id"],
            "note": v["note"],
            "start": format_datetime(v["start"]),
            "end": format_datetime(v["end"]),
            "stops": stops_out,
        })
    
    return {
        "_generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_note": "Auto-detected draft - review and edit before publishing",
        "ports": ports_dict,
        "voyages": voyages_out,
    }


def main():
    print("=== Nuyina Voyage Detection ===")
    
    records = load_data()
    visits = group_port_visits(records)
    voyages = group_voyages(visits)
    output = build_output(voyages)
    
    # Write output
    output_file = "voyages_draft.json"
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)
    
    print(f"\nWritten to {output_file}")
    print(f"  {len(output['voyages'])} voyages")
    print(f"  {sum(len(v['stops']) for v in output['voyages'])} total stops")
    
    # Summary
    print("\nVoyage summary:")
    for v in output["voyages"]:
        stops = ", ".join(s["port"] for s in v["stops"])
        print(f"  {v['id']}: {stops}")


if __name__ == "__main__":
    main()
