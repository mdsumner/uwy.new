#!/usr/bin/env python3
"""
Update Nuyina underway parquet cache from AAD WFS.

Uses DuckDB for efficient parquet operations and WFS reading via GDAL.
No R dependency required.
"""

import os
import sys
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("Installing duckdb...")
    os.system(f"{sys.executable} -m pip install duckdb -q")
    import duckdb


WFS_BASE = "https://data.aad.gov.au/geoserver/underway/ows"
WFS_PARAMS = {
    "service": "WFS",
    "version": "2.0.0",
    "request": "GetFeature",
    "typeName": "underway:nuyina_underway",
    "outputFormat": "csv",
}
RELEASE_URL = "https://github.com/mdsumner/uwy.new/releases/download/v0.0.1/nuyina_underway.parquet"
OUTPUT_FILE = "nuyina_underway.parquet"


def log(msg: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{ts}] {msg}")


def build_wfs_url(cql_filter: str = None) -> str:
    """Build WFS GetFeature URL with optional CQL filter."""
    from urllib.parse import urlencode
    params = WFS_PARAMS.copy()
    if cql_filter:
        params["cql_filter"] = cql_filter
    return f"{WFS_BASE}?{urlencode(params)}"


def download_existing() -> bool:
    """Download existing parquet from release."""
    log("Downloading existing parquet from release...")
    try:
        con = duckdb.connect()
        # Check if remote file exists and is valid
        count = con.sql(f"SELECT COUNT(*) FROM '{RELEASE_URL}'").fetchone()[0]
        if count > 0:
            # Download and save locally
            con.sql(f"""
                COPY (SELECT * FROM '{RELEASE_URL}')
                TO '{OUTPUT_FILE}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
            log(f"Downloaded {count} records from release")
            return True
    except Exception as e:
        log(f"Could not download existing: {e}")
    return False


def get_max_datetime() -> str | None:
    """Get max datetime from existing parquet."""
    if not Path(OUTPUT_FILE).exists():
        return None
    try:
        con = duckdb.connect()
        result = con.sql(f"SELECT MAX(datetime) FROM '{OUTPUT_FILE}'").fetchone()
        if result and result[0]:
            dt = result[0]
            if hasattr(dt, 'isoformat'):
                return dt.isoformat()
            return str(dt)
    except Exception as e:
        log(f"Could not get max datetime: {e}")
    return None


def fetch_from_wfs(since: str = None) -> int:
    """Fetch data from WFS, optionally since a datetime."""
    con = duckdb.connect()
    
    if since:
        # Incremental: fetch only new records
        cql = f"datetime > '{since}'"
        url = build_wfs_url(cql)
        log(f"Fetching incremental data since {since}...")
    else:
        # Full fetch
        url = build_wfs_url()
        log("Fetching full dataset from WFS...")
    
    try:
        # Read from WFS CSV endpoint
        new_data = con.sql(f"SELECT * FROM read_csv_auto('{url}')").fetchdf()
        return len(new_data), new_data
    except Exception as e:
        log(f"WFS fetch error: {e}")
        return 0, None


def update_parquet():
    """Main update logic with atomic operations."""
    backup_file = f"{OUTPUT_FILE}.bak"
    temp_file = f"{OUTPUT_FILE}.tmp"
    
    try:
        # Try to get existing data
        has_existing = download_existing()
        
        if has_existing:
            max_dt = get_max_datetime()
            if max_dt:
                count, new_df = fetch_from_wfs(since=max_dt)
                
                if count == 0:
                    log("No new records found")
                    return True
                
                log(f"Found {count} new records, merging...")
                
                # Backup existing
                shutil.copy2(OUTPUT_FILE, backup_file)
                
                # Merge using DuckDB
                con = duckdb.connect()
                try:
                    # Read existing, union with new, write to temp
                    con.sql(f"""
                        COPY (
                            SELECT * FROM '{OUTPUT_FILE}'
                            UNION ALL
                            SELECT * FROM new_df
                        )
                        TO '{temp_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
                    """)
                    
                    # Validate
                    total = con.sql(f"SELECT COUNT(*) FROM '{temp_file}'").fetchone()[0]
                    if total > 0:
                        # Atomic replace
                        os.replace(temp_file, OUTPUT_FILE)
                        os.remove(backup_file)
                        log(f"Merge successful. Total records: {total}")
                        return True
                    else:
                        raise ValueError("Merged file has no records")
                        
                except Exception as e:
                    log(f"Merge failed: {e}, restoring backup")
                    if Path(backup_file).exists():
                        shutil.move(backup_file, OUTPUT_FILE)
                    return False
            else:
                log("Could not determine max datetime, falling back to full fetch")
                has_existing = False
        
        if not has_existing:
            # Full fetch
            count, df = fetch_from_wfs()
            if count > 0:
                con = duckdb.connect()
                con.sql(f"""
                    COPY (SELECT * FROM df)
                    TO '{OUTPUT_FILE}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """)
                log(f"Full fetch successful. Total records: {count}")
                return True
            else:
                log("ERROR: No data retrieved from WFS")
                return False
                
    finally:
        # Cleanup
        for f in [temp_file, backup_file]:
            try:
                if Path(f).exists():
                    os.remove(f)
            except:
                pass


def main():
    log("Starting underway data update...")
    
    success = update_parquet()
    
    if success and Path(OUTPUT_FILE).exists():
        size = Path(OUTPUT_FILE).stat().st_size
        con = duckdb.connect()
        count = con.sql(f"SELECT COUNT(*) FROM '{OUTPUT_FILE}'").fetchone()[0]
        log(f"SUCCESS: {count} records, {size:,} bytes")
        return 0
    else:
        log("FAILED: Update unsuccessful")
        return 1


if __name__ == "__main__":
    sys.exit(main())
