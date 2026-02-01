#!/bin/bash
set -euo pipefail

# Configuration
WFS_URL="WFS:https://data.aad.gov.au/geoserver/ows?service=wfs&version=2.0.0&request=GetCapabilities"
LAYER_NAME="underway:nuyina_underway"
RELEASE_URL="https://github.com/mdsumner/uwy.new/releases/download/v0.0.1/nuyina_underway.parquet"
OUTPUT_FILE="nuyina_underway.parquet"
TEMP_FILE="${OUTPUT_FILE}.tmp"
BACKUP_FILE="${OUTPUT_FILE}.bak"

log() {
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] $*"
}

cleanup() {
    rm -f "$TEMP_FILE" "${TEMP_FILE}.new" 2>/dev/null || true
}
trap cleanup EXIT

# Download existing parquet from release (if exists)
download_existing() {
    log "Attempting to download existing parquet from release..."
    if curl -fsSL -o "$OUTPUT_FILE" "$RELEASE_URL" 2>/dev/null; then
        log "Downloaded existing parquet ($(stat -c%s "$OUTPUT_FILE" 2>/dev/null || stat -f%z "$OUTPUT_FILE") bytes)"
        return 0
    else
        log "No existing parquet found or download failed - will create fresh"
        return 1
    fi
}

# Get the max datetime from existing parquet
get_max_datetime() {
    if [[ -f "$OUTPUT_FILE" ]]; then
        # Use ogrinfo to query max datetime
        local max_dt
        max_dt=$(ogrinfo -sql "SELECT MAX(datetime) FROM \"${OUTPUT_FILE%.parquet}\"" "$OUTPUT_FILE" 2>/dev/null | \
                 grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}' | tail -1 || echo "")
        if [[ -n "$max_dt" ]]; then
            echo "$max_dt"
            return 0
        fi
    fi
    echo ""
    return 1
}

# Fetch all data fresh (full rebuild)
fetch_full() {
    log "Fetching full dataset from WFS..."
    ogr2ogr -f Parquet "$TEMP_FILE" \
        "$WFS_URL" \
        "$LAYER_NAME" \
        -progress \
        -lco COMPRESSION=ZSTD
    
    if [[ ! -f "$TEMP_FILE" ]] || [[ ! -s "$TEMP_FILE" ]]; then
        log "ERROR: Failed to create parquet file"
        return 1
    fi
    
    local count
    count=$(ogrinfo -so "$TEMP_FILE" -sql "SELECT COUNT(*) FROM \"${TEMP_FILE%.parquet}\"" 2>/dev/null | grep -oE '[0-9]+' | tail -1 || echo "0")
    log "Fetched $count records"
    
    mv "$TEMP_FILE" "$OUTPUT_FILE"
    log "Full fetch complete"
}

# Fetch incremental updates
fetch_incremental() {
    local max_dt="$1"
    log "Fetching incremental updates since $max_dt..."
    
    # Query for new records only
    ogr2ogr -f Parquet "${TEMP_FILE}.new" \
        "$WFS_URL" \
        "$LAYER_NAME" \
        -where "datetime > '${max_dt}'" \
        -progress \
        -lco COMPRESSION=ZSTD 2>/dev/null || true
    
    # Check if we got any new records
    if [[ ! -f "${TEMP_FILE}.new" ]] || [[ ! -s "${TEMP_FILE}.new" ]]; then
        log "No new records found"
        rm -f "${TEMP_FILE}.new"
        return 0
    fi
    
    local new_count
    new_count=$(ogrinfo -so "${TEMP_FILE}.new" -sql "SELECT COUNT(*) FROM \"${TEMP_FILE%.parquet}\"" 2>/dev/null | grep -oE '[0-9]+' | tail -1 || echo "0")
    
    if [[ "$new_count" == "0" ]]; then
        log "No new records to append"
        rm -f "${TEMP_FILE}.new"
        return 0
    fi
    
    log "Found $new_count new records, merging..."
    
    # Backup existing file
    cp "$OUTPUT_FILE" "$BACKUP_FILE"
    
    # Merge: append new to existing using ogrmerge or ogr2ogr -append
    ogr2ogr -f Parquet "$TEMP_FILE" \
        "$OUTPUT_FILE" \
        -lco COMPRESSION=ZSTD
    
    ogr2ogr -f Parquet -append "$TEMP_FILE" \
        "${TEMP_FILE}.new"
    
    # Validate the merged file
    local total_count
    total_count=$(ogrinfo -so "$TEMP_FILE" -sql "SELECT COUNT(*) FROM \"${TEMP_FILE%.parquet}\"" 2>/dev/null | grep -oE '[0-9]+' | tail -1 || echo "0")
    
    if [[ "$total_count" -gt "0" ]]; then
        mv "$TEMP_FILE" "$OUTPUT_FILE"
        rm -f "$BACKUP_FILE" "${TEMP_FILE}.new"
        log "Incremental update complete. Total records: $total_count"
    else
        log "ERROR: Merge validation failed, restoring backup"
        mv "$BACKUP_FILE" "$OUTPUT_FILE"
        rm -f "$TEMP_FILE" "${TEMP_FILE}.new"
        return 1
    fi
}

main() {
    log "Starting underway data update..."
    
    # Try to download existing data
    if download_existing; then
        # Get max datetime for incremental update
        max_dt=$(get_max_datetime) || max_dt=""
        
        if [[ -n "$max_dt" ]]; then
            fetch_incremental "$max_dt"
        else
            log "Could not determine max datetime, doing full fetch"
            fetch_full
        fi
    else
        # No existing data, do full fetch
        fetch_full
    fi
    
    # Final validation
    if [[ -f "$OUTPUT_FILE" ]] && [[ -s "$OUTPUT_FILE" ]]; then
        local final_count
        final_count=$(ogrinfo -so "$OUTPUT_FILE" -sql "SELECT COUNT(*) FROM \"${OUTPUT_FILE%.parquet}\"" 2>/dev/null | grep -oE '[0-9]+' | tail -1 || echo "unknown")
        log "SUCCESS: Final file has $final_count records"
        log "File size: $(stat -c%s "$OUTPUT_FILE" 2>/dev/null || stat -f%z "$OUTPUT_FILE") bytes"
    else
        log "ERROR: No valid output file produced"
        exit 1
    fi
}

main "$@"
