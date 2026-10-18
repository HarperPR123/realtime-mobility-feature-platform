def build_event(row, run_id, now):
    return {
        "schema_version": 1,
        "event_id": f"{run_id}:yellow_2025_01:{row['source_row_id']}",
        "run_id": run_id,
        "zone_id": int(row["zone_id"]),
        "event_timestamp": now,
        "published_at": now,
        "source_dropoff_at": row["source_dropoff_at"].isoformat(),
        "trip_distance": float(row["trip_distance"]),
        "fare_amount": float(row["fare_amount"]),
        "duration_s": float(row["duration_s"]),
    }
