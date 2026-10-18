import json
from pathlib import Path
import pyarrow.parquet as pq


def main():
    path = Path("/app/data/prepared/yellow_2025_01.parquet")
    row = next(pq.ParquetFile(path).iter_batches(batch_size=1)).to_pylist()[0]
    evidence = {
        "source_row": row,
        "expected_features": {
            "last_trip_distance": float(row["trip_distance"]),
            "last_trip_fare": float(row["fare_amount"]),
            "last_trip_duration_s": float(row["duration_s"]),
            "last_fare_per_mile": float(row["fare_amount"]) / float(row["trip_distance"]),
        },
    }
    target = Path("/app/data/evidence/anchor.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(evidence, default=str, indent=2))
    print(json.dumps(evidence, default=str, indent=2))


if __name__ == "__main__":
    main()
