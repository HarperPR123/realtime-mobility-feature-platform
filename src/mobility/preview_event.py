import json
from datetime import datetime, timezone
from pathlib import Path
import pyarrow.parquet as pq
from mobility.events import build_event


def main():
    row = next(pq.ParquetFile("/app/data/prepared/yellow_2025_01.parquet")
               .iter_batches(batch_size=1)).to_pylist()[0]
    print(row)
    event = build_event(row, "data-first-001", datetime.now(timezone.utc).isoformat())
    path = Path("/app/data/evidence/event_preview.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(event, indent=2))
    print(json.dumps(event, indent=2))
    print("Kafka key =", str(event["zone_id"]))


if __name__ == "__main__":
    main()
