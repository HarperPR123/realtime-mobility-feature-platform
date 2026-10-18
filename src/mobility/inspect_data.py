import hashlib
import json
from urllib.request import urlretrieve
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path



URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
RAW = Path("/app/data/raw/yellow_tripdata_2025-01.parquet")
EVIDENCE = Path("/app/data/evidence")
COLUMNS = ["tpep_pickup_datetime", "tpep_dropoff_datetime",
           "DOLocationID", "trip_distance", "fare_amount"]
def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

def main():
    RAW.parent.mkdir(parents= True,exist_ok = True)
    EVIDENCE.mkdir(parents= True,exist_ok = True)
    if not RAW.exists():
        urlretrieve(URL, RAW)
    source = pq.ParquetFile(RAW)
    batch = next(source.iter_batches(batch_size=10,columns = COLUMNS))
    rows = batch.to_pylist()
    report = {
        "source_url":URL,
        "source_bytes":RAW.stat().st_size,
         "source_rows": source.metadata.num_rows,
        "source_sha256": file_sha256(RAW),
        "schema": str(source.schema_arrow),
        "preview_rows": rows,
    }
    print("SCHEMA")
    print(source.schema_arrow)
    print("FIRST 10 ROWS")
    for index, row in enumerate(rows):
        print(index, json.dumps(row, default=str))
    print("FILE METADATA")
    print(json.dumps({k: v for k, v in report.items()
                      if k not in ("schema", "preview_rows")}, indent=2))
    (EVIDENCE / "source_preview.json").write_text(
        json.dumps(report, default=str, indent=2)
    )
    pq.write_table(pa.Table.from_batches([batch]), EVIDENCE / "raw_first10.parquet")


if __name__ == "__main__":
    main()