import json
from pathlib import Path
from urllib.request import urlretrieve

import pandas as pd
import pyarrow.parquet as pq

URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
RAW = Path("/app/data/raw/yellow_tripdata_2025-01.parquet")
OUT = Path("/app/data/prepared/yellow_2025_01.parquet")


def main():
    RAW.parent.mkdir(parents=True, exist_ok=True)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    if not RAW.exists():
        urlretrieve(URL, RAW)
    # 单月准备阶段使用 Pandas；多月改 Spark batch，不一次装进内存。
    columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime",
               "DOLocationID", "trip_distance", "fare_amount"]
    df = pd.read_parquet(RAW, columns=columns)
    df["source_row_id"] = df.index.astype("int64")
    start = pd.to_datetime(df["tpep_pickup_datetime"])
    end = pd.to_datetime(df["tpep_dropoff_datetime"])
    duration = (end - start).dt.total_seconds()
    good = (
        (end >= pd.Timestamp("2025-01-01"))
        & (end < pd.Timestamp("2025-02-01"))
        & df["DOLocationID"].between(1, 265)
        & df["trip_distance"].between(0.01, 200)
        & df["fare_amount"].between(0, 1000)
        & duration.between(1, 21600)
    )
    clean = df.loc[good].copy()
    clean["duration_s"] = duration.loc[good].astype("float64")
    # January 不跨 DST 切换；多月需明确处理 ambiguous/nonexistent。
    clean["source_dropoff_at"] = (
        end.loc[good].dt.tz_localize("America/New_York")
        .dt.tz_convert("UTC")
    )
    clean = clean.rename(columns={"DOLocationID": "zone_id"})
    clean = clean[["source_row_id", "zone_id", "trip_distance",
                   "fare_amount", "duration_s", "source_dropoff_at"]]
    clean.to_parquet(OUT, index=False)
    report = {
        "source_url": URL,
        "source_rows": pq.ParquetFile(RAW).metadata.num_rows,
        "valid_rows": len(clean),
        "rejected_rows": int((~good).sum()),
        "source_bytes": RAW.stat().st_size,
    }
    Path("/app/data/prepared/manifest.json").write_text(json.dumps(report, indent=2))
    print(report)


if __name__ == "__main__":
    main()
