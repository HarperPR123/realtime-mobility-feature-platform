# Compacts and snapshots offline feature history from streaming output.

import os
from pathlib import Path

import pandas as pd

HISTORY = Path('/app/data/history/chunks')
SNAPSHOT = Path('/app/data/offline/zone_trip')
FLOAT_COLUMNS = ['last_trip_distance','last_trip_fare','last_trip_duration_s','last_fare_per_mile','published_at_unix']


def empty_history():
    frame = pd.DataFrame({
        "event_id": pd.Series(dtype="str"),
        "run_id": pd.Series(dtype="str"),
        "zone_id": pd.Series(dtype="int64"),
        "event_timestamp": pd.Series(dtype="datetime64[ns, UTC]"),
        "created_timestamp": pd.Series(dtype="datetime64[ns, UTC]")
    })

    for name in FLOAT_COLUMNS:
        frame[name] = pd.Series(dtype = "float64")
    return frame



def compact():
    HISTORY.mkdir(parents=True,exist_ok=True)
    SNAPSHOT.parent.mkdir(parents=True,exist_ok=True)
    files = sorted(HISTORY.glob("*.parquet"))

    if files:
        frame = pd.concat([pd.read_parquet(path) for path in files],ignore_index=True)
        frame = frame.sort_values("created_timestamp").drop_duplicates("event_id",keep= "first")
        frame = frame.sort_values(["zone_id","event_timestamp","event_id"])
    else:
        frame = empty_history()
    temporary = SNAPSHOT.with_suffix(".tmp")
    frame.to_parquet(temporary,index=False)
    os.replace(temporary,SNAPSHOT)
    print({"input_files":len(files),"distinct_events":len(frame)})
    return len(frame)


if __name__ == "__main__":
    compact()

