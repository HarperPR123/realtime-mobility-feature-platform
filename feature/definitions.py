from datetime import timedelta

from feast import Entity, FeatureService, FeatureView, Field, FileSource, PushSource, ValueType
from feast.types import Float64

zone = Entity(name="zone", join_keys=["zone_id"], value_type=ValueType.INT64)

history = FileSource(
    name="zone_trip_history",
    path="/app/data/offline/zone_trip_history.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

push = PushSource(name="zone_trip_push", batch_source=history)

zone_trip_stats = FeatureView(
    name="zone_trip_stats",
    entities=[zone],
    ttl=timedelta(days=1),
    schema=[
        Field(name="last_trip_distance", dtype=Float64),
        Field(name="last_trip_fare", dtype=Float64),
        Field(name="last_trip_duration_s", dtype=Float64),
        Field(name="last_fare_per_mile", dtype=Float64),
        Field(name="published_at_unix", dtype=Float64),
    ],
    source=push,
    online=True
)

mobility_v1 = FeatureService(name="mobility_v1", features=[zone_trip_stats])
