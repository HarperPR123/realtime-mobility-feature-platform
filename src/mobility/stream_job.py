import os
from pyspark.sql import SparkSession,functions as F, types as T

EVENT_SCHEMA = T.StructType([
    T.StructField("schema_version",T.IntegerType()),
    T.StructField("event_id",T.StringType()),
    T.StructField("run_id",T.StringType()),
    T.StructField("zone_id",T.LongType()),
    T.StructField("event_timestamp",T.StringType()),
    T.StructField("published_at", T.StringType()),
    T.StructField("source_dropoff_at", T.StringType()),
    T.StructField("trip_distance", T.DoubleType()),
    T.StructField("fare_amount", T.DoubleType()),
    T.StructField("duration_s", T.DoubleType())
])

def parse_events(raw):
    parsed = raw.select(
        F.col('value').cast('string').alias('raw_json'),
        F.from_json(F.col('value').cast("string"),EVENT_SCHEMA).alias('event')).select('raw_json','event.*')
    return parsed.withColumn('event_timestamp',F.to_timestamp('event_timestamp')).withColumn('published_at',F.to_timestamp('published_at'))



def mark_valid(parsed):
    rule = (
        (F.col("schema_version") == 1)
        & F.col("event_id").isNotNull()
        & (F.length("event_id") > 0)
        & F.col("zone_id").between(1, 265)
        & F.col("event_timestamp").isNotNull()
        & F.col("published_at").isNotNull()
        & F.col("trip_distance").between(0.01, 200)
        & F.col("fare_amount").between(0, 1000)
        & F.col("duration_s").between(1, 21600)
    )

    return parsed.withColumn('is_valid',F.coalesce(rule,F.lit(False)))




def build_features(valid):
    return valid.select(
        "event_id", "run_id", "zone_id", "event_timestamp",
        F.col("trip_distance").alias("last_trip_distance"),
        F.col("fare_amount").alias("last_trip_fare"),
        F.col("duration_s").alias("last_trip_duration_s"),
        (F.col("fare_amount") / F.col("trip_distance")).alias("last_fare_per_mile"),
        F.col("published_at").cast("double").alias("published_at_unix"),
    )
