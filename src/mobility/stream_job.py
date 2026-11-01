# Spark streaming job that parses, validates, and emits trip feature updates.

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

def main():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "broker:29092")
    spark = (SparkSession
             .builder
             .appName('mobillity-features-v1')
             .config("spark.sql.session.timeZone",'UTC')
             .config('spark.sql.shuffle.partitions','3')
             .getOrCreate()

    )
    spark.sparkContext.setLogLevel('WARN')
    raw = (spark
           .readStream
           .format('kafka')
           .option('kafka.bootstrap.servers',bootstrap)
           .option('subscribe','trip-events-v1')
           .option('startingOffsets','earliest')
           .option('maxOffsetsPerTrigger',5000)
           .option('failOnDataLoss','true')
           .load()
    )

    checked = mark_valid(parse_events(raw))
    features = build_features(checked.where(F.col("is_valid")))

    feature_messages = features.select(
        F.col('zone_id').cast('string').alias('key'),
        F.to_json(F.struct(*[F.col(c) for c in features.columns]),
                  options = {"timestampFormat":"yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"}).alias('value')
    )

    feature_query = (
        feature_messages
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("topic","zone-trip-features-v1")
        .option("checkpointLocation","/app/data/checkpoints/features-v1")
        .outputMode('append')
        .trigger(processingTime= '5 seconds')
        .start()
    )

    bronze = raw.selectExpr(
        'CAST(value AS STRING) AS raw_string',
        'topic',
        'partition',
        'offset',
        'timestamp'
    )

    bronze_query = (
        bronze
        .writeStream
        .format('parquet')
        .option('path','/app/data/raw/events')
        .option('checkpointLocation','/app/data/checkpoints/bronze-v1')
        .outputMode('append')
        .trigger(processingTime='10 seconds')
        .start()
    )

    invalid = (
    checked
    .where(~F.col("is_valid"))
    .select(
        F.coalesce(
            F.col("zone_id").cast("string"),
            F.lit("unknown")
        ).alias("key"),
        F.to_json(
            F.struct(
                F.col("raw_json"),
                F.lit("schema_or_business_rule_invalid").alias("reason")
            )
        ).alias("value")
    )
    )
    dlq_query = (
        invalid
        .writeStream
        .format('kafka')
        .option("kafka.bootstrap.servers", bootstrap)
        .option('topic','invalid-trip-events-v1')
        .option('checkpointLocation','/app/data/checkpoints/dlq-v1')
        .outputMode('append')
        .trigger(processingTime = '5 seconds')
        .start()
    )

    print('started queries',feature_query.id,bronze_query.id,dlq_query.id)
    spark.streams.awaitAnyTermination()






if __name__=='__main__':
    main()





