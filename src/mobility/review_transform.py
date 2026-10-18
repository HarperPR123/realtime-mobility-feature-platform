import json
import math
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from mobility.stream_job import parse_events, mark_valid, build_features


def main():
    evidence = Path("/app/data/evidence")
    item = json.loads((evidence / "raw_consumed.jsonl").read_text().splitlines()[0])
    anchor = json.loads((evidence / "anchor.json").read_text())
    spark = (SparkSession.builder.master("local[2]")
             .appName("review-one-real-event")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        raw = spark.createDataFrame([(json.dumps(item["event"]),)], ["value"])
        checked = mark_valid(parse_events(raw))
        checked.show(truncate=False)
        assert checked.first()["is_valid"] is True
        frame = build_features(checked.where("is_valid"))
        frame.show(truncate=False)
        actual = frame.first().asDict()
        for name, expected in anchor["expected_features"].items():
            assert math.isclose(actual[name], expected, rel_tol=1e-9, abs_tol=1e-9)
        actual = json.loads(json.dumps(actual, default=str))
        (evidence / "expected_spark_feature.json").write_text(json.dumps(actual, indent=2))
        print("PASS: observed Kafka event matches the hand-checkable anchor")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
