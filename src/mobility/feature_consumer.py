import hashlib
import json
import os
from pathlib import Path

import pandas as pd
from confluent_kafka import Consumer, TopicPartition
from feast import FeatureStore
from feast.data_source import PushMode


def main():
    store = FeatureStore(repo_path=os.getenv("FEAST_REPO", "/app/feature"))
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "broker:29092"),
        "group.id": "zone-feature-writer-v1",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "max.poll.interval.ms": 300000,
    })
    consumer.subscribe(["zone-trip-features-v1"])
    directory = Path("/app/data/history/chunks")
    directory.mkdir(parents=True, exist_ok=True)
    try:
        while True:
            messages = consumer.consume(num_messages=500, timeout=1)
            if not messages:
                continue
            rows = []
            offsets = {}
            for msg in messages:
                if msg.error():
                    raise RuntimeError(msg.error())
                row = json.loads(msg.value())
                row["kafka_topic"] = msg.topic()
                row["kafka_partition"] = msg.partition()
                row["kafka_offset"] = msg.offset()
                rows.append(row)
                key = (msg.topic(), msg.partition())
                offsets[key] = max(offsets.get(key, -1), msg.offset())
            frame = pd.DataFrame(rows)
            frame["event_timestamp"] = pd.to_datetime(frame["event_timestamp"], utc=True)
            frame["zone_id"] = frame["zone_id"].astype("int64")
            frame["created_timestamp"] = pd.Timestamp.now(tz="UTC")

            identity = sorted((m.topic(), m.partition(), m.offset()) for m in messages)
            digest = hashlib.sha256(json.dumps(identity).encode()).hexdigest()
            destination = directory / f"{digest}.parquet"
            if not destination.exists():
                temporary = destination.with_suffix(".tmp")
                frame.to_parquet(temporary, index=False)
                os.replace(temporary, destination)

            latest = (
                frame.sort_values(["event_timestamp", "event_id"])
                .drop_duplicates("zone_id", keep="last")
            )
            store.push("zone_trip_push", latest, to=PushMode.ONLINE)

            commits = [TopicPartition(topic, partition, offset + 1)
                       for (topic, partition), offset in offsets.items()]
            consumer.commit(offsets=commits, asynchronous=False)
            print(json.dumps({"archived": len(frame), "online_zones": len(latest),
                              "committed_partitions": len(commits)}))
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
