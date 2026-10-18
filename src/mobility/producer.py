import os
import pyarrow.parquet as pq
import argparse
import time
from datetime import datetime,timezone
from pathlib import Path
from confluent_kafka import Producer
from mobility.events import build_event
import json
import uuid





def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=10000)
    parser.add_argument("--rate", type=float, default=100)
    parser.add_argument("--run-id", default=uuid.uuid4().hex[:12])
    args = parser.parse_args()
    if args.limit < 1 or args.rate <= 0:
        raise ValueError("limit and rate must be positive")

    producer = Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "broker:29092"),
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 10,
        "compression.type": "lz4",
    })

    failures = []
    delivered = 0

    def call_back(error, msg):
        nonlocal delivered
        if error is not None:
            print(f"发送失败：{error}")
            failures.append(str(error))
        else:
            print(f"发送成功：partition={msg.partition()}, "
                f"offset={msg.offset()}")
            delivered += 1

    file = pq.ParquetFile("/app/data/prepared/yellow_2025_01.parquet")
    sent = 0
    started = time.monotonic()
    print(json.dumps({"run_id": args.run_id, "limit": args.limit, "rate": args.rate}))
    for batch in file.iter_batches(batch_size=5000):
        for row in batch.to_pylist():
            # 每次发送前检查，达到上限就退出内层循环
            if sent >= args.limit:
                break

            now = datetime.now(timezone.utc).isoformat()
            event = build_event(row, args.run_id, now)
            value = json.dumps(event)

            while True:
                try:
                    producer.produce(
                        "trip-events-v1",
                        key=str(event["zone_id"]),
                        value=value,
                        on_delivery=call_back,
                    )
                    break
                except BufferError:
                    producer.poll(0.1)

            producer.poll(0)
            sent += 1

        # 退出外层循环，不再读取下一个 batch
        if sent >= args.limit:
            break
    remaining= producer.flush(30)
    report = {"queued": sent, "delivered": delivered, "remaining": remaining,
              "seconds": round(time.monotonic() - started, 2), "errors": failures[:3]}
    print(json.dumps(report))
    if remaining or failures or delivered != sent:
        raise RuntimeError("Producer did not deliver every queued event")


                
                                     
            



    


if __name__ == '__main__':
    main()