import argparse
import os
from confluent_kafka import Consumer
from pathlib import Path
import uuid
import time
import json

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic',required=True)
    parser.add_argument('--run-id',required=True)
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.count < 1 or args.timeout <= 0:
        raise ValueError("count and timeout must be positive")
    consumer = Consumer({
        "bootstrap.servers":os.getenv("KAFKA_BOOTSTRAP", "broker:29092"),
        "group.id" :f"trace-{uuid.uuid4().hex}",
        "auto.offset.reset":"earliest",
        "enable.auto.commit":False

    })
    consumer.subscribe([args.topic])
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + args.timeout
    event_ids = set()
    received = 0
    try:
        with target.open('w') as output:
            while received < args.count and time.monotonic()< deadline:
                msg = consumer.poll(1.0)
                if not msg:
                    continue
                if msg.error():
                    raise RuntimeError(msg.error())
                if not msg.value():
                    continue
                
                event = json.loads(msg.value())
                if event.get("run_id") != args.run_id:
                    continue
                item = {
                    "topic": msg.topic(), 
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "key": msg.key().decode() if msg.key() else None,
                    "event": event,
                }
                line = json.dumps(item)
                print(line)
                output.write(line + '\n')
                output.flush()
                event_ids.add(event["event_id"])
                received += 1


            if received != args.count:
                raise RuntimeError(f"Only received {received}/{args.count}; inspect pipeline")
            print(json.dumps({"received": received, "distinct_event_ids": len(event_ids)}))
    finally:
        consumer.close()
            
                          
                          


if __name__ == '__main__':
    main()
