"""Lambda: simulate the live sensors, sending one minute of readings to the stream's SQS queue.

Run every minute by EventBridge Scheduler (deployed DISABLED: switch it on for a test). Each station sends a reading
for the current minute; some are resent and some arrive late, at the rates in config/sources.toml [[streams]], and
the batch is shuffled, so the consumer sees what real sensors produce (medallion/stream.py simulate_readings).

Event: ignored. Environment: STREAM (the stream's bronze table, e.g. simulator_readings), QUEUE_URL.
"""

import datetime as dt
import json
import os
import random
from pathlib import Path

import boto3

from medallion.config import parse_config
from medallion.stream import simulate_readings

CONFIG = parse_config((Path(__file__).parent / "sources.toml").read_text(encoding="utf-8"))
sqs = boto3.client("sqs")


def handler(event, context):
    stream = CONFIG.stream(os.environ["STREAM"])
    rng = random.Random()
    messages = simulate_readings(CONFIG.stations, stream, dt.datetime.now(dt.timezone.utc), rng)
    rng.shuffle(messages)
    for start in range(0, len(messages), 10):  # SendMessageBatch takes at most 10 messages
        chunk = messages[start : start + 10]
        response = sqs.send_message_batch(
            QueueUrl=os.environ["QUEUE_URL"],
            Entries=[{"Id": str(i), "MessageBody": json.dumps(m)} for i, m in enumerate(chunk)],
        )
        if failed := response.get("Failed"):
            raise RuntimeError(f"SQS refused {len(failed)} of {len(chunk)} messages: {failed}")
    result = {"stream": stream.bronze_table, "sent": len(messages), "readings": len({m["event_id"] for m in messages})}
    print(json.dumps(result))
    return result
