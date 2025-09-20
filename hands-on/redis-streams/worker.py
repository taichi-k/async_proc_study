import json, time, random
import redis
from typing import List, Tuple

STREAM = "jobs"
GROUP = "workers"
CONSUMER = f"c-{int(time.time())}"
MAX_ATTEMPTS = 3

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

try:
    r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
except redis.exceptions.ResponseError as e:
    if "BUSYGROUP" not in str(e):
        raise

print("Worker started as", CONSUMER)

while True:
    # ブロッキングで読み取り（新規）
    resp: List[Tuple[str, List[Tuple[str, List[Tuple[str, str]]]]]] = r.xreadgroup(
        groupname=GROUP,
        consumername=CONSUMER,
        streams={STREAM: ">"},
        count=10,
        block=20_000,
    )
    if not resp:
        continue

    for stream, messages in resp:
        for msg_id, fields in messages:
            try:
                data_json = fields.get("data")
                job = json.loads(data_json)
                job_id = job["id"]
                attempt = int(job.get("attempt", 0))

                # 疑似失敗: 件名に"FAIL"を含むと失敗
                should_fail = "subject" in job["payload"] and "FAIL" in job["payload"]["subject"]

                print(f"PROCESSING {msg_id} id={job_id} attempt={attempt}")
                time.sleep(random.uniform(0.2, 0.8))

                if should_fail and attempt < MAX_ATTEMPTS - 1:
                    raise RuntimeError("simulated failure")
                print(f"SUCCESS id={job_id}")
                r.xack(STREAM, GROUP, msg_id)
            except Exception as e:
                backoff = min(2 ** attempt, 30)
                print(f"FAIL id={job_id} -> retry in {backoff}s: {e}")
                time.sleep(backoff)
                job["attempt"] = attempt + 1
                r.xadd(STREAM, {"data": json.dumps(job)}, maxlen=10000, approximate=True)
                r.xack(STREAM, GROUP, msg_id)