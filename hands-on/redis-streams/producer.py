import json, time, uuid
import redis

STREAM = "jobs"
GROUP = "workers"

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

try:
    r.xgroup_create(STREAM, GROUP, id="$", mkstream=True)
except redis.exceptions.ResponseError as e:
    if "BUSYGROUP" not in str(e):
        raise

job = {
    "id": str(uuid.uuid4()),
    "type": "send_email",
    "payload": {"to": "user@example.com", "subject": "Hello FAIL", "body": "Hi"},
    "attempt": 0,
    "ts": int(time.time())
}

msg_id = r.xadd(STREAM, {"data": json.dumps(job)}, maxlen=10000, approximate=True)
print("ENQUEUED:", msg_id, job)