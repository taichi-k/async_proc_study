import json, uuid
import pika

conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
ch = conn.channel()

ch.queue_declare(queue="jobs", durable=True, arguments={
    "x-dead-letter-exchange": "",
    "x-dead-letter-routing-key": "jobs.dlq"
})
ch.queue_declare(queue="jobs.dlq", durable=True)

job = {
    "id": str(uuid.uuid4()),
    "type": "send_email",
    "payload": {"to": "user@example.com", "subject": "Hello", "body": "Hi"},
    "attempt": 0
}
ch.basic_publish(
    exchange="",
    routing_key="jobs",
    body=json.dumps(job),
    properties=pika.BasicProperties(delivery_mode=2) # 永続化
)
print("ENQUEUED", job["id"])
conn.close()