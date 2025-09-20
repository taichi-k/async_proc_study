import json, time, random
import pika

MAX_ATTEMPTS = 3

conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
ch = conn.channel()
ch.queue_declare(queue="jobs", durable=True, arguments={
    "x-dead-letter-exchange": "",
    "x-dead-letter-routing-key": "jobs.dlq"
})
ch.queue_declare(queue="jobs.dlq", durable=True)
ch.basic_qos(prefetch_count=10)

print("Worker started")

def handle(ch, method, properties, body):
    job = json.loads(body)
    attempt = int(job.get("attempt", 0))
    should_fail = "subject" in job["payload"] and "FAIL" in job["payload"]["subject"]
    try:
        time.sleep(random.uniform(0.2, 0.8))
        if should_fail and attempt < MAX_ATTEMPTS - 1:
            raise RuntimeError("simulated")
        print("SUCCESS", job["id"])
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print("FAIL", job["id"], e)
        # 簡易リトライ: attempt++ して再投入、元はACK
        job["attempt"] = attempt + 1
        ch.basic_publish(exchange="", routing_key="jobs", body=json.dumps(job),
        properties=properties)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        # ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

ch.basic_consume(queue="jobs", on_message_callback=handle, auto_ack=False)
ch.start_consuming()