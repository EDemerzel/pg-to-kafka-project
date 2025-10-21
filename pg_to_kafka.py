# Minimal version of the listener (replace with your full prod version)
import os, json, time, logging, psycopg2, select
from confluent_kafka import Producer

PG_CONN_STR = os.getenv("PG_CONN_STR")
PG_CHANNEL  = os.getenv("PG_CHANNEL", "kafka_channel")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "")

logging.basicConfig(level=logging.INFO, format="%(message)s")

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "enable.idempotence": True, "acks":"all"})
conn = psycopg2.connect(PG_CONN_STR)
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()
cur.execute(f"LISTEN {PG_CHANNEL};")
logging.info(f"Listening on {PG_CHANNEL}")

def send(payload):
    try:
        producer.produce(KAFKA_TOPIC, json.dumps(payload).encode("utf-8"))
        producer.flush(5)
        logging.info("Delivered to Kafka")
    except Exception as e:
        logging.error(f"Kafka delivery failed: {e}")
        if KAFKA_DLQ_TOPIC:
            try:
                producer.produce(KAFKA_DLQ_TOPIC, json.dumps(payload).encode("utf-8"))
                producer.flush(5)
                logging.warning("Sent to DLQ")
            except Exception as e2:
                logging.error(f"DLQ failed: {e2}")

while True:
    if select.select([conn], [], [], 5) == ([], [], []): continue
    conn.poll()
    while conn.notifies:
        n = conn.notifies.pop(0)
        try:
            payload = json.loads(n.payload)
        except Exception:
            logging.error(f"Bad payload: {n.payload}")
            continue
        send(payload)
