"""PostgreSQL to Kafka bridge service."""
import json
import logging
import os
import select
import signal
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import psycopg2
from confluent_kafka import Producer
from psycopg2 import extensions

# === Config from env ===
PG_CONN_STR = os.getenv("PG_CONN_STR")
PG_CHANNEL = os.getenv("PG_CHANNEL", "kafka_channel")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
RETRY_BASE = float(os.getenv("RETRY_BASE", "0.5"))
RETRY_FACTOR = float(os.getenv("RETRY_FACTOR", "2.0"))
DELIVERY_TIMEOUT = float(os.getenv("DELIVERY_TIMEOUT", "5"))
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8080"))

# === Logging ===
logging.basicConfig(level=logging.INFO, format="%(message)s")


def log_event(level, msg, **kw):
    """
    Logs an event by printing a JSON-formatted message with a timestamp, log level, message,
    and additional keyword arguments.

    Args:
        level (str): The severity level of the log (e.g., 'INFO', 'ERROR').
        msg (str): The log message to record.
        **kw: Additional keyword arguments to include in the log entry.

    Example:
        log_event('INFO', 'Process started', user='alice', process_id=123)
    """
    print(json.dumps({"ts": int(time.time()),
          "level": level, "msg": msg, **kw}))


# === Metrics ===
metrics = {"notifications": 0, "delivered": 0, "failed": 0, "dlq": 0}

# === State ===
state = {"running": False, "pg_connected": False, "kafka_ready": False}

# === Kafka ===
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP,
                    "enable.idempotence": True, "acks": "all"})

# === Global PostgreSQL connection ===
conn = None  # pylint: disable=invalid-name
cur = None  # pylint: disable=invalid-name


def verify_kafka():
    """
    Checks the readiness of the Kafka broker by attempting to list available topics.

    If the broker is reachable within the timeout, sets the 'kafka_ready' state to True and logs an info event.
    If an exception occurs, sets the 'kafka_ready' state to False and logs an error event with the exception details.
    """
    try:
        producer.list_topics(timeout=3)
        state["kafka_ready"] = True
        log_event("INFO", "Kafka ready", bootstrap=KAFKA_BOOTSTRAP)
    except Exception as e:  # pylint: disable=broad-except
        state["kafka_ready"] = False
        log_event("ERROR", "Kafka not ready", error=str(e))

# === Postgres ===


def connect_pg():
    """
    Establishes a connection to the PostgreSQL database and sets up a listener on the specified channel.

    This function initializes global connection and cursor objects, sets the isolation level to autocommit,
    executes a LISTEN command on the configured PostgreSQL channel, updates the connection state, and logs the event.

    Raises:
        psycopg2.Error: If the connection or LISTEN command fails.
    """
    global conn, cur  # pylint: disable=global-statement
    conn = psycopg2.connect(PG_CONN_STR)
    conn.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute(f"LISTEN {PG_CHANNEL};")
    state["pg_connected"] = True
    log_event("INFO", "Postgres connected", channel=PG_CHANNEL)

# === Health server ===


class Handler(BaseHTTPRequestHandler):
    """
    Handler class for HTTP requests, extending BaseHTTPRequestHandler.

    Endpoints:
        - /healthz: Returns service health status as JSON. Responds with 200 if all components are running
          and connected; otherwise, 503.
        - /metrics: Returns service metrics as JSON with a 200 response.
        - Any other path: Responds with 404 Not Found.

    Attributes:
        None

    Methods:
        do_GET(): Handles GET requests for health check, metrics, and unknown endpoints.
    """

    def do_GET(self):  # pylint: disable=invalid-name
        """
        Handles HTTP GET requests for health and metrics endpoints.

        - If the request path is "/healthz", returns a JSON response with the current application state.
          The response status code is 200 if the application is running, PostgreSQL is connected, and Kafka is ready;
          otherwise, returns 503.
        - If the request path is "/metrics", returns a JSON response with application metrics and a 200 status code.
        - For any other path, returns a 404 status code.

        Responses are sent as JSON-encoded bodies.
        """
        if self.path == "/healthz":
            code = 200 if state["running"] and state["pg_connected"] and state["kafka_ready"] else 503
            body = json.dumps(state).encode()
            self.send_response(code)
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/metrics":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps(metrics).encode())
        else:
            self.send_response(404)
            self.end_headers()


def start_http():
    """
    Starts an HTTP health server in a separate daemon thread.

    The server listens on all interfaces at the port specified by HEALTH_PORT,
    using the Handler class to process incoming requests. Logs an informational
    event when the server has started.

    Raises:
        Exception: If the server fails to start.
    """
    srv = HTTPServer(("0.0.0.0", HEALTH_PORT), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    log_event("INFO", "Health server started", port=HEALTH_PORT)


# === Shutdown ===
RUNNING = True


def shutdown_handler(_sig, _frame):
    """
    Handles graceful shutdown of the application when a termination signal is received.

    This function sets the global and state flags to indicate the application is no longer running,
    attempts to flush any remaining messages in the Kafka producer, and logs the shutdown event.

    Args:
        _sig: The signal number received.
        _frame: The current stack frame (unused).

    Returns:
        None
    """
    global RUNNING  # pylint: disable=global-statement
    RUNNING = False
    state["running"] = False
    try:
        producer.flush(2)
    except Exception:  # pylint: disable=broad-except
        pass
    log_event("INFO", "Shutting down")


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# === Kafka send with retry ===


def send_with_retry(payload):
    """
    Attempts to send a payload to a Kafka topic with retry logic and exponential backoff.

    On failure after maximum retries, optionally sends the payload to a Dead Letter Queue (DLQ) topic if configured.
    Metrics for delivered, failed, and DLQ messages are updated accordingly, and events are logged for errors
    and warnings.

    Args:
        payload (dict): The message payload to be sent to Kafka.

    Returns:
        bool: True if the payload was successfully delivered to Kafka, False otherwise.
    """
    attempt = 0
    backoff = RETRY_BASE
    while attempt <= MAX_RETRIES:
        try:
            if KAFKA_TOPIC is None:
                log_event("ERROR", "KAFKA_TOPIC is not set; cannot send message")
                metrics["failed"] += 1
                return False
            producer.produce(KAFKA_TOPIC, value=json.dumps(payload).encode())
            producer.flush(DELIVERY_TIMEOUT)
            metrics["delivered"] += 1
            return True
        except Exception as e:  # pylint: disable=broad-except
            attempt += 1
            metrics["failed"] += 1
            log_event("ERROR", "Kafka delivery failed",
                      error=str(e), attempt=attempt)
            time.sleep(backoff)
            backoff *= RETRY_FACTOR
    if KAFKA_DLQ_TOPIC:
        try:
            producer.produce(
                KAFKA_DLQ_TOPIC, value=json.dumps(payload).encode())
            producer.flush(DELIVERY_TIMEOUT)
            metrics["dlq"] += 1
            log_event("WARN", "Sent to DLQ")
        except Exception as e:  # pylint: disable=broad-except
            log_event("CRITICAL", "DLQ failed", error=str(e))
    return False

# === Main ===


def main():
    """
    Main entry point for the PostgreSQL to Kafka service.

    Starts the HTTP server, verifies Kafka connectivity, and connects to PostgreSQL.
    Enters a loop to listen for notifications from PostgreSQL, processes each notification,
    and sends the payload to Kafka with retry logic. Handles JSON decoding errors and logs events.
    Stops the service gracefully when the running state is set to False.
    """
    start_http()
    verify_kafka()
    connect_pg()
    state["running"] = True
    while RUNNING:
        if select.select([conn], [], [], 5) == ([], [], []):
            continue
        if conn is not None:
            conn.poll()
            while conn.notifies:
                n = conn.notifies.pop(0)
                metrics["notifications"] += 1
                try:
                    payload = json.loads(n.payload)
                    send_with_retry(payload)
                except json.JSONDecodeError as e:
                    log_event("ERROR", "Invalid JSON in notification",
                              error=str(e), payload=n.payload)
        else:
            log_event("ERROR", "Postgres connection is None")
            time.sleep(1)

    log_event("INFO", "Service stopped")


if __name__ == "__main__":
    main()
