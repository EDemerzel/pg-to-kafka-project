# PostgreSQL to Kafka Listener

A lightweight Python service that listens to PostgreSQL `NOTIFY` events and forwards them to Apache Kafka with built-in retry logic, dead-letter queue (DLQ) support, and health monitoring.

## Overview

This service bridges PostgreSQL's LISTEN/NOTIFY mechanism with Apache Kafka, enabling real-time event streaming from database triggers to Kafka topics. It's designed for cloud-native environments with Kubernetes deployment support via Helm.

## Features

- **PostgreSQL LISTEN/NOTIFY**: Subscribes to PostgreSQL notification channels
- **Kafka Producer**: Publishes messages to Kafka topics with idempotent delivery
- **Retry Logic**: Configurable exponential backoff for failed deliveries
- **Dead Letter Queue (DLQ)**: Failed messages sent to a separate DLQ topic after max retries
- **Health Monitoring**: HTTP endpoints for health checks and metrics
- **Graceful Shutdown**: Handles SIGINT/SIGTERM signals properly
- **Structured Logging**: JSON-formatted logs for easy parsing
- **Well-Documented Code**: Comprehensive docstrings and type hints throughout
- **Kubernetes Ready**: Includes Helm chart with HPA and NetworkPolicy
- **Production Ready**: Runs as non-root user, includes resource limits

## Architecture

```plaintext
PostgreSQL → NOTIFY → Listener Service → Kafka Topic
                           ↓ (on failure)
                      DLQ Topic (optional)
```

## Prerequisites

- Python 3.11+
- PostgreSQL with LISTEN/NOTIFY configured
- Apache Kafka cluster
- Kubernetes cluster (for Helm deployment)
- Docker (for containerized deployment)

## Configuration

All configuration is done via environment variables:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `PG_CONN_STR` | Yes | - | PostgreSQL connection string (e.g., `postgresql://user:pass@host:5432/db`) |
| `PG_CHANNEL` | No | `kafka_channel` | PostgreSQL notification channel to listen on |
| `KAFKA_BOOTSTRAP` | Yes | - | Kafka bootstrap servers (e.g., `localhost:9092`) |
| `KAFKA_TOPIC` | Yes | - | Primary Kafka topic for messages |
| `KAFKA_DLQ_TOPIC` | No | `""` | Dead letter queue topic for failed messages |
| `MAX_RETRIES` | No | `5` | Maximum retry attempts for Kafka delivery |
| `RETRY_BASE` | No | `0.5` | Base backoff time in seconds |
| `RETRY_FACTOR` | No | `2.0` | Exponential backoff multiplier |
| `DELIVERY_TIMEOUT` | No | `5` | Kafka delivery timeout in seconds |
| `HEALTH_PORT` | No | `8080` | HTTP port for health and metrics endpoints |

## Local Development

### Setup

1. Install dependencies:

```bash
pip install -r app/requirements.txt
```

1. Set environment variables:

```bash
export PG_CONN_STR="postgresql://user:pass@localhost:5432/mydb"
export KAFKA_BOOTSTRAP="localhost:9092"
export KAFKA_TOPIC="poc_topic"
export KAFKA_DLQ_TOPIC="poc_topic_dlq"
```

1. Run the service:

```bash
python app/pg_to_kafka.py
```

### Testing PostgreSQL NOTIFY

In your PostgreSQL database, trigger a notification:

```sql
-- Listen to the channel
LISTEN kafka_channel;

-- Send a test notification
NOTIFY kafka_channel, '{"event": "test", "data": {"id": 1, "message": "Hello Kafka"}}';
```

## Docker Deployment

### Build Image

```bash
docker build -t pg-to-kafka:latest ./app
```

### Run Container

```bash
docker run -d \
  -e PG_CONN_STR="postgresql://user:pass@host:5432/db" \
  -e KAFKA_BOOTSTRAP="kafka:9092" \
  -e KAFKA_TOPIC="events" \
  -e KAFKA_DLQ_TOPIC="events_dlq" \
  -p 8080:8080 \
  pg-to-kafka:latest
```

## Kubernetes Deployment

### Helm Installation

1. Update values in `helm/pg-to-kafka/values.yaml`:

```yaml
env:
  PG_CHANNEL: kafka_channel
  KAFKA_TOPIC: your_topic
  KAFKA_DLQ_TOPIC: your_topic_dlq
  KAFKA_BOOTSTRAP: kafka-service:9092
  PG_CONN_STR: postgresql://user:pass@postgres:5432/db
```

1. Install the Helm chart:

```bash
helm install pg-to-kafka helm/pg-to-kafka/
```

1. Verify deployment:

```bash
kubectl get pods -l app=pg-to-kafka
kubectl logs -f <pod-name>
```

### Helm Configuration

The Helm chart includes:

- **Deployment**: Multi-replica deployment with configurable resources
- **Service**: ClusterIP service exposing health endpoint
- **HPA**: Horizontal Pod Autoscaler (optional, enabled by default)
- **NetworkPolicy**: Restricts ingress/egress to PostgreSQL and Kafka ports
- **Health Probes**: Liveness and readiness checks on `/healthz`

Key Helm values:

```yaml
replicaCount: 2
image:
  repository: pg-to-kafka
  tag: dev
resources:
  requests: { cpu: 100m, memory: 128Mi }
  limits: { cpu: 500m, memory: 256Mi }
hpa:
  enabled: true
  minReplicas: 2
  maxReplicas: 10
  cpuUtilization: 60
```

## Monitoring

### Health Endpoint

Check service health:

```bash
curl http://localhost:8080/healthz
```

Response:

```json
{
  "running": true,
  "pg_connected": true,
  "kafka_ready": true
}
```

- **200 OK**: All systems operational
- **503 Service Unavailable**: One or more components not ready

### Metrics Endpoint

Retrieve runtime metrics:

```bash
curl http://localhost:8080/metrics
```

Response:

```json
{
  "notifications": 1523,
  "delivered": 1500,
  "failed": 20,
  "dlq": 3
}
```

Metrics:

- `notifications`: Total PostgreSQL notifications received
- `delivered`: Successfully delivered to Kafka
- `failed`: Failed delivery attempts (includes retries)
- `dlq`: Messages sent to dead letter queue

## Logging

The service outputs structured JSON logs:

```json
{
  "ts": 1729523400,
  "level": "INFO",
  "msg": "Postgres connected",
  "channel": "kafka_channel"
}
```

Log levels:

- `INFO`: Normal operations
- `WARN`: Non-critical issues (e.g., DLQ usage)
- `ERROR`: Delivery failures, parsing errors
- `CRITICAL`: Unrecoverable errors (e.g., DLQ failure)

## Error Handling

### Retry Logic

Failed Kafka deliveries trigger exponential backoff:

1. Initial attempt fails
2. Wait `RETRY_BASE` seconds (default: 0.5s)
3. Retry with `RETRY_BASE * RETRY_FACTOR` (default: 1s)
4. Continue up to `MAX_RETRIES` (default: 5)
5. If all retries fail, send to DLQ (if configured)

### Dead Letter Queue

Messages that exceed max retries are sent to `KAFKA_DLQ_TOPIC` for later analysis. If DLQ delivery also fails, a CRITICAL log is generated.

## Security

- **Non-root User**: Container runs as UID 1000
- **NetworkPolicy**: Kubernetes deployment includes network isolation
- **Connection Security**: Supports SSL/TLS for PostgreSQL and Kafka
- **Idempotent Producer**: Kafka producer configured with `enable.idempotence=True`

## PostgreSQL Setup

To use this service, configure your PostgreSQL database with NOTIFY triggers:

```sql
-- Create a function to notify Kafka
CREATE OR REPLACE FUNCTION notify_kafka()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_notify(
    'kafka_channel',
    json_build_object(
      'table', TG_TABLE_NAME,
      'operation', TG_OP,
      'data', row_to_json(NEW)
    )::text
  );
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach trigger to a table
CREATE TRIGGER poc_table_notify
AFTER INSERT OR UPDATE ON poc_table
FOR EACH ROW
EXECUTE FUNCTION notify_kafka();
```

## Performance Considerations

- **Polling Interval**: Uses `select()` with 5-second timeout for efficient notification polling
- **Resource Usage**: Minimal footprint (~100-200MB memory)
- **Scalability**: Horizontal scaling via HPA based on CPU utilization
- **Backpressure**: Kafka producer flush ensures delivery before proceeding
- **Connection Pooling**: Single persistent PostgreSQL connection with auto-commit

## Troubleshooting

### Service won't start

- Verify `PG_CONN_STR` and `KAFKA_BOOTSTRAP` are correct
- Check network connectivity to PostgreSQL and Kafka
- Review logs for connection errors

### Messages not being delivered

- Verify PostgreSQL LISTEN channel matches `PG_CHANNEL`
- Check Kafka topic exists and is accessible
- Review `/metrics` endpoint for failure counts
- Check DLQ topic for failed messages

### High DLQ rate

- Increase `MAX_RETRIES` or `DELIVERY_TIMEOUT`
- Check Kafka cluster health and available resources
- Verify network stability between service and Kafka

## Development

### Project Structure

```plaintext
pg-to-kafka-project/
├── app/
│   ├── pg_to_kafka.py      # Main application with full docstrings
│   ├── requirements.txt     # Python dependencies (runtime + type stubs)
│   └── Dockerfile          # Container image definition
├── helm/
│   └── pg-to-kafka/        # Helm chart
│       ├── Chart.yaml
│       ├── values.yaml
│       └── templates/
└── README.md               # This file
```

### Code Quality

The codebase follows Python best practices:

- **Module docstring**: Clear description at the top of the file
- **Function docstrings**: Comprehensive documentation for all functions
- **Type hints**: Type stubs installed for better IDE support
- **Pylint compliant**: Code follows linting standards with appropriate disable comments where needed
- **Error handling**: Specific exception handling with broad-except only where necessary
- **Graceful shutdown**: Proper signal handling for SIGINT/SIGTERM

### Dependencies

- `psycopg2-binary==2.9.10`: PostgreSQL adapter (binary distribution)
- `types-psycopg2==2.9.21`: Type stubs for psycopg2 (development/IDE support)
- `confluent-kafka==2.6.0`: Kafka client library
- `types-confluent-kafka==1.3.6`: Type stubs for confluent-kafka (development/IDE support)

**Note**: The `types-*` packages provide type hints for better IDE support and static type checking. They are optional for runtime but recommended for development.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

Copyright © 2025 Ralph O'Flinn

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues and questions:

- Check logs for error messages
- Review `/healthz` and `/metrics` endpoints
- Verify PostgreSQL and Kafka connectivity
- Consult troubleshooting section above
