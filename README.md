# pg-to-kafka Local Development Environment

This repository provides a complete local development environment for the **pg-to-kafka** application, which streams PostgreSQL notifications to Apache Kafka using Postgres LISTEN/NOTIFY.

## Overview

The `setup_local.sh` script automates the creation of a local Kubernetes cluster (k3d) with:

- **PostgreSQL** database with trigger-based notifications
- **Apache Kafka** (via Strimzi operator) for message streaming
- **pg-to-kafka listener** application with horizontal pod autoscaling
- Kubernetes resources for deployment, service, and HPA

## Architecture

```plaintext
┌─────────────────┐         ┌──────────────────┐         ┌─────────────┐
│   PostgreSQL    │ NOTIFY  │  pg-to-kafka     │ Produce │   Kafka     │
│   (with trigger)│────────▶│  (2-10 replicas) │────────▶│  (3 nodes)  │
└─────────────────┘         └──────────────────┘         └─────────────┘
                                     │
                                     │ DLQ on error
                                     ▼
                            ┌──────────────────┐
                            │  poc-topic-dlq   │
                            └──────────────────┘
```

## Prerequisites

The script will automatically install the following if not present:

- Docker
- kubectl
- Helm
- k3d

**Manual prerequisite:** Ensure you have `sudo` access and an active internet connection.

## Quick Start

```bash
# Clone the repository
cd /path/to/pg-to-kafka-project

# Make the script executable
chmod +x setup_local.sh

# Run the setup script
./setup_local.sh
```

The script will:

1. Install prerequisites (Docker, kubectl, Helm, k3d)
2. Create a k3d cluster named `pg-kafka-dev`
3. Install Strimzi Kafka operator
4. Deploy a 3-node Kafka cluster
5. Install PostgreSQL via Bitnami Helm chart
6. Build and load the pg-to-kafka Docker image
7. Create Kafka topics (`poc-topic` and `poc-topic-dlq`)
8. Deploy the listener application with HPA
9. Apply PostgreSQL trigger function

**Expected runtime:** 5-10 minutes (depending on download speeds)

## Configuration

Edit the configuration variables at the top of `setup_local.sh` to customize:

```bash
CLUSTER_NAME="pg-kafka-dev"           # k3d cluster name
K3D_VERSION="v5.7.3"                  # k3d version
K8S_NAMESPACE="data-integration"      # Namespace for app and Postgres
POSTGRES_DB="devdb"                   # Database name
POSTGRES_USER="devuser"               # Database user
POSTGRES_PASSWORD="devpass"           # Database password
KAFKA_NAMESPACE="kafka"               # Namespace for Kafka
STRIMZI_VERSION="0.41.0"              # Strimzi operator version
KAFKA_TOPIC="poc-topic"               # Main Kafka topic
KAFKA_DLQ_TOPIC="poc-topic-dlq"       # Dead letter queue topic
PG_CHANNEL="kafka_channel"            # PostgreSQL NOTIFY channel
```

## Components

### PostgreSQL

- Deployed via Bitnami Helm chart
- Configured with a trigger function `notify_kafka()` on table `poc_table`
- Creates notifications on INSERT operations
- Service: `<POSTGRES_RELEASE>-postgresql.<K8S_NAMESPACE>.svc.cluster.local`

### Kafka

- 3-broker cluster managed by Strimzi
- Zookeeper ensemble included
- Topics configured with 6 partitions, 3 replicas, `min.insync.replicas=2`
- Bootstrap servers: `dev-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092`

### pg-to-kafka Listener

- Python application using `psycopg2` and `confluent-kafka`
- Listens to PostgreSQL NOTIFY channel
- Produces messages to Kafka with idempotence enabled
- DLQ support for failed messages
- Horizontal Pod Autoscaler (HPA): 2-10 replicas based on CPU/memory usage

## Testing

### Insert Test Data

After setup completes, test the pipeline:

```bash
# Port-forward to PostgreSQL
kubectl port-forward -n data-integration svc/<POSTGRES_RELEASE>-postgresql 5432:5432 &

# Insert test data
PGPASSWORD=devpass psql -h localhost -U devuser -d devdb -c \
  "INSERT INTO poc_table (data) VALUES ('{\"test\": \"message\"}');"
```

### Monitor Logs

```bash
# Watch pg-to-kafka logs
kubectl logs -n data-integration deploy/pg-to-kafka -f

# Check Kafka topic messages (from a Kafka pod)
kubectl exec -it dev-kafka-kafka-0 -n kafka -- \
  bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic poc-topic \
  --from-beginning
```

### Check Resources

```bash
# View all pods
kubectl get pods -n data-integration
kubectl get pods -n kafka

# Check HPA status
kubectl get hpa -n data-integration

# View Kafka topics
kubectl get kafkatopics -n kafka
```

## Scaling

### Manual Scaling

```bash
kubectl scale deploy/pg-to-kafka -n data-integration --replicas=5
```

### Automatic Scaling

The HPA will automatically scale between 2-10 replicas based on:

- CPU utilization (target: 70%)
- Memory utilization (target: 80%)

## Troubleshooting

### Cluster Creation Issues

```bash
# Check if cluster exists
k3d cluster list

# Delete and recreate
k3d cluster delete pg-kafka-dev
./setup_local.sh
```

### Kafka Not Ready

```bash
# Check Kafka pods
kubectl get pods -n kafka

# View Kafka cluster status
kubectl get kafka -n kafka dev-kafka -o yaml

# Check operator logs
kubectl logs -n kafka deploy/strimzi-cluster-operator
```

### PostgreSQL Connection Issues

```bash
# Get Postgres service
kubectl get svc -n data-integration -l app.kubernetes.io/name=postgresql

# Test connection from a pod
kubectl run -it --rm psql-test --image=postgres:15 -n data-integration -- \
  psql -h <POSTGRES_RELEASE>-postgresql -U devuser -d devdb
```

### Application Logs

```bash
# View recent logs
kubectl logs -n data-integration deploy/pg-to-kafka --tail=100

# Stream logs from all pods
kubectl logs -n data-integration -l app=pg-to-kafka -f

# Check pod status
kubectl describe pod -n data-integration -l app=pg-to-kafka
```

## Cleanup

To remove the entire environment:

```bash
# Delete the k3d cluster
k3d cluster delete pg-kafka-dev

# Verify deletion
k3d cluster list
```

## Development Workflow

### Updating the Application

1. Modify `pg_to_kafka.py` or `requirements.txt`
2. Rebuild the image:

   ```bash
   docker build -t pg-to-kafka:dev .
   k3d image import pg-to-kafka:dev -c pg-kafka-dev
   ```

3. Restart the deployment:

   ```bash
   kubectl rollout restart deploy/pg-to-kafka -n data-integration
   ```

### Using Helm for Deployment

The project includes Helm charts in `pg-to-kafka-project/helm/pg-to-kafka/` for production deployments. For local development, the script uses inline YAML for simplicity.

## Environment Variables

The pg-to-kafka application uses the following environment variables:

| Variable | Description | Source |
|----------|-------------|--------|
| `PG_CONN_STR` | PostgreSQL connection string | Secret: `pg-secrets` |
| `PG_CHANNEL` | NOTIFY channel to listen on | ConfigMap: `pg-kafka-config` |
| `KAFKA_BOOTSTRAP` | Kafka bootstrap servers | ConfigMap: `pg-kafka-config` |
| `KAFKA_TOPIC` | Primary Kafka topic | ConfigMap: `pg-kafka-config` |
| `KAFKA_DLQ_TOPIC` | Dead letter queue topic | ConfigMap: `pg-kafka-config` |

## Additional Resources

- [Strimzi Documentation](https://strimzi.io/docs/operators/latest/overview.html)
- [k3d Documentation](https://k3d.io/)
- [PostgreSQL LISTEN/NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html)
- [Confluent Kafka Python Client](https://docs.confluent.io/kafka-clients/python/current/overview.html)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

Copyright © 2025 Ralph O'Flinn

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
