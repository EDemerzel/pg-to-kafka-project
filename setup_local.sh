#!/usr/bin/env bash
set -euo pipefail

# === Config ===
CLUSTER_NAME="pg-kafka-dev"
K3D_VERSION="v5.7.3"        # k3d release
K8S_NAMESPACE="data-integration"
POSTGRES_RELEASE="pg-dev"
POSTGRES_DB="devdb"
POSTGRES_USER="devuser"
POSTGRES_PASSWORD="devpass"

KAFKA_NAMESPACE="kafka"
STRIMZI_VERSION="0.41.0"

IMAGE_NAME="pg-to-kafka"
IMAGE_TAG="dev"
FULL_IMAGE="${IMAGE_NAME}:${IMAGE_TAG}"

# Listener deployment config
KAFKA_TOPIC="poc-topic"
KAFKA_DLQ_TOPIC="poc-topic-dlq"
PG_CHANNEL="kafka_channel"

# === Helper ===
command_exists() { command -v "$1" >/dev/null 2>&1; }

# === Install prerequisites ===
install_prereqs() {
  echo "[*] Installing prerequisites (Docker, kubectl, Helm, k3d)..."
  sudo apt-get update -y
  sudo apt-get install -y curl wget ca-certificates gnupg lsb-release

  # Docker
  if ! command_exists docker; then
    echo "[*] Installing Docker..."
    sudo apt-get install -y docker.io
    sudo usermod -aG docker "$USER" || true
    echo "[!] You may need to log out/in for Docker group to take effect."
  fi

  # kubectl
  if ! command_exists kubectl; then
    echo "[*] Installing kubectl..."
    curl -fsSL "https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl" -o kubectl
    chmod +x kubectl
    sudo mv kubectl /usr/local/bin/
  fi

  # Helm
  if ! command_exists helm; then
    echo "[*] Installing Helm..."
    curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
  fi

  # k3d
  if ! command_exists k3d; then
    echo "[*] Installing k3d ${K3D_VERSION}..."
    curl -fsSL https://github.com/k3d-io/k3d/releases/download/${K3D_VERSION}/k3d-linux-amd64 -o k3d
    chmod +x k3d
    sudo mv k3d /usr/local/bin/
  fi
}

# === Create k3d cluster ===
create_cluster() {
  echo "[*] Creating k3d cluster '${CLUSTER_NAME}'..."
  k3d cluster create "${CLUSTER_NAME}" \
    --agents 3 \
    --servers 1 \
    --kubeconfig-switch-context \
    --wait
  echo "[*] Cluster created. Current context:"
  kubectl config current-context
}

# === Namespaces ===
create_namespaces() {
  kubectl create namespace "${K8S_NAMESPACE}" || true
  kubectl create namespace "${KAFKA_NAMESPACE}" || true
}

# === Install Strimzi ===
install_strimzi() {
  echo "[*] Installing Strimzi Operator into '${KAFKA_NAMESPACE}'..."
  
  # Download and modify the Strimzi install YAML to use the correct namespace
  curl -sL "https://github.com/strimzi/strimzi-kafka-operator/releases/download/${STRIMZI_VERSION}/strimzi-cluster-operator-${STRIMZI_VERSION}.yaml" \
    | sed "s/namespace: .*/namespace: ${KAFKA_NAMESPACE}/" \
    | kubectl apply -n "${KAFKA_NAMESPACE}" -f -
  
  echo "[*] Waiting for Strimzi operator to be ready..."
  kubectl rollout status deployment/strimzi-cluster-operator -n "${KAFKA_NAMESPACE}" --timeout=180s
  
  # Give the operator a moment to settle
  sleep 5

  echo "[*] Creating Kafka cluster (3 brokers)..."
  cat <<EOF | kubectl apply -n "${KAFKA_NAMESPACE}" -f -
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: dev-kafka
spec:
  kafka:
    version: 3.7.0
    replicas: 3
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
    config:
      offsets.topic.replication.factor: 3
      transaction.state.log.replication.factor: 3
      transaction.state.log.min.isr: 2
      default.replication.factor: 3
      min.insync.replicas: 2
      inter.broker.protocol.version: "3.7"
      log.message.format.version: "3.7"
    resources:
      requests:
        memory: 1Gi
        cpu: 500m
      limits:
        memory: 2Gi
        cpu: 1000m
    storage:
      type: ephemeral
  zookeeper:
    replicas: 3
    resources:
      requests:
        memory: 512Mi
        cpu: 250m
      limits:
        memory: 1Gi
        cpu: 500m
    storage:
      type: ephemeral
  entityOperator:
    topicOperator: {}
    userOperator: {}
EOF

  echo "[*] Waiting for Kafka to be ready (this can take 2-3 minutes)..."
  echo "[*] Creating Kafka pods..."
  
  # Wait for Kafka pods to be created first
  for i in {1..60}; do
    KAFKA_PODS=$(kubectl get pods -n "${KAFKA_NAMESPACE}" -l strimzi.io/name=dev-kafka-kafka --no-headers 2>/dev/null | wc -l)
    if [ "$KAFKA_PODS" -ge 3 ]; then
      echo "[*] Kafka pods created, waiting for them to become ready..."
      break
    fi
    echo "    Waiting for Kafka pods to be scheduled... ($i/60)"
    sleep 5
  done
  
  # Now wait for the Kafka resource to be fully ready
  if kubectl wait kafka/dev-kafka -n "${KAFKA_NAMESPACE}" --for=condition=Ready --timeout=600s; then
    echo "[✓] Kafka cluster is ready!"
  else
    echo "[!] Warning: Kafka might still be starting. Check with: kubectl get kafka -n ${KAFKA_NAMESPACE}"
    echo "[!] Check pods with: kubectl get pods -n ${KAFKA_NAMESPACE}"
  fi
}

# === Install Postgres via Helm ===
install_postgres() {
  echo "[*] Installing Postgres via Bitnami chart..."
  helm repo add bitnami https://charts.bitnami.com/bitnami
  helm repo update

  helm upgrade --install "${POSTGRES_RELEASE}" bitnami/postgresql \
    --namespace "${K8S_NAMESPACE}" \
    --set auth.username="${POSTGRES_USER}" \
    --set auth.password="${POSTGRES_PASSWORD}" \
    --set auth.database="${POSTGRES_DB}" \
    --set primary.persistence.enabled=false \
    --wait

  echo "[*] Postgres service:"
  kubectl get svc -n "${K8S_NAMESPACE}" -l app.kubernetes.io/name=postgresql
}

# === Build and load listener image ===
build_image() {
  echo "[*] Building listener Docker image '${FULL_IMAGE}'..."
  # Create sample Dockerfile and app if missing
  if [ ! -f Dockerfile ]; then
    cat > Dockerfile <<'DF'
FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends gcc librdkafka-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pg_to_kafka.py .
ENV PYTHONUNBUFFERED=1
USER 1000:1000
CMD ["python", "pg_to_kafka.py"]
DF
  fi

  if [ ! -f requirements.txt ]; then
    cat > requirements.txt <<'REQ'
psycopg2-binary==2.9.10
types-psycopg2==2.9.21
confluent-kafka==2.6.0
types-confluent-kafka==1.3.6
REQ
  fi

  if [ ! -f pg_to_kafka.py ]; then
    cat > pg_to_kafka.py <<'PY'
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
PY
  fi

  docker build -t "${FULL_IMAGE}" .
  echo "[*] Loading image into k3d cluster..."
  k3d image import "${FULL_IMAGE}" -c "${CLUSTER_NAME}"
}

# === Deploy listener with HPA ===
deploy_listener() {
  echo "[*] Creating secrets and config for listener..."
  # Get Postgres service DNS name
  PG_SVC=$(kubectl get svc -n "${K8S_NAMESPACE}" -l app.kubernetes.io/name=postgresql -o jsonpath='{.items[0].metadata.name}')
  PG_HOST="${PG_SVC}.${K8S_NAMESPACE}.svc.cluster.local"

  # Strimzi Kafka bootstrap service
  KAFKA_BOOTSTRAP_SVC="dev-kafka-kafka-bootstrap.${KAFKA_NAMESPACE}.svc.cluster.local:9092"

  kubectl create secret generic pg-secrets -n "${K8S_NAMESPACE}" \
    --from-literal=PG_CONN_STR="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${PG_HOST}:5432/${POSTGRES_DB}" \
    --dry-run=client -o yaml | kubectl apply -f -

  kubectl create configmap pg-kafka-config -n "${K8S_NAMESPACE}" \
    --from-literal=PG_CHANNEL="${PG_CHANNEL}" \
    --from-literal=KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP_SVC}" \
    --from-literal=KAFKA_TOPIC="${KAFKA_TOPIC}" \
    --from-literal=KAFKA_DLQ_TOPIC="${KAFKA_DLQ_TOPIC}" \
    --dry-run=client -o yaml | kubectl apply -f -

  echo "[*] Applying Deployment, Service, and HPA..."
  cat <<EOF | kubectl apply -n "${K8S_NAMESPACE}" -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pg-to-kafka
  labels: { app: pg-to-kafka }
spec:
  replicas: 2
  selector:
    matchLabels: { app: pg-to-kafka }
  template:
    metadata:
      labels: { app: pg-to-kafka }
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        runAsGroup: 1000
        seccompProfile: { type: RuntimeDefault }
      containers:
        - name: listener
          image: ${FULL_IMAGE}
          imagePullPolicy: IfNotPresent
          env:
            - name: PG_CONN_STR
              valueFrom:
                secretKeyRef: { name: pg-secrets, key: PG_CONN_STR }
            - name: PG_CHANNEL
              valueFrom:
                configMapKeyRef: { name: pg-kafka-config, key: PG_CHANNEL }
            - name: KAFKA_BOOTSTRAP
              valueFrom:
                configMapKeyRef: { name: pg-kafka-config, key: KAFKA_BOOTSTRAP }
            - name: KAFKA_TOPIC
              valueFrom:
                configMapKeyRef: { name: pg-kafka-config, key: KAFKA_TOPIC }
            - name: KAFKA_DLQ_TOPIC
              valueFrom:
                configMapKeyRef: { name: pg-kafka-config, key: KAFKA_DLQ_TOPIC }
          resources:
            requests: { cpu: "100m", memory: "128Mi" }
            limits: { cpu: "500m", memory: "256Mi" }
          livenessProbe:
            exec:
              command: ["sh", "-c", "echo ok"]    # Replace with HTTP health endpoint if you extend the app
            initialDelaySeconds: 10
            periodSeconds: 10
          readinessProbe:
            exec:
              command: ["sh", "-c", "echo ready"]
            initialDelaySeconds: 5
            periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: pg-to-kafka
  labels: { app: pg-to-kafka }
spec:
  selector: { app: pg-to-kafka }
  ports:
    - name: http
      port: 8080
      targetPort: 8080
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: pg-to-kafka-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: pg-to-kafka
  minReplicas: 2
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 60
EOF
}

# === Create Kafka topics via Strimzi ===
create_topics() {
  echo "[*] Creating Kafka topics '${KAFKA_TOPIC}' and DLQ '${KAFKA_DLQ_TOPIC}'..."
  cat <<EOF | kubectl apply -n "${KAFKA_NAMESPACE}" -f -
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: ${KAFKA_TOPIC}
  labels:
    strimzi.io/cluster: dev-kafka
spec:
  partitions: 6
  replicas: 3
  config:
    min.insync.replicas: 2
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: ${KAFKA_DLQ_TOPIC}
  labels:
    strimzi.io/cluster: dev-kafka
spec:
  partitions: 6
  replicas: 3
  config:
    min.insync.replicas: 2
EOF
}

# === Apply Postgres trigger function ===
apply_pg_trigger() {
  echo "[*] Applying Postgres trigger function and test table..."
  # Port-forward Postgres to local for psql convenience
  PG_POD=$(kubectl get pods -n "${K8S_NAMESPACE}" -l app.kubernetes.io/name=postgresql -o jsonpath='{.items[0].metadata.name}')
  kubectl port-forward -n "${K8S_NAMESPACE}" "${PG_POD}" 55432:5432 >/dev/null 2>&1 &
  sleep 3

  # Install psql client if missing
  if ! command_exists psql; then
    sudo apt-get install -y postgresql-client
  fi

  PSQL="psql postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:55432/${POSTGRES_DB}"

  ${PSQL} -c "CREATE TABLE IF NOT EXISTS poc_table (id SERIAL PRIMARY KEY, status TEXT, amount NUMERIC);"

  ${PSQL} <<'SQL'
CREATE OR REPLACE FUNCTION notify_kafka()
RETURNS trigger AS $$
DECLARE payload text;
BEGIN
  payload := json_build_object(
    'event', TG_OP,
    'table', TG_TABLE_NAME,
    'id', NEW.id,
    'ts', NOW(),
    'data', json_build_object('status', NEW.status, 'amount', NEW.amount)
  )::text;
  PERFORM pg_notify('kafka_channel', payload);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS poc_table_notify ON poc_table;
CREATE TRIGGER poc_table_notify
AFTER INSERT ON poc_table
FOR EACH ROW
EXECUTE FUNCTION notify_kafka();
SQL

  echo "[*] Trigger installed. You can test with:"
  echo "${PSQL} -c \"INSERT INTO poc_table(status, amount) VALUES ('NEW', 42.00);\""
}

# === Main ===
install_prereqs
create_cluster
create_namespaces
install_strimzi
install_postgres
build_image
create_topics
deploy_listener
apply_pg_trigger

echo
echo "✅ Environment ready."
echo "- Kubernetes context: $(kubectl config current-context)"
echo "- Kafka bootstrap (in-cluster): dev-kafka-kafka-bootstrap.${KAFKA_NAMESPACE}.svc.cluster.local:9092"
echo "- Postgres: Service in namespace ${K8S_NAMESPACE}, user=${POSTGRES_USER}, db=${POSTGRES_DB}"
echo "- Test insert command shown above will emit to Kafka topic '${KAFKA_TOPIC}'."
echo
echo "To observe logs:"
echo "  kubectl logs -n ${K8S_NAMESPACE} deploy/pg-to-kafka -f"
echo "To scale manually:"
echo "  kubectl scale deploy/pg-to-kafka -n ${K8S_NAMESPACE} --replicas=5"
echo "To check Kafka topics:"
echo "  kubectl get kafkatopics -n ${KAFKA_NAMESPACE}"
