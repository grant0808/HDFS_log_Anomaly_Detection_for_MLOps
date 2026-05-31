# GCP 배포 및 운영 가이드

이 문서는 HDFS 로그 이상 탐지 플랫폼을 GCP에서 실행하기 위한 상세 절차입니다. 대상 구성은 다음과 같습니다.

- GKE: Kubernetes 실행 환경
- Artifact Registry: Docker 이미지 저장소
- GCS: 원본 로그, 처리 로그, 학습 데이터, 모델 아티팩트 저장소
- Helm: Kubernetes 리소스 배포
- ArgoCD: GitOps 기반 배포 동기화
- PostgreSQL, ClickHouse, Redis, Kafka, Prometheus, Grafana, MLflow, Airflow: 클러스터 내부 서비스 또는 관리형 서비스

## 1. 사전 준비

로컬 또는 CI 환경에 아래 도구가 필요합니다.

```bash
gcloud --version
kubectl version --client
docker version
helm version
argocd version --client
```

GCP 인증:

```bash
gcloud auth login
gcloud auth application-default login
```

기본 변수 예시:

```bash
export PROJECT_ID="your-gcp-project"
export REGION="asia-northeast3"
export ZONE="asia-northeast3-a"
export CLUSTER_NAME="hdfs-observability-gke"
export NAMESPACE="hdfs-observability"
export REPOSITORY="mlops"
export IMAGE_NAME="hdfs-log-anomaly"
export STREAMING_IMAGE_NAME="hdfs-log-anomaly-streaming"
export IMAGE_TAG="v0.1.0"
```

프로젝트 설정:

```bash
gcloud config set project ${PROJECT_ID}
gcloud config set compute/region ${REGION}
gcloud config set compute/zone ${ZONE}
```

## 2. GCP API 활성화

필요한 API를 활성화합니다.

```bash
gcloud services enable \
  container.googleapis.com \
  artifactregistry.googleapis.com \
  storage.googleapis.com \
  iam.googleapis.com \
  cloudbuild.googleapis.com \
  secretmanager.googleapis.com \
  monitoring.googleapis.com \
  logging.googleapis.com
```

## 3. GCS 버킷 생성

플랫폼에서 사용하는 저장소를 생성합니다.

```bash
gsutil mb -l ${REGION} gs://${PROJECT_ID}-hdfs-raw-logs
gsutil mb -l ${REGION} gs://${PROJECT_ID}-hdfs-processed-logs
gsutil mb -l ${REGION} gs://${PROJECT_ID}-hdfs-training-datasets
gsutil mb -l ${REGION} gs://${PROJECT_ID}-hdfs-model-artifacts
```

권장 폴더 구조:

```text
gs://${PROJECT_ID}-hdfs-raw-logs/hdfs/
gs://${PROJECT_ID}-hdfs-processed-logs/events/
gs://${PROJECT_ID}-hdfs-training-datasets/deeplog/
gs://${PROJECT_ID}-hdfs-model-artifacts/mlflow/
gs://${PROJECT_ID}-hdfs-model-artifacts/models/
```

샘플 로그 업로드:

```bash
gsutil cp data/sample/hdfs.log gs://${PROJECT_ID}-hdfs-raw-logs/hdfs/hdfs.log
gsutil cp data/sample/hdfs_events.txt gs://${PROJECT_ID}-hdfs-training-datasets/deeplog/hdfs_events.txt
```

## 4. Artifact Registry 생성

Docker 이미지 저장소를 생성합니다.

```bash
gcloud artifacts repositories create ${REPOSITORY} \
  --repository-format=docker \
  --location=${REGION} \
  --description="MLOps Docker images"
```

Docker 인증 설정:

```bash
gcloud auth configure-docker ${REGION}-docker.pkg.dev
```

이미지 빌드 및 푸시:

```bash
docker build -t ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${IMAGE_NAME}:${IMAGE_TAG} .
docker push ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${IMAGE_NAME}:${IMAGE_TAG}

docker build -f Dockerfile.streaming -t ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${STREAMING_IMAGE_NAME}:${IMAGE_TAG} .
docker push ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${STREAMING_IMAGE_NAME}:${IMAGE_TAG}
```

## 5. GKE 클러스터 생성

Autopilot을 사용하는 경우:

```bash
gcloud container clusters create-auto ${CLUSTER_NAME} \
  --region=${REGION}
```

Standard 클러스터를 사용하는 경우:

```bash
gcloud container clusters create ${CLUSTER_NAME} \
  --region=${REGION} \
  --num-nodes=3 \
  --machine-type=e2-standard-4 \
  --enable-ip-alias \
  --workload-pool=${PROJECT_ID}.svc.id.goog
```

kubectl 컨텍스트 연결:

```bash
gcloud container clusters get-credentials ${CLUSTER_NAME} --region=${REGION}
kubectl create namespace ${NAMESPACE}
```

## 6. Workload Identity 설정

Kubernetes ServiceAccount와 GCP ServiceAccount를 연결합니다.

```bash
gcloud iam service-accounts create hdfs-anomaly-platform \
  --display-name="HDFS anomaly platform"
```

GCS 접근 권한 부여:

```bash
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:hdfs-anomaly-platform@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
```

Artifact Registry 읽기 권한:

```bash
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:hdfs-anomaly-platform@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/artifactregistry.reader"
```

Kubernetes ServiceAccount 생성:

```bash
kubectl create serviceaccount hdfs-anomaly-platform -n ${NAMESPACE}
```

IAM 바인딩:

```bash
gcloud iam service-accounts add-iam-policy-binding \
  hdfs-anomaly-platform@${PROJECT_ID}.iam.gserviceaccount.com \
  --role="roles/iam.workloadIdentityUser" \
  --member="serviceAccount:${PROJECT_ID}.svc.id.goog[${NAMESPACE}/hdfs-anomaly-platform]"
```

Kubernetes ServiceAccount annotation:

```bash
kubectl annotate serviceaccount hdfs-anomaly-platform \
  -n ${NAMESPACE} \
  iam.gke.io/gcp-service-account=hdfs-anomaly-platform@${PROJECT_ID}.iam.gserviceaccount.com
```

## 7. Secret 생성

운영에서는 비밀번호와 접속 정보를 Secret으로 관리합니다.

```bash
kubectl create secret generic hdfs-platform-secrets \
  -n ${NAMESPACE} \
  --from-literal=POSTGRES_DSN="postgresql+psycopg://hdfs:hdfs@postgres:5432/hdfs" \
  --from-literal=REDIS_URL="redis://redis:6379/0" \
  --from-literal=CLICKHOUSE_PASSWORD="" \
  --from-literal=SMTP_HOST="smtp.example.com" \
  --from-literal=SMTP_PORT="587" \
  --from-literal=SMTP_USER="alerts@example.com" \
  --from-literal=SMTP_PASSWORD="change-me" \
  --from-literal=SMTP_FROM="alerts@example.com" \
  --from-literal=SMTP_TO="mlops@example.com"
```

실제 운영에서는 `Secret Manager` 또는 External Secrets Operator 연동을 권장합니다.

## 8. 인프라 서비스 배포

이 저장소에는 애플리케이션 Helm 차트와 각 서브시스템의 values 예시가 포함되어 있습니다.

```text
k8s/kafka/values.yaml
k8s/flink/values.yaml
k8s/postgres/schema.sql
k8s/clickhouse/schema.sql
k8s/prometheus/values.yaml
k8s/grafana/dashboards.yaml
k8s/mlflow/values.yaml
k8s/airflow/values.yaml
k8s/evidently/values.yaml
```

운영에서는 아래 중 하나를 선택합니다.

1. 클러스터 내부 Helm 차트로 Kafka/PostgreSQL/Redis/ClickHouse 등을 배포
2. Cloud SQL, Memorystore, 관리형 Kafka, 외부 ClickHouse Cloud 같은 관리형 서비스를 사용

예시: Bitnami PostgreSQL 설치

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

helm upgrade --install postgres bitnami/postgresql \
  -n ${NAMESPACE} \
  --set auth.username=hdfs \
  --set auth.password=hdfs \
  --set auth.database=hdfs
```

예시: Redis 설치

```bash
helm upgrade --install redis bitnami/redis \
  -n ${NAMESPACE} \
  --set architecture=standalone \
  --set auth.enabled=false
```

예시: Kafka 설치

```bash
helm upgrade --install kafka bitnami/kafka \
  -n ${NAMESPACE} \
  --set kraft.enabled=true \
  --set listeners.client.protocol=PLAINTEXT
```

Kafka 토픽 생성:

```bash
kubectl run kafka-client -n ${NAMESPACE} --restart=Never --rm -it \
  --image=bitnami/kafka:latest -- bash

kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic hdfs.raw.logs
kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic hdfs.parsed.events
kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic hdfs.feature.windows
kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic hdfs.anomalies
kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic hdfs.inference.history
```

## 9. 데이터베이스 스키마 적용

애플리케이션은 시작 시 SQLAlchemy 메타데이터 테이블을 생성합니다. 별도 관리가 필요하면 아래 스키마 파일을 참고합니다.

```bash
k8s/postgres/schema.sql
k8s/clickhouse/schema.sql
```

PostgreSQL 스키마 수동 적용 예시:

```bash
kubectl port-forward svc/postgres 5432:5432 -n ${NAMESPACE}
psql "postgresql://hdfs:hdfs@localhost:5432/hdfs" -f k8s/postgres/schema.sql
```

ClickHouse 스키마 수동 적용 예시:

```bash
kubectl port-forward svc/clickhouse 8123:8123 -n ${NAMESPACE}
curl 'http://localhost:8123/' --data-binary @k8s/clickhouse/schema.sql
```

## 10. Helm values 설정

`helm/hdfs-log-anomaly/values.yaml`의 핵심 값을 GCP 환경에 맞게 설정합니다.

예시:

```yaml
image:
  repository: asia-northeast3-docker.pkg.dev/your-gcp-project/mlops/hdfs-log-anomaly
  tag: v0.1.0

streamingImage:
  repository: asia-northeast3-docker.pkg.dev/your-gcp-project/mlops/hdfs-log-anomaly-streaming
  tag: v0.1.0

serviceAccount:
  annotations:
    iam.gke.io/gcp-service-account: hdfs-anomaly-platform@your-gcp-project.iam.gserviceaccount.com

env:
  GCP_PROJECT_ID: your-gcp-project
  GCS_RAW_BUCKET: your-gcp-project-hdfs-raw-logs
  GCS_PROCESSED_BUCKET: your-gcp-project-hdfs-processed-logs
  GCS_ARTIFACTS_BUCKET: your-gcp-project-hdfs-model-artifacts
  KAFKA_BOOTSTRAP_SERVERS: kafka:9092
  RAW_TOPIC: hdfs.raw.logs
  PARSED_TOPIC: hdfs.parsed.events
  FEATURE_TOPIC: hdfs.feature.windows
  ANOMALY_TOPIC: hdfs.anomalies
  INFERENCE_TOPIC: hdfs.inference.history
```

차트는 `secret.name`을 `envFrom.secretRef`로 참조합니다. 운영 접속 정보는 `secret.create=false`로 두고 External Secrets 또는 사전에 생성한 Kubernetes Secret을 연결하는 구성을 권장합니다.

## 11. 애플리케이션 배포

Helm lint:

```bash
helm lint helm/hdfs-log-anomaly
```

배포:

```bash
helm upgrade --install hdfs-log-anomaly helm/hdfs-log-anomaly \
  --namespace ${NAMESPACE} \
  --set image.repository=${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${IMAGE_NAME} \
  --set streamingImage.repository=${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${STREAMING_IMAGE_NAME} \
  --set image.tag=${IMAGE_TAG} \
  --set streamingImage.tag=${IMAGE_TAG} \
  --set env.GCP_PROJECT_ID=${PROJECT_ID} \
  --set env.GCS_RAW_BUCKET=${PROJECT_ID}-hdfs-raw-logs \
  --set env.GCS_PROCESSED_BUCKET=${PROJECT_ID}-hdfs-processed-logs \
  --set env.GCS_ARTIFACTS_BUCKET=${PROJECT_ID}-hdfs-model-artifacts
```

상태 확인:

```bash
kubectl get pods -n ${NAMESPACE}
kubectl get svc -n ${NAMESPACE}
kubectl logs deploy/hdfs-log-anomaly-api -n ${NAMESPACE}
```

## 12. ArgoCD로 GitOps 배포

ArgoCD가 설치되어 있지 않다면 설치합니다.

```bash
kubectl create namespace argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
```

ArgoCD Application 설정 파일:

```bash
argocd/application.yaml
```

`repoURL`, `targetRevision`, `path`, `namespace` 값을 실제 Git 저장소와 환경에 맞게 수정한 뒤 적용합니다.

```bash
kubectl apply -f argocd/application.yaml
```

동기화:

```bash
argocd app sync hdfs-log-anomaly
argocd app get hdfs-log-anomaly
```

## 13. GitHub Actions 설정

워크플로 파일:

```text
.github/workflows/mlops.yml
```

GitHub Repository Secrets:

```text
GCP_WORKLOAD_IDENTITY_PROVIDER
GCP_SERVICE_ACCOUNT
ARGOCD_SERVER
ARGOCD_TOKEN
```

GitHub Repository Variables:

```text
GCP_PROJECT_ID
GCP_REGION
```

CI/CD 흐름:

```text
push 또는 pull_request
  -> Python 테스트
  -> Docker build
  -> main 브랜치일 경우 Artifact Registry push
  -> Helm lint
  -> main 브랜치일 경우 ArgoCD sync
```

## 14. 동작 검증

API 포트 포워딩:

```bash
kubectl port-forward svc/hdfs-log-anomaly-api 8000:8000 -n ${NAMESPACE}
```

헬스 체크:

```bash
curl http://localhost:8000/health
```

예측 요청:

```bash
curl -X POST http://localhost:8000/predict \
  -H "content-type: application/json" \
  -d '{"sequence":["E001","E002","E003"],"actual_event":"E004","metadata":{"sequence_id":"gcp-smoke-test"}}'
```

메트릭 확인:

```bash
curl http://localhost:8000/metrics
```

드리프트 확인:

```bash
curl http://localhost:8000/drift
```

최근 이상 탐지 이력:

```bash
curl http://localhost:8000/anomalies
```

## 15. Kafka 스트리밍 검증

샘플 로그를 Kafka 토픽에 넣습니다.

```bash
kubectl run kafka-producer -n ${NAMESPACE} --restart=Never --rm -it \
  --image=bitnami/kafka:latest -- bash
```

컨테이너 내부에서:

```bash
kafka-console-producer.sh --bootstrap-server kafka:9092 --topic hdfs.raw.logs
```

예시 로그 입력:

```text
PacketResponder 1 for block blk_388650 terminating
```

파싱 결과 토픽 확인:

```bash
kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic hdfs.parsed.events \
  --from-beginning
```

## 16. Prometheus와 Grafana 확인

Prometheus 포트 포워딩:

```bash
kubectl port-forward svc/prometheus-server 9090:80 -n ${NAMESPACE}
```

Grafana 포트 포워딩:

```bash
kubectl port-forward svc/grafana 3000:80 -n ${NAMESPACE}
```

확인할 메트릭:

```text
logs_processed_total
anomalies_total
anomaly_rate
inference_latency
data_drift_score
consumer_lag
unknown_template_count
model_version
prediction_confidence
```

## 17. 운영 체크리스트

배포 전 확인:

- Docker 이미지가 Artifact Registry에 push 되었는지 확인
- GKE 노드 또는 Workload Identity가 이미지 pull 권한을 갖는지 확인
- GCS 버킷 이름이 Helm values와 일치하는지 확인
- Kafka bootstrap 주소와 토픽이 준비되었는지 확인
- PostgreSQL, ClickHouse, Redis 접속 정보가 Secret으로 관리되는지 확인
- SMTP 알림 정보가 설정되었는지 확인
- `/health`, `/predict`, `/metrics`, `/drift`가 정상 응답하는지 확인
- Prometheus가 FastAPI `/metrics`를 scrape하는지 확인
- Grafana 대시보드에서 지표가 보이는지 확인
- ArgoCD Application이 `Synced`와 `Healthy` 상태인지 확인

## 18. 장애 대응 팁

Pod 상태 확인:

```bash
kubectl describe pod POD_NAME -n ${NAMESPACE}
kubectl logs POD_NAME -n ${NAMESPACE}
```

이미지 pull 오류:

```bash
gcloud artifacts repositories add-iam-policy-binding ${REPOSITORY} \
  --location=${REGION} \
  --member="serviceAccount:hdfs-anomaly-platform@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/artifactregistry.reader"
```

DB 연결 오류:

```bash
kubectl get secret hdfs-platform-secrets -n ${NAMESPACE} -o yaml
kubectl exec -it deploy/hdfs-log-anomaly-api -n ${NAMESPACE} -- env
```

Kafka 연결 오류:

```bash
kubectl get svc kafka -n ${NAMESPACE}
kubectl logs statefulset/kafka-controller -n ${NAMESPACE}
```

Helm 렌더링 확인:

```bash
helm template hdfs-log-anomaly helm/hdfs-log-anomaly \
  --namespace ${NAMESPACE}
```

ArgoCD 동기화 문제:

```bash
argocd app get hdfs-log-anomaly
argocd app diff hdfs-log-anomaly
argocd app sync hdfs-log-anomaly
```

## 19. 권장 운영 개선 사항

현재 저장소는 ML-first 실행 가능한 기준선을 제공합니다. 운영 안정성을 높이려면 아래 개선을 권장합니다.

- Helm chart에서 Secret 참조를 `envFrom` 또는 `secretKeyRef`로 전환
- PostgreSQL을 Cloud SQL로 분리
- Redis를 Memorystore로 분리
- Kafka를 관리형 Kafka 또는 별도 고가용성 Kafka 클러스터로 운영
- ClickHouse를 ClickHouse Cloud 또는 고가용성 StatefulSet으로 운영
- Prometheus/Grafana를 kube-prometheus-stack으로 통합
- External Secrets Operator로 Secret Manager 연동
- 모델 아티팩트 로딩을 GCS 기반으로 확장
- Flink 작업을 Kubernetes Job이 아니라 Flink Kubernetes Operator로 운영
- Airflow를 Cloud Composer 또는 공식 Helm chart로 운영
