# HDFS 로그 이상 탐지 플랫폼

HDFS 로그를 실시간으로 수집, 파싱, 특징화하고 DeepLog 기반 모델로 이상 여부를 판단하는 Kubernetes 기반 MLOps 관측 플랫폼입니다.

이 프로젝트는 로컬 개발 환경에서는 Docker Compose로 전체 흐름을 실행할 수 있고, 운영 환경에서는 GitHub Actions, Artifact Registry, Helm, ArgoCD를 통해 Kubernetes에 배포할 수 있도록 구성되어 있습니다.

## 아키텍처

```text
GitHub Actions -> Docker Build -> Artifact Registry -> Helm + ArgoCD -> Kubernetes

HDFS Logs -> Kafka -> PyFlink -> Drain3 -> Feature Extraction -> FastAPI Inference
                                                                  -> Kafka anomaly topic
                                                                  -> PostgreSQL + ClickHouse

Monitoring: Prometheus + Grafana + MLflow + Evidently + SMTP alerts
Storage:    GCS + PostgreSQL + ClickHouse + Redis
```

## 주요 기능

- Drain3 기반 HDFS 로그 템플릿 추출
- `template_id`, `event_id`, `block_id`, `host`, `sequence_id` 메타데이터 생성
- 슬라이딩 윈도우 기반 이벤트 시퀀스 생성
- DeepLog LSTM 추론 및 규칙 기반 fallback 탐지
- FastAPI 기반 온라인 추론 API
- PostgreSQL 기반 메타데이터/Feature Store 저장
- ClickHouse 기반 추론/이상 탐지 이력 저장
- Prometheus 메트릭 노출 및 Grafana 연동
- Evidently 스타일 데이터 드리프트 계산
- SMTP 이메일 알림
- PyFlink + Kafka 스트리밍 파이프라인
- Airflow 학습 DAG 및 MLflow 운영 구성
- Docker Compose 로컬 실행 모드
- Helm + ArgoCD Kubernetes 배포 모드

## 로컬 빠른 실행

개발 의존성을 설치합니다.

```bash
pip install -e ".[dev]"
```

FastAPI 서버를 실행합니다.

```bash
uvicorn app.main:app --reload
```

헬스 체크:

```bash
curl http://localhost:8000/health
```

예측 요청:

```bash
curl -X POST http://localhost:8000/predict \
  -H "content-type: application/json" \
  -d '{"sequence":["E001","E002","E003"],"actual_event":"E004","metadata":{"sequence_id":"demo"}}'
```

API 문서는 아래 주소에서 확인할 수 있습니다.

```text
http://localhost:8000/docs
```

## Docker Compose 전체 스택 실행

Kafka, PostgreSQL, ClickHouse, Redis, MLflow, Prometheus, Grafana, MailHog, FastAPI를 함께 실행합니다.

```bash
docker compose up --build
```

주요 접속 주소:

- FastAPI: `http://localhost:8000/docs`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`
- MLflow: `http://localhost:5000`
- MailHog: `http://localhost:8025`

## 학습

로컬에서 DeepLog 학습을 실행하려면 PyTorch 학습 의존성을 설치합니다.

```bash
pip install -e ".[training]"
```

샘플 이벤트 데이터로 모델을 학습합니다.

```bash
python -m app.training.train_deeplog --input data/sample/hdfs_events.txt --output-dir artifacts
```

학습 결과는 아래 경로에 저장됩니다.

- `artifacts/model.pt`
- `artifacts/vocab.json`

Airflow 학습 DAG는 `airflow/dags/hdfs_training_pipeline.py`에 있습니다.

MLflow는 Python 런타임 의존성이 아니라 Docker Compose/Kubernetes 서비스로 배포됩니다. Drain3가 구버전 `cachetools`를 고정하고 있어 최신 MLflow 클라이언트와 의존성 충돌이 발생할 수 있기 때문입니다.

## 스트리밍 파이프라인

PyFlink 작업은 Kafka의 `hdfs.raw.logs` 토픽에서 원본 로그를 읽고, Drain3 파싱 및 윈도우 생성을 거쳐 `hdfs.parsed.events` 토픽으로 결과를 발행합니다.

```bash
python -m flink.hdfs_log_job
```

샘플 HDFS 로그를 Kafka로 전송하려면 아래 스크립트를 실행합니다.

```bash
python scripts/produce_sample_logs.py
```

사용 토픽:

- `hdfs.raw.logs`
- `hdfs.parsed.events`
- `hdfs.feature.windows`
- `hdfs.anomalies`
- `hdfs.inference.history`

## API

제공되는 엔드포인트:

- `GET /health`: 서비스 상태 확인
- `POST /parse`: 원본 HDFS 로그 파싱
- `POST /predict`: 이벤트 시퀀스 이상 탐지
- `GET /metrics`: Prometheus 메트릭
- `GET /drift`: 데이터 드리프트 리포트
- `GET /anomalies`: 최근 이상 탐지 이력
- `GET /model_version`: 현재 모델 버전

`POST /predict` 요청 예시:

```json
{
  "sequence": ["E001", "E002", "E003"],
  "actual_event": "E004",
  "metadata": {
    "sequence_id": "demo"
  }
}
```

## 모니터링과 알림

Prometheus로 노출되는 주요 메트릭:

- `logs_processed_total`
- `anomalies_total`
- `anomaly_rate`
- `inference_latency`
- `data_drift_score`
- `consumer_lag`
- `unknown_template_count`
- `model_version`
- `prediction_confidence`

드리프트 계산 코드는 `monitoring/evidently`에 있고, SMTP 이메일 알림 코드는 `alerts`에 있습니다.

알림 조건:

- 데이터 드리프트 임계값 초과
- 이상 탐지 급증
- 추론 지연 시간 급증
- Kafka consumer lag 증가
- 모델 장애

## Kubernetes 배포

Helm 차트를 검증합니다.

```bash
helm lint helm/hdfs-log-anomaly
```

Kubernetes에 설치합니다.

```bash
helm upgrade --install hdfs-log-anomaly helm/hdfs-log-anomaly \
  --namespace hdfs-observability --create-namespace \
  --set image.repository=us-central1-docker.pkg.dev/PROJECT/mlops/hdfs-log-anomaly \
  --set image.tag=COMMIT_SHA
```

ArgoCD Application을 적용합니다.

```bash
kubectl apply -f argocd/application.yaml
```

운영 배포 전 아래 항목을 먼저 준비해야 합니다.

- GCP Workload Identity
- Artifact Registry
- GCS 버킷
- PostgreSQL, ClickHouse, Redis 접속 정보
- SMTP 인증 정보
- ArgoCD 접근 토큰
- Kubernetes Secret 및 Helm values

## CI/CD

GitHub Actions 워크플로는 `.github/workflows/mlops.yml`에 있습니다.

수행 단계:

- Python 테스트 실행
- Docker 이미지 빌드
- GCP Artifact Registry 이미지 푸시
- Helm lint
- ArgoCD sync

## 테스트

전체 테스트를 실행합니다.

```bash
python -m pytest
```

정적 검사:

```bash
python -m ruff check .
```

문법 컴파일 확인:

```bash
python -m compileall app feature_store monitoring alerts flink scripts tests
```

현재 테스트 범위:

- Drain3 파서
- 이벤트 윈도우 생성
- Feature Store write/read/versioning
- DeepLog fallback 추론
- 규칙 기반 탐지
- Drift 리포트
- FastAPI 주요 엔드포인트

## 디렉터리 구조

```text
app/                 FastAPI, 파서, 모델, 추론, DB, 메트릭
feature_store/       PostgreSQL 기반 Feature Store
monitoring/          드리프트 계산 및 Prometheus 설정
alerts/              SMTP 이메일 알림
flink/               PyFlink Kafka 스트리밍 작업
airflow/             학습 DAG
helm/                Kubernetes Helm 차트
k8s/                 운영 구성 값과 스키마
argocd/              ArgoCD Application
data/sample/         샘플 HDFS 로그와 이벤트 데이터
artifacts/           모델 및 vocab 아티팩트
tests/               단위/API/드리프트 테스트
```
