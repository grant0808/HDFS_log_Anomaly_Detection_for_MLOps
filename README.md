# HDFS 로그 이상 탐지 플랫폼

HDFS 로그를 수집, 파싱, 특징화하고 DeepLog 기반 모델 또는 규칙 기반 fallback으로 이상 여부를 판단하는 Kubernetes 지향 MLOps 관측 플랫폼입니다.

이 프로젝트는 로컬 개발 환경에서는 Docker Compose로 API와 관측 스택을 실행할 수 있고, 운영 환경에서는 GitHub Actions, Artifact Registry, Helm, ArgoCD를 통해 Kubernetes에 배포할 수 있도록 구성되어 있습니다.

현재 저장소는 운영 아키텍처의 기준 구현과 배포 골격을 제공하는 MVP입니다. 실시간 스트리밍 추론, 모델 자동 승격, 운영 알림 룰은 일부 스캐폴딩 상태이므로 운영 배포 전 `ARCHITECTURE.md`의 보강 로드맵을 확인해야 합니다.

## 아키텍처

상세 아키텍처와 현재 구현/목표 구현의 차이는 `ARCHITECTURE.md`를 참고합니다.

```text
GitHub Actions -> Docker Build -> Artifact Registry -> Helm + ArgoCD -> Kubernetes

현재 구현:
HDFS Logs -> FastAPI /parse -> Drain3 -> PostgreSQL
Event Sequence -> FastAPI /predict -> DeepLog or Rule fallback -> PostgreSQL
Kafka raw logs -> PyFlink parser/window job -> Kafka feature topic -> inference worker -> Kafka anomaly/history topics

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
- PyFlink + Kafka 스트리밍 파이프라인 골격
- Airflow 학습 DAG 및 MLflow 운영 구성 골격
- Docker Compose 로컬 실행 모드
- Helm + ArgoCD Kubernetes 배포 모드

## 구현 상태 요약

| 영역 | 현재 상태 | 운영 전 보강 필요 |
| --- | --- | --- |
| 파싱/API | `/parse`, `/predict`, `/metrics`, `/drift`, `/anomalies` 제공 | 인증, 요청 제한, 장애 응답 표준화 |
| 모델 추론 | DeepLog 아티팩트가 있으면 사용하고 실패 시 규칙 기반 fallback | MLflow Registry 연동, 모델 버전 자동 로딩 |
| 스트리밍 | Kafka raw topic을 읽어 feature topic으로 발행하는 PyFlink 작업과 inference worker | 운영 Kafka/Flink 배포 검증, backpressure/lag 관리 |
| 저장소 | PostgreSQL 메타데이터/feature/inference 테이블, ClickHouse writer 코드와 worker 연결 옵션 | schema migration 관리 |
| 학습 | 로컬 DeepLog 학습 스크립트, Airflow DAG 골격 | 데이터 준비, 평가 기준, MLflow logging/register/promote 구현 |
| 모니터링 | Prometheus metric과 drift 계산 함수 | 실제 reference/current 데이터 연결, Alertmanager/SMTP 룰 연결 |
| 배포 | Docker Compose, Helm, ArgoCD, GitHub Actions | Secret 참조, Workload Identity, streaming 이미지 전략 |

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

streaming inference worker까지 함께 실행하려면 profile을 활성화합니다.

```bash
docker compose --profile streaming up --build
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

MLflow는 Python 런타임 의존성이 아니라 Docker Compose/Kubernetes 서비스로 배포됩니다. Drain3가 구버전 `cachetools`를 고정하고 있어 최신 MLflow 클라이언트와 의존성 충돌이 발생할 수 있기 때문입니다. 현재 Airflow DAG는 운영 흐름을 설명하는 골격이며, 실제 MLflow experiment logging과 model registry promotion은 보강 대상입니다.

## 스트리밍 파이프라인

PyFlink 작업은 Kafka의 `hdfs.raw.logs` 토픽에서 원본 로그를 읽고, Drain3 파싱 및 윈도우 생성을 거쳐 `hdfs.feature.windows` 토픽으로 결과를 발행합니다. 윈도우가 아직 완성되지 않은 메시지는 `window: null`로 발행되고, inference worker는 완성된 윈도우만 추론합니다.

inference worker는 `hdfs.feature.windows`를 소비해 DeepLog 또는 규칙 기반 fallback으로 추론하고, `hdfs.anomalies`, `hdfs.inference.history` 토픽으로 결과를 발행합니다.

```bash
python -m flink.hdfs_log_job
```

```bash
python -m app.workers.inference_worker
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
- `model_fallbacks_total`
- `model_version`
- `prediction_confidence`

드리프트 계산 코드는 `monitoring/evidently`에 있고, SMTP 이메일 알림 코드는 `alerts`에 있습니다.

운영 알림 조건으로 사용할 기준:

- 데이터 드리프트 임계값 초과
- 이상 탐지 급증
- 추론 지연 시간 급증
- Kafka consumer lag 증가
- 모델 장애

현재 `/drift` 엔드포인트는 샘플 reference/current 데이터로 계산합니다. 운영에서는 ClickHouse 또는 PostgreSQL의 최근 추론 이력과 학습 기준 데이터를 연결해야 합니다.

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

추가로 점검해야 할 항목:

- Helm values의 평문 접속 정보 제거 및 Secret/External Secrets 참조
- API 이미지와 streaming job 이미지 분리 또는 streaming extra 포함 이미지 빌드
- 모델 아티팩트 다운로드/initContainer 또는 shared volume 전략
- PostgreSQL/ClickHouse schema migration 절차
- Prometheus alert rule과 SMTP/Alertmanager 연결

API 이미지는 `Dockerfile`, streaming/Flink/worker 이미지는 `Dockerfile.streaming`을 기준으로 빌드할 수 있습니다.

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

macOS 등 일부 환경에서 `python` 명령이 없으면 `python3`를 사용합니다. bytecode cache 권한 문제가 있으면 아래처럼 캐시 위치를 작업 가능한 디렉터리로 지정합니다.

```bash
PYTHONPYCACHEPREFIX=/private/tmp/hdfs_pycache python3 -m compileall app feature_store monitoring alerts flink scripts tests
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
