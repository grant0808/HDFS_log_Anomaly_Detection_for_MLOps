# HDFS Log Anomaly Detection Architecture

이 문서는 HDFS 로그 이상 탐지 플랫폼의 현재 구현, 목표 운영 아키텍처, 운영 전 보강 항목을 분리해서 설명합니다.

## 1. 목표

- HDFS 로그를 실시간 또는 준실시간으로 수집한다.
- Drain3로 로그 템플릿과 이벤트 ID를 추출한다.
- 이벤트 시퀀스를 윈도우 단위 feature로 변환한다.
- DeepLog 모델로 다음 이벤트를 예측하고, 예측 실패 또는 모델 부재 시 규칙 기반 fallback을 사용한다.
- 추론 결과, 이상 이력, 드리프트 지표, 시스템 지표를 관측 저장소에 남긴다.
- GitHub Actions, Artifact Registry, Helm, ArgoCD로 Kubernetes에 반복 가능하게 배포한다.

## 2. 현재 구현 아키텍처

```text
Developer
  |
  v
FastAPI
  |-- POST /parse
  |     |
  |     v
  |   HDFSLogParser(Drain3) -> parsed_logs(PostgreSQL or local SQLite)
  |
  |-- POST /predict
  |     |
  |     v
  |   InferenceService
  |     |-- DeepLog model artifacts: artifacts/model.pt, artifacts/vocab.json
  |     |-- RuleDetector fallback
  |     v
  |   inference_history(PostgreSQL or local SQLite)
  |
  |-- GET /metrics -> Prometheus format
  |-- GET /drift   -> sample reference/current drift report
  |-- GET /anomalies

Kafka hdfs.raw.logs
  |
  v
PyFlink hdfs_log_job
  |
  |-- Drain3 parsing
  |-- Sliding window feature extraction
  v
Kafka hdfs.parsed.events
```

현재 구현은 API 기반 파싱/추론과 Kafka 기반 파싱 작업을 각각 제공합니다. 스트리밍 작업이 FastAPI 추론 또는 anomaly topic까지 이어지는 구조는 아직 연결되어 있지 않습니다.

## 3. 목표 운영 아키텍처

```text
                +-------------------+
                | GitHub Actions    |
                | tests/build/lint  |
                +---------+---------+
                          |
                          v
                +-------------------+
                | Artifact Registry |
                +---------+---------+
                          |
                          v
                +-------------------+
                | ArgoCD + Helm     |
                +---------+---------+
                          |
                          v
+-------------------------------------------------------------------+
| Kubernetes / GKE                                                   |
|                                                                   |
|  HDFS Logs                                                        |
|     |                                                             |
|     v                                                             |
|  Kafka: hdfs.raw.logs                                             |
|     |                                                             |
|     v                                                             |
|  PyFlink Parser + Window Job                                      |
|     |                                                             |
|     |-- parsed events/features -> Kafka: hdfs.feature.windows     |
|     |                                                             |
|     v                                                             |
|  Inference Worker or FastAPI /predict                             |
|     |                                                             |
|     |-- anomaly result -> Kafka: hdfs.anomalies                   |
|     |-- inference event -> Kafka: hdfs.inference.history          |
|     |-- metadata/features -> PostgreSQL                           |
|     |-- analytical history -> ClickHouse                          |
|                                                                   |
|  FastAPI                                                          |
|     |-- online parse/predict API                                  |
|     |-- health/readiness/metrics                                  |
|                                                                   |
|  Airflow Training Pipeline                                        |
|     |-- collect raw logs from GCS                                 |
|     |-- build training dataset                                    |
|     |-- train DeepLog                                             |
|     |-- evaluate model                                            |
|     |-- log/register in MLflow                                    |
|     |-- promote approved model                                    |
|                                                                   |
|  Monitoring                                                       |
|     |-- Prometheus metrics                                        |
|     |-- Grafana dashboards                                        |
|     |-- drift job using reference/current data                    |
|     |-- Alertmanager or SMTP alerts                               |
+-------------------------------------------------------------------+

External/Managed Storage:
  GCS: raw logs, processed logs, datasets, model artifacts
  PostgreSQL: metadata, feature values, model registry metadata
  ClickHouse: high-volume inference/anomaly history
  Redis: cache or low-latency state
  MLflow: experiment tracking and model registry
```

## 4. 데이터 흐름

1. HDFS 로그가 `hdfs.raw.logs` Kafka topic에 적재됩니다.
2. PyFlink 작업이 원본 로그를 읽고 Drain3 템플릿, `event_id`, `block_id`, `sequence_id`를 생성합니다.
3. Sliding window builder가 이벤트 시퀀스를 만들고 feature를 계산합니다.
4. 운영 목표에서는 inference worker가 feature window를 읽어 DeepLog 추론을 수행합니다.
5. DeepLog 아티팩트가 없거나 추론에 실패하면 RuleDetector fallback이 동작합니다.
6. 이상 결과는 `hdfs.anomalies`, 추론 이력은 `hdfs.inference.history`에 발행됩니다.
7. PostgreSQL은 메타데이터와 feature store 역할을 담당하고, ClickHouse는 대량 추론 이력 분석을 담당합니다.
8. Prometheus/Grafana는 API/모델/스트리밍 지표를 관측하고, drift job은 reference dataset과 최근 데이터를 비교합니다.

## 5. 학습 및 모델 운영 흐름

```text
GCS raw logs
  -> Airflow prepare_dataset
  -> DeepLog training
  -> evaluation
  -> MLflow experiment
  -> MLflow Model Registry
  -> approval/promotion
  -> model artifact published to GCS
  -> inference deployment reload or rollout
```

현재 `app/training/train_deeplog.py`는 로컬 샘플 데이터 학습을 지원합니다. `airflow/dags/hdfs_training_pipeline.py`는 위 흐름의 골격이며, 실제 GCS read/write, MLflow logging, model promotion, deployment trigger는 보강 대상입니다.

## 6. 저장소별 책임

| 저장소 | 책임 | 현재 상태 |
| --- | --- | --- |
| PostgreSQL | parsed logs, feature values, inference metadata, model registry metadata | SQLAlchemy table 생성 및 API 일부 연결 |
| ClickHouse | 대량 inference/anomaly history 분석 | writer 코드와 schema 예시 존재, API 연결 미완 |
| Redis | cache, low-latency state, future online feature serving | 설정과 배포 골격 존재 |
| GCS | raw logs, processed logs, datasets, model artifacts | 설정과 배포 가이드 존재 |
| MLflow | experiment tracking, registry, promotion | 서비스 배포 골격 존재, Python client 연동 미완 |

## 7. 운영 전 우선 보강 항목

### P0: 실행 경로 정합성

- API 이미지와 streaming job 이미지 전략을 결정합니다.
- Helm `flink-submit` Job이 실행할 이미지에 `flink/` 코드와 `apache-flink` 의존성을 포함합니다.
- `hdfs.feature.windows`, `hdfs.anomalies`, `hdfs.inference.history` topic 생산자를 구현합니다.
- FastAPI 또는 별도 inference worker가 Kafka feature window를 소비하도록 연결합니다.

### P1: 모델 운영

- 학습 DAG에서 실제 GCS dataset을 읽고 학습 결과를 MLflow에 기록합니다.
- 평가 기준을 정의합니다: top-k accuracy, false positive rate, latency, fallback rate.
- 모델 승격 정책을 둡니다: staging -> production approval gate.
- inference 서비스가 `MODEL_VERSION`, `MODEL_PATH`, `VOCAB_PATH`를 배포 시점에 명확히 받도록 만듭니다.

### P1: 관측과 알림

- `/drift`를 샘플 DataFrame이 아니라 reference dataset과 최근 inference history에 연결합니다.
- Prometheus alert rule을 추가합니다.
- Alertmanager 또는 SMTP 발송 경로를 실제 메트릭 임계값과 연결합니다.
- Kafka consumer lag, model fallback rate, unknown template rate를 핵심 SLO 지표로 관리합니다.

### P2: 보안과 배포

- Helm values의 평문 DB 접속 정보를 Secret 또는 External Secrets로 이동합니다.
- GKE Workload Identity serviceAccount를 Helm chart에 반영합니다.
- PostgreSQL/ClickHouse schema migration 절차를 추가합니다.
- Docker image vulnerability scan과 dependency lock 정책을 추가합니다.

## 8. 권장 디렉터리 확장

```text
app/workers/              Kafka inference worker
app/storage/              ClickHouse/PostgreSQL write adapters
airflow/include/          reusable training operators/scripts
helm/hdfs-log-anomaly/    API chart with Secret/serviceAccount support
helm/hdfs-streaming/      optional streaming job chart
monitoring/alerts/        Prometheus alert rules
docs/runbooks/            incident and operation runbooks
```

## 9. 운영 검증 체크리스트

- `docker compose up --build` 후 `/health`, `/predict`, `/metrics`가 정상 응답합니다.
- 샘플 로그 produce 후 Kafka parsed topic에 메시지가 생성됩니다.
- inference worker가 feature window를 소비하고 anomaly/history topic에 결과를 발행합니다.
- PostgreSQL에 parsed log와 inference history가 저장됩니다.
- ClickHouse에 inference history가 적재되고 Grafana에서 조회됩니다.
- MLflow에 학습 run, metrics, model artifact가 기록됩니다.
- ArgoCD sync 후 API readiness/liveness probe가 정상입니다.
- drift score, anomaly rate, inference latency, consumer lag alert가 의도대로 발생합니다.
