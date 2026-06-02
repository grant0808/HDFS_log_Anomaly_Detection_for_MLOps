# HDFS 로그 이상 탐지 플랫폼 — 아키텍처 문서

> Kubernetes 기반 MLOps 플랫폼으로, HDFS 로그를 실시간으로 수집·파싱·추론하고  
> DeepLog LSTM 모델로 이상을 탐지합니다.

---

## 1. 전체 시스템 아키텍처

```mermaid
flowchart TB
    subgraph SOURCE["📥 데이터 소스"]
        HDFS["HDFS 클러스터<br/>Raw Logs"]
        SCRIPT["produce_sample_logs.py<br/>샘플 생성기"]
    end

    subgraph STREAMING["⚡ 실시간 스트리밍 계층 (PyFlink + Kafka)"]
        direction LR
        RAW_TOPIC["Kafka Topic<br/>hdfs.raw.logs"]
        FLINK["PyFlink Job<br/>hdfs-log-drain3-feature-inference"]
        PARSED_TOPIC["Kafka Topic<br/>hdfs.parsed.events"]
        FEATURE_TOPIC["Kafka Topic<br/>hdfs.feature.windows"]
        ANOMALY_TOPIC["Kafka Topic<br/>hdfs.anomalies"]
        INF_TOPIC["Kafka Topic<br/>hdfs.inference.history"]

        RAW_TOPIC --> FLINK
        FLINK -->|"Drain3 파싱 + 슬라이딩 윈도우"| PARSED_TOPIC
        FLINK --> FEATURE_TOPIC
    end

    subgraph API["🚀 FastAPI 추론 서비스"]
        direction TB
        PARSER["HDFSLogParser<br/>(Drain3)"]
        WINDOW["SlidingWindowBuilder<br/>sequence_length=10"]
        INFERENCE["InferenceService"]
        DEEPLOG["DeepLog LSTM<br/>모델"]
        RULES["규칙 기반<br/>Fallback"]

        PARSER --> WINDOW
        WINDOW --> INFERENCE
        INFERENCE --> DEEPLOG
        INFERENCE --> RULES
    end

    subgraph STORAGE["🗄️ 스토리지 계층"]
        PG["PostgreSQL<br/>메타데이터 / Feature Store"]
        CH["ClickHouse<br/>추론 이력 / 이상 탐지 로그"]
        REDIS["Redis<br/>캐시 / 세션"]
        GCS["GCS<br/>원본·처리 데이터 / 모델 아티팩트"]
        FS["Feature Store<br/>feature_store/store.py"]

        PG --- FS
    end

    subgraph TRAINING["🎓 학습 파이프라인 (Airflow + MLflow)"]
        direction LR
        AF_DAG["Airflow DAG<br/>hdfs_deeplog_training<br/>@daily"]
        COLLECT["collect_logs"]
        PREPARE["prepare_dataset"]
        TRAIN["train<br/>DeepLog LSTM"]
        EVALUATE["evaluate<br/>accuracy / top-k"]
        REGISTER["register_model<br/>MLflow Registry"]
        DEPLOY_TASK["deploy"]

        AF_DAG --> COLLECT --> PREPARE --> TRAIN --> EVALUATE --> REGISTER --> DEPLOY_TASK
    end

    subgraph MONITORING["📊 모니터링 & 알림"]
        PROM["Prometheus<br/>메트릭 스크래핑"]
        GRAFANA["Grafana<br/>대시보드"]
        EVIDENTLY["Evidently<br/>데이터 드리프트"]
        ALERT["SMTP Alert<br/>emailer.py"]

        PROM --> GRAFANA
        EVIDENTLY --> ALERT
    end

    subgraph CICD["🔄 CI/CD (GitOps)"]
        GITHUB["GitHub Actions<br/>mlops.yml"]
        REGISTRY["GCP Artifact Registry<br/>Docker Image"]
        HELM["Helm Chart<br/>hdfs-log-anomaly"]
        ARGOCD["ArgoCD<br/>자동 동기화"]
        K8S["Kubernetes<br/>hdfs-observability"]

        GITHUB -->|"빌드·푸시"| REGISTRY
        GITHUB -->|"Helm lint"| HELM
        GITHUB -->|"ArgoCD sync"| ARGOCD
        HELM --> ARGOCD
        ARGOCD -->|"GitOps 자동 배포"| K8S
    end

    HDFS --> RAW_TOPIC
    SCRIPT --> RAW_TOPIC
    PARSED_TOPIC --> API
    API -->|"추론 결과"| ANOMALY_TOPIC
    API -->|"추론 이력"| INF_TOPIC
    API --> PG
    API --> CH
    API --> PROM
    API --> EVIDENTLY

    REGISTER -->|"model.pt / vocab.json"| GCS
    GCS -->|"아티팩트 로드"| DEEPLOG

    K8S -.->|"운영"| API
```

---

## 2. 실시간 스트리밍 파이프라인

```mermaid
flowchart LR
    subgraph INPUT["입력"]
        LOG["HDFS 원본 로그\ne.g. 081109 204218 143 INFO dfs.DataNode..."]
    end

    subgraph KAFKA_IN["Kafka"]
        T1["hdfs.raw.logs"]
    end

    subgraph FLINK_PROC["PyFlink 처리"]
        DRAIN3["Drain3 파서\n템플릿 추출"]
        META["메타데이터 생성\ntemplate_id / event_id\nblock_id / host / sequence_id"]
        SLIDE["슬라이딩 윈도우\nsequence_length=10"]
        FEAT["Feature 추출\nextract_window_features()"]
    end

    subgraph KAFKA_OUT["Kafka 출력 토픽"]
        T2["hdfs.parsed.events"]
        T3["hdfs.feature.windows"]
        T4["hdfs.anomalies"]
        T5["hdfs.inference.history"]
    end

    LOG --> T1
    T1 --> DRAIN3
    DRAIN3 --> META
    META --> SLIDE
    SLIDE --> FEAT
    FEAT --> T2
    FEAT --> T3
    T3 -->|"FastAPI 추론 후"| T4
    T3 -->|"FastAPI 추론 후"| T5
```

---

## 3. 추론 서비스 내부 흐름

```mermaid
flowchart TD
    REQ["POST /predict\n{sequence, actual_event, metadata}"]

    subgraph INFER["InferenceService"]
        LOAD["모델 로드 확인\nDeepLogModel.available?"]
        DL_PATH["DeepLog LSTM\n상위 K개 예측 이벤트\ntop_k=3"]
        RULE_PATH["규칙 기반 Fallback\nrules.py"]
        DECISION{"actual_event\n∈ top-k?"}
        NORMAL["정상 판정\nanomaly=False"]
        ANOMALY["이상 판정\nanomaly=True"]
    end

    subgraph PERSIST["저장 / 발행"]
        PG_SAVE["PostgreSQL\ninference 기록"]
        CH_SAVE["ClickHouse\n이상 탐지 이력"]
        PROM_INC["Prometheus 메트릭\nanomalies_total++\ninference_latency"]
    end

    RESP["PredictResponse\n{anomaly, confidence, latency_ms, top_k_events}"]

    REQ --> LOAD
    LOAD -->|"가용"| DL_PATH
    LOAD -->|"불가"| RULE_PATH
    DL_PATH --> DECISION
    RULE_PATH --> DECISION
    DECISION -->|"Yes"| NORMAL
    DECISION -->|"No"| ANOMALY
    NORMAL --> PG_SAVE
    ANOMALY --> PG_SAVE
    ANOMALY --> CH_SAVE
    PG_SAVE --> PROM_INC
    PROM_INC --> RESP
```

---

## 4. 학습 파이프라인 (Airflow DAG)

```mermaid
flowchart LR
    START(["Airflow Scheduler\n@daily"])

    subgraph DAG["hdfs_deeplog_training DAG"]
        COLLECT["collect_logs\nGCS raw bucket URI 반환"]
        PREPARE["prepare_dataset\nraw → processed Parquet"]
        TRAIN["train\nDeepLog LSTM 학습\nartifacts/model.pt"]
        EVALUATE["evaluate\naccuracy / top_k 측정"]
        REGISTER["register_model\nMLflow Model Registry 등록"]
        DEPLOY["deploy\n모델 참조 URL 반환"]
    end

    subgraph STORAGE["스토리지"]
        GCS_RAW["GCS\nhdfs-raw-logs/*"]
        GCS_PROC["GCS\nhdfs-processed-logs/*.parquet"]
        GCS_ART["GCS\nhdfs-model-artifacts/model.pt"]
        MLFLOW["MLflow\nExperiment Tracking\nModel Registry"]
    end

    START --> COLLECT
    COLLECT --> PREPARE
    PREPARE --> TRAIN
    TRAIN --> EVALUATE
    EVALUATE --> REGISTER
    REGISTER --> DEPLOY

    COLLECT -.-> GCS_RAW
    PREPARE -.-> GCS_PROC
    TRAIN -.-> GCS_ART
    EVALUATE -.-> MLFLOW
    REGISTER -.-> MLFLOW
```

---

## 5. CI/CD 및 GitOps 배포 파이프라인

```mermaid
flowchart TD
    DEV["개발자 Push / PR\nGitHub main 브랜치"]

    subgraph GHA["GitHub Actions (mlops.yml)"]
        TEST["pytest\n단위·API·드리프트 테스트"]
        LINT["ruff check\n정적 분석"]
        BUILD["Docker Build\nDockerfile"]
        PUSH["GCP Artifact Registry 푸시\nus-central1-docker.pkg.dev/.../hdfs-log-anomaly:SHA"]
        HELM_LINT["helm lint\nhelm/hdfs-log-anomaly"]
        ARGOCD_SYNC["ArgoCD sync\n자동 동기화 트리거"]
    end

    subgraph GITOPS["GitOps (ArgoCD)"]
        ARGO_APP["ArgoCD Application\nhdfs-log-anomaly"]
        ARGO_WATCH["Git 저장소 감시\ntargetRevision: main"]
        ARGO_APPLY["Helm 차트 렌더링\n+ kubectl apply"]
    end

    subgraph K8S_NS["Kubernetes (hdfs-observability)"]
        FASTAPI_POD["FastAPI Pod"]
        KAFKA_SVC["Kafka Service"]
        FLINK_POD["Flink Job Pod"]
        AIRFLOW_SVC["Airflow Service"]
        MLFLOW_SVC["MLflow Service"]
        PROM_SVC["Prometheus Service"]
        GRAFANA_SVC["Grafana Service"]
        INGRESS["Ingress Controller"]
    end

    DEV --> TEST
    TEST --> LINT
    LINT --> BUILD
    BUILD --> PUSH
    PUSH --> HELM_LINT
    HELM_LINT --> ARGOCD_SYNC
    ARGOCD_SYNC --> ARGO_APP
    ARGO_APP --> ARGO_WATCH
    ARGO_WATCH --> ARGO_APPLY
    ARGO_APPLY --> FASTAPI_POD
    ARGO_APPLY --> KAFKA_SVC
    ARGO_APPLY --> FLINK_POD
    ARGO_APPLY --> AIRFLOW_SVC
    ARGO_APPLY --> MLFLOW_SVC
    ARGO_APPLY --> PROM_SVC
    ARGO_APPLY --> GRAFANA_SVC
    INGRESS --> FASTAPI_POD
```

---

## 6. 모니터링 & 알림 아키텍처

```mermaid
flowchart LR
    subgraph METRICS_SRC["메트릭 소스"]
        API_METRICS["FastAPI /metrics\n(Prometheus Client)"]
    end

    subgraph PROM_STACK["Prometheus 스택"]
        PROM["Prometheus\nprometheus.yml\n스크래핑 주기 설정"]
        GRAFANA["Grafana\n대시보드 시각화"]
    end

    subgraph DRIFT["드리프트 감지 (Evidently)"]
        DRIFT_SVC["DriftService\ncompute(reference_df, current_df)"]
        DRIFT_SCORE["data_drift_score\n임계값: 0.35"]
    end

    subgraph ALERT_SYS["알림 시스템"]
        EMAILER["emailer.py\nSMTP 이메일 발송"]
        MAILHOG["MailHog\n(로컬 개발 SMTP)"]
    end

    subgraph ALERT_COND["알림 조건"]
        C1["데이터 드리프트\n> 0.35"]
        C2["이상 탐지 급증\n> 25건"]
        C3["추론 지연\n> 250ms"]
        C4["Kafka Consumer Lag\n> 1000"]
        C5["모델 장애\navailable=False"]
    end

    subgraph KEY_METRICS["주요 Prometheus 메트릭"]
        M1["logs_processed_total"]
        M2["anomalies_total"]
        M3["anomaly_rate"]
        M4["inference_latency"]
        M5["data_drift_score"]
        M6["consumer_lag"]
        M7["unknown_template_count"]
        M8["model_version"]
        M9["prediction_confidence"]
    end

    API_METRICS --> PROM
    PROM --> GRAFANA
    PROM --> KEY_METRICS

    DRIFT_SVC --> DRIFT_SCORE
    DRIFT_SCORE --> C1
    C1 --> EMAILER
    C2 --> EMAILER
    C3 --> EMAILER
    C4 --> EMAILER
    C5 --> EMAILER
    EMAILER --> MAILHOG
```

---

## 7. 스토리지 계층 구성

```mermaid
erDiagram
    POSTGRESQL {
        string sequence_id PK
        timestamp timestamp
        string actual_event
        boolean anomaly
        float confidence
        float latency_ms
        jsonb payload
    }

    CLICKHOUSE {
        string sequence_id PK
        datetime timestamp
        boolean anomaly
        float confidence
        float latency_ms
        string template_id
    }

    FEATURE_STORE {
        string feature_key PK
        jsonb feature_value
        timestamp created_at
        int version
    }

    GCS_RAW {
        string bucket "hdfs-raw-logs"
        string path "hdfs/*.log"
    }

    GCS_PROCESSED {
        string bucket "hdfs-processed-logs"
        string path "*.parquet"
    }

    GCS_ARTIFACTS {
        string bucket "hdfs-model-artifacts"
        string model_pt "model.pt"
        string vocab_json "vocab.json"
    }

    REDIS {
        string key
        string value
        int ttl
    }

    MLFLOW_REGISTRY {
        string model_name "hdfs-deeplog"
        string version
        string stage "Staging / Production"
        string artifact_uri
    }

    POSTGRESQL ||--o{ FEATURE_STORE : "저장"
    GCS_RAW ||--|| GCS_PROCESSED : "ETL 처리"
    GCS_ARTIFACTS ||--|| MLFLOW_REGISTRY : "아티팩트 등록"
```

---

## 8. Kubernetes 리소스 구성

```mermaid
flowchart TB
    subgraph NS["Namespace: hdfs-observability"]
        subgraph CORE["핵심 서비스"]
            FASTAPI["Deployment: fastapi\n포트 8000"]
            FLINK["Deployment: flink\n스트리밍 처리"]
            AIRFLOW["Deployment: airflow\n학습 DAG 스케줄러"]
        end

        subgraph DATA["데이터 서비스"]
            KAFKA["StatefulSet: kafka\n포트 9092"]
            PG["StatefulSet: postgres\n포트 5432"]
            CH["StatefulSet: clickhouse\n포트 8123/9000"]
            REDIS_K8S["StatefulSet: redis\n포트 6379"]
        end

        subgraph OBS["관측 서비스"]
            MLFLOW_K8S["Deployment: mlflow\n포트 5000"]
            PROM_K8S["Deployment: prometheus\n포트 9090"]
            GRAFANA_K8S["Deployment: grafana\n포트 3000"]
            EVIDENTLY_K8S["Deployment: evidently\n드리프트 계산"]
        end

        subgraph NET["네트워크"]
            INGRESS_K8S["Ingress\nnginx / GCP LB"]
            SVC["ClusterIP / NodePort Services"]
        end

        subgraph GITOPS_NS["GitOps"]
            ARGOCD_NS["ArgoCD Application\nhdfs-log-anomaly\nnamespace: argocd"]
        end
    end

    INGRESS_K8S --> FASTAPI
    INGRESS_K8S --> GRAFANA_K8S
    INGRESS_K8S --> MLFLOW_K8S
    FASTAPI --> KAFKA
    FASTAPI --> PG
    FASTAPI --> CH
    FASTAPI --> REDIS_K8S
    FASTAPI --> PROM_K8S
    FLINK --> KAFKA
    AIRFLOW --> MLFLOW_K8S
    ARGOCD_NS -.->|"자동 동기화"| CORE
    ARGOCD_NS -.->|"자동 동기화"| DATA
    ARGOCD_NS -.->|"자동 동기화"| OBS
```

---

## 9. 로컬 개발 환경 (Docker Compose)

```mermaid
flowchart LR
    subgraph DC["Docker Compose 서비스"]
        direction TB
        API_DC["api:8000\nFastAPI"]
        PG_DC["postgres:5432\nPostgreSQL 16"]
        REDIS_DC["redis:6379\nRedis 7"]
        CH_DC["clickhouse:8123\nClickHouse 24.3"]
        ZK["zookeeper:2181\nConfluentInc"]
        KAFKA_DC["kafka:9092\nConfluentInc 7.6.1"]
        MLFLOW_DC["mlflow:5000\nv2.13.0"]
        PROM_DC["prometheus:9090\nv2.52.0"]
        GRAFANA_DC["grafana:3000\nv10.4.2"]
        MH["mailhog:8025\nSMTP 1025"]
    end

    DEV_USER["개발자 브라우저"]
    DEV_USER -->|"FastAPI Docs"| API_DC
    DEV_USER -->|"대시보드"| GRAFANA_DC
    DEV_USER -->|"실험 추적"| MLFLOW_DC
    DEV_USER -->|"메트릭"| PROM_DC
    DEV_USER -->|"이메일 테스트"| MH

    API_DC --> PG_DC
    API_DC --> REDIS_DC
    API_DC --> CH_DC
    API_DC --> KAFKA_DC
    API_DC --> MH
    KAFKA_DC --> ZK
    PROM_DC -.->|"스크래핑"| API_DC
    GRAFANA_DC -.->|"쿼리"| PROM_DC
```

---

## 10. 컴포넌트 책임 요약

| 컴포넌트 | 역할 | 기술 스택 |
|---|---|---|
| **FastAPI** | REST 추론 API, 파싱, 메트릭 노출 | Python, Uvicorn, Prometheus Client |
| **PyFlink** | 실시간 로그 스트리밍 처리 | Apache Flink, PyFlink |
| **Kafka** | 이벤트 버스 (5개 토픽) | Apache Kafka, Zookeeper |
| **Drain3** | 로그 템플릿 추출 파서 | Drain3 (UIUC) |
| **DeepLog** | LSTM 기반 이상 탐지 모델 | PyTorch |
| **Airflow** | 학습 파이프라인 스케줄러 | Apache Airflow, @daily DAG |
| **MLflow** | 실험 추적, 모델 레지스트리 | MLflow v2.13 |
| **PostgreSQL** | 메타데이터, Feature Store | PostgreSQL 16 |
| **ClickHouse** | 추론 이력, 대용량 로그 분석 | ClickHouse 24.3 |
| **Redis** | 캐시, 세션 상태 | Redis 7 |
| **GCS** | 원본 로그, 처리 데이터, 모델 아티팩트 | Google Cloud Storage |
| **Prometheus** | 메트릭 수집 및 저장 | Prometheus v2.52 |
| **Grafana** | 시각화 대시보드 | Grafana v10.4 |
| **Evidently** | 데이터 드리프트 감지 | Evidently AI |
| **SMTP/MailHog** | 알림 이메일 발송 | MailHog (개발), SMTP (운영) |
| **ArgoCD** | GitOps 자동 배포 | ArgoCD |
| **Helm** | Kubernetes 패키지 관리 | Helm 3 |
| **GitHub Actions** | CI/CD 자동화 | GitHub Actions |
| **GCP Artifact Registry** | Docker 이미지 저장소 | Google Artifact Registry |

---

## 11. 데이터 흐름 요약

```mermaid
sequenceDiagram
    participant HDFS as HDFS 클러스터
    participant KAFKA as Apache Kafka
    participant FLINK as PyFlink
    participant API as FastAPI
    participant DL as DeepLog LSTM
    participant PG as PostgreSQL
    participant CH as ClickHouse
    participant PROM as Prometheus
    participant GRAFANA as Grafana
    participant ALERT as SMTP 알림

    HDFS->>KAFKA: 원본 로그 전송 (hdfs.raw.logs)
    KAFKA->>FLINK: 로그 소비
    FLINK->>FLINK: Drain3 파싱 + 슬라이딩 윈도우
    FLINK->>KAFKA: 파싱 이벤트 발행 (hdfs.parsed.events)
    KAFKA->>API: 이벤트 수신
    API->>DL: 시퀀스 추론 요청
    DL->>API: top-k 예측 이벤트 반환
    API->>PG: 추론 결과 저장
    API->>CH: 이상 탐지 이력 저장
    API->>KAFKA: 이상 이벤트 발행 (hdfs.anomalies)
    API->>PROM: 메트릭 노출 (/metrics)
    PROM->>GRAFANA: 메트릭 수집 및 시각화
    PROM->>ALERT: 임계값 초과 시 알림 트리거
    ALERT->>ALERT: SMTP 이메일 발송
```

---

*이 문서는 프로젝트 코드베이스를 기반으로 자동 생성되었습니다.*  
*최종 업데이트: 2026-06-02*
