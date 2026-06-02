# 🛠️ 직접 수정해야 하는 설정 체크리스트

> **로컬(docker-compose)** 과 **GCP(K8s/Helm)** 두 가지 실행 환경 기준으로 정리합니다.
> 아래 체크박스를 하나씩 완료하면서 진행하세요.

---

## ⚡ 시나리오별 최소 변경 요약 (빠른 참고)

| 시나리오 | 필수 변경 항목 |
|---------|---------------|
| **A. 로컬 uvicorn 직접 실행** | ① `model.pt` 준비 (or Fallback) ② (선택) `.env` 에 `POSTGRES_DSN` 설정 |
| **B. Docker Compose 전체 스택** | ① `model.pt` 준비 (or Fallback) ② (선택) DB 비밀번호 변경 |
| **C. GCP / Kubernetes 프로덕션** | ① `values.yaml` 전체 ② ArgoCD `repoURL` ③ GitHub Secrets/Variables 4+2개 ④ K8s Secret 생성 ⑤ GCS 버킷 생성 ⑥ GKE 클러스터 생성 ⑦ Airflow Variable |

---

## 📋 목차

1. [공통 사전 준비](#1-공통-사전-준비)
2. [로컬 실행 (docker-compose)](#2-로컬-실행-docker-compose)
3. [GCP / Kubernetes 배포](#3-gcp--kubernetes-배포)
4. [GitHub Actions CI/CD](#4-github-actions-cicd)
5. [ArgoCD GitOps](#5-argocd-gitops)
6. [Airflow DAG 변수](#6-airflow-dag-변수)
7. [알림(SMTP) 설정](#7-알림smtp-설정)
8. [모델 아티팩트 준비](#8-모델-아티팩트-준비)
9. [Flink 스트리밍 잡 설정](#9-flink-스트리밍-잡-설정)
10. [스크립트 설정](#10-스크립트-설정)
11. [알림 임계값 튜닝](#11-알림-임계값-튜닝)


---

## 1. 공통 사전 준비

| 항목 | 설명 |
|------|------|
| Python 3.11+ | `python --version` 으로 확인 |
| Docker | `docker version` 으로 확인 |
| `artifacts/model.pt` | **학습된 DeepLog 모델 파일 필요** (현재 없음 → [8번](#8-모델-아티팩트-준비) 참고) |

---

## 2. 로컬 실행 (docker-compose)

### 2-1. `.env` 파일 생성 (선택 사항)

`app/config.py` 는 `.env` 파일을 자동으로 읽습니다.
변경하고 싶은 값만 `.env` 에 작성하면 됩니다.

**파일 위치:** 프로젝트 루트 `.env`

```dotenv
# ── 필수: 실제 이메일 수신 알림이 필요하면 변경 ──────────────────────
SMTP_HOST=mailhog            # 로컬에서는 mailhog(기본값) 그대로 유지 OK
SMTP_PORT=1025
SMTP_FROM=hdfs-alerts@example.com   # ← 원하는 발신 주소로 변경
SMTP_TO=mlops@example.com           # ← 실제 수신 이메일 주소로 변경

# ── GCS를 사용하려면 변경 (로컬에서는 그냥 두어도 됨) ────────────────
GCP_PROJECT_ID=local-dev
GCS_RAW_BUCKET=hdfs-raw-logs
GCS_PROCESSED_BUCKET=hdfs-processed-logs
GCS_ARTIFACTS_BUCKET=hdfs-model-artifacts
```

> **로컬 기본값으로 바로 실행 가능 여부:**
> - PostgreSQL, Redis, Kafka, ClickHouse, Prometheus, Grafana, MailHog 모두 docker-compose가 띄워줌
> - GCS 관련 기능(업로드/다운로드)은 로컬에서는 사용 안 해도 됨

### 2-2. `docker-compose.yml` — 비밀번호 변경 (운영 환경만)

**파일 위치:** [`docker-compose.yml`](docker-compose.yml)

로컬 개발에서는 기본값(`hdfs`/`hdfs`) 그대로 사용해도 됩니다.
운영 목적이라면 아래 값을 강력한 패스워드로 교체하세요.

```yaml
# Line 34-36
postgres:
  environment:
    POSTGRES_USER: hdfs           # ← 변경 권장 (운영 시)
    POSTGRES_PASSWORD: hdfs       # ← 변경 권장 (운영 시)
    POSTGRES_DB: hdfs

# Line 6 (api 서비스)
POSTGRES_DSN: postgresql+psycopg://hdfs:hdfs@postgres:5432/hdfs
# ↑ 위 비밀번호 바꾸면 여기도 같이 변경
```

### 2-3. `monitoring/prometheus.yml` — 스크랩 대상

**파일 위치:** [`monitoring/prometheus.yml`](monitoring/prometheus.yml)

현재 `api:8000` 으로 고정되어 있습니다.
docker-compose 내부 네트워크에서는 변경 불필요.
외부 서비스를 추가 모니터링하려면 `targets` 항목 추가:

```yaml
# Line 7
static_configs:
  - targets: ["api:8000"]   # ← 추가 서비스가 있으면 여기에 추가
```

### 2-4. 실행 명령

```bash
# 기본 실행
docker compose up -d

# 스트리밍 워커 포함 실행
docker compose --profile streaming up -d
```

---

## 3. GCP / Kubernetes 배포

### 3-1. `helm/hdfs-log-anomaly/values.yaml` — **핵심 수정 파일**

**파일 위치:** [`helm/hdfs-log-anomaly/values.yaml`](helm/hdfs-log-anomaly/values.yaml)

| 키 | 현재 값 (플레이스홀더) | 변경 방법 |
|----|----------------------|-----------|
| `image.repository` | `us-central1-docker.pkg.dev/local-dev/mlops/hdfs-log-anomaly` | `{REGION}-docker.pkg.dev/{PROJECT_ID}/mlops/hdfs-log-anomaly` |
| `streamingImage.repository` | `us-central1-docker.pkg.dev/local-dev/mlops/hdfs-log-anomaly-streaming` | `{REGION}-docker.pkg.dev/{PROJECT_ID}/mlops/hdfs-log-anomaly-streaming` |
| `image.tag` | `latest` | 실제 배포 태그 (예: `v0.1.0`) |
| `env.GCP_PROJECT_ID` | `local-dev` | 실제 GCP 프로젝트 ID |
| `env.GCS_RAW_BUCKET` | `hdfs-raw-logs` | `{PROJECT_ID}-hdfs-raw-logs` |
| `env.GCS_PROCESSED_BUCKET` | `hdfs-processed-logs` | `{PROJECT_ID}-hdfs-processed-logs` |
| `env.GCS_ARTIFACTS_BUCKET` | `hdfs-model-artifacts` | `{PROJECT_ID}-hdfs-model-artifacts` |
| `ingress.host` | `hdfs-anomaly.local` | 실제 도메인 (예: `hdfs-anomaly.example.com`) |
| `serviceAccount.annotations` | `{}` (비어있음) | Workload Identity 설정 시 아래 추가 필요 |

**Workload Identity 어노테이션 추가 예시:**
```yaml
serviceAccount:
  annotations:
    iam.gke.io/gcp-service-account: hdfs-anomaly-platform@{PROJECT_ID}.iam.gserviceaccount.com
```

### 3-2. `k8s/ingress/values.yaml` — 도메인 변경

**파일 위치:** [`k8s/ingress/values.yaml`](k8s/ingress/values.yaml)

```yaml
host: hdfs-anomaly.example.com   # ← 실제 도메인으로 변경
tls: true
```

### 3-3. `k8s/mlflow/values.yaml` — GCS 버킷 경로

**파일 위치:** [`k8s/mlflow/values.yaml`](k8s/mlflow/values.yaml)

```yaml
artifactRoot: gs://hdfs-model-artifacts/mlflow
# ↑ 실제 버킷명으로 변경: gs://{PROJECT_ID}-hdfs-model-artifacts/mlflow
```

### 3-4. Kubernetes Secret 생성 — **직접 실행 필요**

**파일 참고:** [`GCP_DEPLOYMENT_GUIDE.md`](GCP_DEPLOYMENT_GUIDE.md) §7

아래 명령을 실행할 때 `change-me`, `example.com` 부분을 실제 값으로 변경:

```bash
kubectl create secret generic hdfs-platform-secrets \
  -n hdfs-observability \
  --from-literal=POSTGRES_DSN="postgresql+psycopg://hdfs:{실제_비밀번호}@postgres:5432/hdfs" \
  --from-literal=REDIS_URL="redis://redis:6379/0" \
  --from-literal=CLICKHOUSE_PASSWORD="{실제_비밀번호}" \
  --from-literal=SMTP_HOST="smtp.{도메인}.com" \
  --from-literal=SMTP_PORT="587" \
  --from-literal=SMTP_USER="{발신_이메일}" \
  --from-literal=SMTP_PASSWORD="{실제_SMTP_비밀번호}" \
  --from-literal=SMTP_FROM="{발신_이메일}" \
  --from-literal=SMTP_TO="{수신_이메일}"
```

### 3-5. GCP 환경 변수 — `GCP_DEPLOYMENT_GUIDE.md` §1 참고

터미널에서 아래 변수를 **본인 환경에 맞게** 설정하세요:

```bash
export PROJECT_ID="your-gcp-project"        # ← 실제 GCP 프로젝트 ID
export REGION="asia-northeast3"             # ← 원하는 리전 (서울: asia-northeast3)
export ZONE="asia-northeast3-a"
export CLUSTER_NAME="hdfs-observability-gke"
export NAMESPACE="hdfs-observability"
export REPOSITORY="mlops"
export IMAGE_TAG="v0.1.0"                   # ← 배포 버전 태그
```

---

## 4. GitHub Actions CI/CD

**파일 위치:** [`.github/workflows/mlops.yml`](.github/workflows/mlops.yml)

GitHub 저장소 설정에서 아래 **Secrets** 및 **Variables** 를 추가해야 합니다.

### 4-1. Repository Secrets (Settings → Secrets → Actions)

| Secret 이름 | 설명 | 어디서 얻나 |
|-------------|------|------------|
| `GCP_WORKLOAD_IDENTITY_PROVIDER` | GCP Workload Identity Provider 리소스 이름 | GCP IAM → Workload Identity 설정 후 확인 |
| `GCP_SERVICE_ACCOUNT` | GCP 서비스 계정 이메일 | 예: `hdfs-anomaly-platform@{PROJECT_ID}.iam.gserviceaccount.com` |
| `ARGOCD_SERVER` | ArgoCD 서버 주소 | 예: `argocd.example.com` |
| `ARGOCD_TOKEN` | ArgoCD API 토큰 | ArgoCD UI → Settings → Accounts → Generate Token |

### 4-2. Repository Variables (Settings → Variables → Actions)

| Variable 이름 | 설명 | 예시 값 |
|--------------|------|--------|
| `GCP_PROJECT_ID` | GCP 프로젝트 ID | `my-gcp-project-123` |
| `GCP_REGION` | GCP 리전 | `asia-northeast3` |

---

## 5. ArgoCD GitOps

**파일 위치:** [`argocd/application.yaml`](argocd/application.yaml)

```yaml
spec:
  source:
    repoURL: https://github.com/grant0808/HDFS_log_Anomaly_Detection_for_MLOps.git
    # ↑ Fork했다면 본인 저장소 URL로 변경
    targetRevision: main   # ← 사용하는 브랜치명 확인
    path: helm/hdfs-log-anomaly
  destination:
    server: https://kubernetes.default.svc
    namespace: hdfs-observability   # ← 변경하려면 namespace도 함께 수정
```

> **수정 필요 여부:** 원본 저장소를 그대로 사용한다면 `repoURL`은 그대로 둬도 됩니다.
> Fork하거나 private 저장소를 사용한다면 URL을 변경하고 ArgoCD에 Git 자격증명도 추가해야 합니다.

---

## 6. Airflow DAG 변수

**파일 위치:** [`airflow/dags/hdfs_training_pipeline.py`](airflow/dags/hdfs_training_pipeline.py)

DAG 내부에서 Airflow Variable `gcs_raw_bucket` 을 참조합니다:

```python
# Line 18
return "gs://{{ var.value.gcs_raw_bucket }}/hdfs/*.log"
```

Airflow UI (Admin → Variables) 에서 아래 변수를 생성해야 합니다:

| Variable Key | 값 예시 |
|-------------|--------|
| `gcs_raw_bucket` | `{PROJECT_ID}-hdfs-raw-logs` |

---

## 7. 알림(SMTP) 설정

**관련 파일:** [`app/config.py`](app/config.py) (L37~46)

실제 이메일 알림을 받으려면 아래 항목을 `.env` 또는 Kubernetes Secret으로 설정:

| 설정 키 | 기본값 | 변경 예시 |
|--------|--------|---------|
| `SMTP_HOST` | `localhost` | `smtp.gmail.com` 또는 회사 SMTP 서버 |
| `SMTP_PORT` | `1025` | `587` (TLS) 또는 `465` (SSL) |
| `SMTP_USER` | (비어있음) | `your-email@gmail.com` |
| `SMTP_PASSWORD` | (비어있음) | Gmail App Password 또는 SMTP 비밀번호 |
| `SMTP_FROM` | `hdfs-alerts@example.com` | 실제 발신 이메일 |
| `SMTP_TO` | `mlops@example.com` | 실제 수신 이메일 |
| `ALERT_DRIFT_THRESHOLD` | `0.35` | 드리프트 감지 민감도 조절 가능 |
| `ALERT_ANOMALY_SPIKE_THRESHOLD` | `25` | 이상 탐지 스파이크 임계값 |

> **로컬에서는** MailHog(`localhost:8025`)로 이메일을 확인할 수 있으므로 SMTP 설정 없이도 테스트 가능합니다.

---

## 8. 모델 아티팩트 준비

**관련 파일:** [`app/config.py`](app/config.py) (L30~35), [`artifacts/`](artifacts/)

현재 `artifacts/` 에는 `vocab.json` 만 있고 **`model.pt` 파일이 없습니다.**

| 파일 | 현재 상태 | 필요 조치 |
|------|----------|---------|
| `artifacts/model.pt` | **❌ 없음** | 아래 방법 중 하나로 준비 |
| `artifacts/vocab.json` | ✅ 있음 | 그대로 사용 가능 |

### 모델 준비 방법 (선택)

**방법 A — 학습 스킵 (규칙 기반 Fallback 사용):**
- `model.pt` 없이도 앱이 시작됨 (`model_version: local-rule-fallback` 모드로 동작)
- 실제 DeepLog 추론은 불가능하지만 파싱/API/모니터링은 확인 가능

**방법 B — DeepLog 모델 직접 학습:**
```bash
# 의존성 설치 (학습 포함)
pip install -e ".[training]"

# 학습 스크립트 실행 (app/training/ 디렉토리 참고)
python -m app.training.train
```

**방법 C — GCS에서 다운로드 (GCP 배포 환경):**
```bash
gsutil cp gs://{PROJECT_ID}-hdfs-model-artifacts/models/model.pt artifacts/model.pt
```

---

## ✅ 최종 실행 전 체크리스트

### 로컬 (docker-compose)

- [ ] Docker가 실행 중인지 확인
- [ ] `artifacts/model.pt` 준비 또는 Fallback 모드 확인
- [ ] (선택) `.env` 파일에 SMTP 수신 이메일 설정
- [ ] `docker compose up -d` 실행
- [ ] `curl http://localhost:8000/health` 로 헬스 체크

### GCP / Kubernetes

- [ ] `PROJECT_ID`, `REGION` 환경 변수 설정
- [ ] GCP API 활성화 (`GCP_DEPLOYMENT_GUIDE.md` §2)
- [ ] GCS 버킷 생성 (`GCP_DEPLOYMENT_GUIDE.md` §3)
- [ ] Artifact Registry 생성 및 이미지 push (`GCP_DEPLOYMENT_GUIDE.md` §4)
- [ ] GKE 클러스터 생성 (`GCP_DEPLOYMENT_GUIDE.md` §5)
- [ ] Workload Identity 설정 (`GCP_DEPLOYMENT_GUIDE.md` §6)
- [ ] `kubectl create secret` 으로 Secret 생성 ([3-4번](#3-4-kubernetes-secret-생성--직접-실행-필요))
- [ ] `helm/hdfs-log-anomaly/values.yaml` 수정 ([3-1번](#3-1-helmhdfs-log-anomalyvaluesyaml--핵심-수정-파일))
- [ ] Helm으로 앱 배포 (`GCP_DEPLOYMENT_GUIDE.md` §11)
- [ ] GitHub Actions Secrets/Variables 설정 ([4번](#4-github-actions-cicd))
- [ ] ArgoCD `application.yaml` repoURL 확인 ([5번](#5-argocd-gitops))
- [ ] Airflow Variable `gcs_raw_bucket` 생성 ([6번](#6-airflow-dag-변수))

---

---

## 9. Flink 스트리밍 잡 설정

**관련 파일:** `flink/hdfs_log_job.py`

| 환경변수 | 기본값 | 설명 |
|---------|--------|------|
| `FLINK_PARALLELISM` | `1` | 프로덕션에서는 `2` 이상으로 설정 권장 |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | `kafka:9092` (Docker) 또는 실제 Kafka 주소 |

Flink 컨슈머 그룹 ID `hdfs-drain3-parser` 는 코드에 하드코딩되어 있습니다.
변경이 필요하다면 `flink/hdfs_log_job.py` 에서 직접 수정하세요.

```bash
# 환경변수로 설정하는 방법
export FLINK_PARALLELISM=2
export KAFKA_BOOTSTRAP_SERVERS=kafka:9092
```

---

## 10. 스크립트 설정

**파일 위치:** [`scripts/produce_sample_logs.py`](scripts/produce_sample_logs.py)

| 항목 | 현재 값 | 변경 조건 |
|------|---------|----------|
| 샘플 로그 파일 경로 | `data/sample/hdfs.log` | 다른 경로의 로그 파일을 사용하는 경우 변경 |
| Kafka 주소 | `settings.kafka_bootstrap_servers` 참조 | `KAFKA_BOOTSTRAP_SERVERS` 환경변수로 설정 |

```bash
# 샘플 로그를 Kafka에 넣는 방법
python scripts/produce_sample_logs.py
```

---

## 11. 알림 임계값 튜닝

**관련 파일:** [`app/config.py`](app/config.py) (L43~46)

기본값으로 동작하지만, 실제 운영 환경에 맞게 조정이 필요할 수 있습니다:

| 설정 키 | 기본값 | 설명 |
|--------|--------|------|
| `ANOMALY_THRESHOLD` | `0.5` | 이상 탐지 신뢰도 임계값 |
| `TOP_K` | `3` | DeepLog Top-K 예측 수 |
| `SEQUENCE_LENGTH` | `10` | 이벤트 시퀀스 슬라이딩 윈도우 크기 |
| `ALERT_DRIFT_THRESHOLD` | `0.35` | 데이터 드리프트 감지 임계값 |
| `ALERT_ANOMALY_SPIKE_THRESHOLD` | `25` | 이상 탐지 스파이크 임계값 (이 수 이상이면 알림) |
| `ALERT_LATENCY_MS_THRESHOLD` | `250.0` | 추론 지연 임계값 (ms) |
| `ALERT_CONSUMER_LAG_THRESHOLD` | `1000` | Kafka 컨슈머 랙 임계값 |

이 값들은 `.env` 파일 또는 K8s Secret에서 환경변수로 오버라이드 가능합니다.

---

## ✅ 최종 실행 전 체크리스트

### 로컬 (docker-compose)

- [ ] Docker가 실행 중인지 확인
- [ ] `artifacts/model.pt` 준비 또는 Fallback 모드 확인
- [ ] (선택) `.env` 파일에 SMTP 수신 이메일 설정
- [ ] `docker compose up -d` 실행
- [ ] `curl http://localhost:8000/health` 로 헬스 체크

### GCP / Kubernetes

- [ ] `PROJECT_ID`, `REGION` 환경 변수 설정
- [ ] GCP API 활성화 (`GCP_DEPLOYMENT_GUIDE.md` §2)
- [ ] GCS 버킷 생성 (`GCP_DEPLOYMENT_GUIDE.md` §3)
- [ ] Artifact Registry 생성 및 이미지 push (`GCP_DEPLOYMENT_GUIDE.md` §4)
- [ ] GKE 클러스터 생성 (`GCP_DEPLOYMENT_GUIDE.md` §5)
- [ ] Workload Identity 설정 (`GCP_DEPLOYMENT_GUIDE.md` §6)
- [ ] `kubectl create secret` 으로 Secret 생성 ([3-4번](#3-4-kubernetes-secret-생성--직접-실행-필요))
- [ ] `helm/hdfs-log-anomaly/values.yaml` 수정 ([3-1번](#3-1-helmhdfs-log-anomalyvaluesyaml--핵심-수정-파일))
- [ ] Helm으로 앱 배포 (`GCP_DEPLOYMENT_GUIDE.md` §11)
- [ ] GitHub Actions Secrets/Variables 설정 ([4번](#4-github-actions-cicd))
- [ ] ArgoCD `application.yaml` repoURL 확인 ([5번](#5-argocd-gitops))
- [ ] Airflow Variable `gcs_raw_bucket` 생성 ([6번](#6-airflow-dag-변수))
- [ ] (선택) Flink 병렬성 `FLINK_PARALLELISM` 설정 ([9번](#9-flink-스트리밍-잡-설정))

---

> **참고 문서:**
> - 전체 GCP 배포 절차: [`GCP_DEPLOYMENT_GUIDE.md`](GCP_DEPLOYMENT_GUIDE.md)
> - 시스템 아키텍처: [`ARCHITECTURE.md`](ARCHITECTURE.md)
> - 프로젝트 개요: [`README.md`](README.md)
