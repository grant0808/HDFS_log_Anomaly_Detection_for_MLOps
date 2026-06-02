from datetime import UTC, datetime

import pandas as pd
from fastapi import FastAPI
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.responses import Response

from app.config import get_settings
from app.db import MetadataRepository
from app.logging_config import configure_logging
from app.metrics import DATA_DRIFT_SCORE, LOGS_PROCESSED
from app.model import InferenceService
from app.parser import HDFSLogParser
from app.schemas import PredictRequest, PredictResponse, RawLog
from app.telemetry import configure_tracing
from monitoring.evidently import DriftService
from alerts import AlertManager, EmailAlerter

settings = get_settings()
configure_logging(settings.log_level)
app = FastAPI(title="HDFS Log Anomaly Detection API", version="0.1.0")
configure_tracing(app, settings.service_name)
parser = HDFSLogParser()
inference = InferenceService(settings)
repo = MetadataRepository(settings.postgres_dsn)
drift_service = DriftService()
emailer = EmailAlerter(settings)
alert_manager = AlertManager(settings, emailer)
reference_df = pd.DataFrame({"template_id": ["T001", "T002"], "anomaly": [0, 0], "confidence": [0.9, 0.85]})



@app.on_event("startup")
def startup() -> None:
    repo.init_schema()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "model_version": settings.model_version}


@app.post("/predict", response_model=PredictResponse)
def predict(request: PredictRequest) -> PredictResponse:
    response = inference.predict(request)
    repo.save_inference(
        {
            "timestamp": datetime.now(UTC),
            "sequence_id": request.metadata.get("sequence_id"),
            "actual_event": request.actual_event,
            "anomaly": int(response.anomaly),
            "confidence": response.confidence,
            "latency_ms": response.latency_ms,
            "payload": response.model_dump(),
        }
    )
    return response


@app.post("/parse")
def parse(raw: RawLog) -> dict[str, object]:
    parsed = parser.parse(raw)
    repo.save_parsed_log(parsed)
    LOGS_PROCESSED.inc()
    return parsed.model_dump()


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/drift")
def drift() -> dict[str, object]:
    logs = repo.get_recent_logs(limit=1000)
    if not logs:
        current_df = reference_df.copy()
    else:
        current_df = pd.DataFrame(logs)
        if "anomaly" not in current_df.columns:
            current_df["anomaly"] = 0
        if "confidence" not in current_df.columns:
            current_df["confidence"] = 1.0

    report = drift_service.compute(reference_df, current_df)
    DATA_DRIFT_SCORE.set(report.data_drift_score)
    alert_manager.evaluate(drift=report)
    return report.__dict__



@app.get("/anomalies")
def anomalies(limit: int = 100) -> list[dict[str, object]]:
    return repo.recent_anomalies(limit=limit)


@app.get("/model_version")
def model_version() -> dict[str, str]:
    return {"model_version": settings.model_version}
