from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class RawLog(BaseModel):
    message: str
    timestamp: datetime | None = None
    host: str | None = None
    source: str | None = None


class ParsedLog(BaseModel):
    raw: str
    template: str
    template_id: str
    event_id: str
    timestamp: datetime
    host: str
    block_id: str | None
    sequence_id: str
    parameters: list[str] = Field(default_factory=list)


class PredictRequest(BaseModel):
    sequence: list[str]
    actual_event: str | None = None
    raw_log: RawLog | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class PredictResponse(BaseModel):
    anomaly: bool
    actual_event: str | None
    top_k_predictions: list[str]
    probabilities: dict[str, float]
    confidence: float
    model_version: str
    rule_fallback: bool
    latency_ms: float
    reason: str
