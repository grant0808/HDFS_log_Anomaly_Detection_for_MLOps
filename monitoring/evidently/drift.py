from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class DriftReport:
    data_drift_score: float
    schema_drift: bool
    unseen_template_ratio: float
    anomaly_count: int
    template_distribution_shift: float
    null_ratio: float
    prediction_confidence: float
    false_positive_count: int
    parser_unknown_template_count: int


class DriftService:
    def compute(
        self,
        reference: pd.DataFrame,
        current: pd.DataFrame,
        confidence_column: str = "confidence",
    ) -> DriftReport:
        ref_cols = set(reference.columns)
        cur_cols = set(current.columns)
        schema_drift = ref_cols != cur_cols
        null_ratio = float(current.isnull().mean().mean()) if not current.empty else 0.0
        ref_templates = set(reference.get("template_id", pd.Series(dtype=str)).dropna())
        cur_templates = set(current.get("template_id", pd.Series(dtype=str)).dropna())
        unseen = cur_templates - ref_templates
        unseen_template_ratio = len(unseen) / max(len(cur_templates), 1)
        anomaly_count = int(current.get("anomaly", pd.Series(dtype=int)).sum()) if "anomaly" in current else 0
        prediction_confidence = (
            float(current[confidence_column].mean())
            if confidence_column in current and not current.empty
            else 0.0
        )
        template_distribution_shift = self._distribution_shift(reference, current)
        score = min(1.0, unseen_template_ratio + template_distribution_shift + null_ratio)
        return DriftReport(
            data_drift_score=score,
            schema_drift=schema_drift,
            unseen_template_ratio=unseen_template_ratio,
            anomaly_count=anomaly_count,
            template_distribution_shift=template_distribution_shift,
            null_ratio=null_ratio,
            prediction_confidence=prediction_confidence,
            false_positive_count=0,
            parser_unknown_template_count=int(current.get("unknown_template", pd.Series(dtype=int)).sum())
            if "unknown_template" in current
            else 0,
        )

    @staticmethod
    def _distribution_shift(reference: pd.DataFrame, current: pd.DataFrame) -> float:
        if "template_id" not in reference or "template_id" not in current or current.empty:
            return 0.0
        ref = reference["template_id"].value_counts(normalize=True)
        cur = current["template_id"].value_counts(normalize=True)
        keys = set(ref.index) | set(cur.index)
        return float(sum(abs(float(ref.get(key, 0.0)) - float(cur.get(key, 0.0))) for key in keys) / 2)
