import smtplib
from dataclasses import dataclass
from email.message import EmailMessage

from app.config import Settings
from monitoring.evidently import DriftReport


class EmailAlerter:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def send(self, subject: str, body: str) -> None:
        import structlog
        logger = structlog.get_logger()
        message = EmailMessage()
        message["From"] = self.settings.smtp_from
        message["To"] = self.settings.smtp_to
        message["Subject"] = subject
        message.set_content(body)
        try:
            with smtplib.SMTP(self.settings.smtp_host, self.settings.smtp_port, timeout=5) as smtp:
                if self.settings.smtp_user:
                    smtp.starttls()
                    smtp.login(self.settings.smtp_user, self.settings.smtp_password)
                smtp.send_message(message)
        except Exception as exc:
            logger.error("Failed to send SMTP alert email", error=str(exc))



@dataclass(frozen=True)
class AlertDecision:
    triggered: bool
    reason: str


class AlertManager:
    def __init__(self, settings: Settings, alerter: EmailAlerter) -> None:
        self.settings = settings
        self.alerter = alerter

    def evaluate(
        self,
        drift: DriftReport | None = None,
        latency_ms: float = 0.0,
        consumer_lag: int = 0,
        model_failure: bool = False,
    ) -> list[AlertDecision]:
        decisions: list[AlertDecision] = []
        if drift and drift.data_drift_score > self.settings.alert_drift_threshold:
            decisions.append(AlertDecision(True, f"drift>{self.settings.alert_drift_threshold}"))
        if drift and drift.anomaly_count > self.settings.alert_anomaly_spike_threshold:
            decisions.append(AlertDecision(True, "anomaly_spike"))
        if latency_ms > self.settings.alert_latency_ms_threshold:
            decisions.append(AlertDecision(True, "latency_spike"))
        if consumer_lag > self.settings.alert_consumer_lag_threshold:
            decisions.append(AlertDecision(True, "consumer_lag"))
        if model_failure:
            decisions.append(AlertDecision(True, "model_failure"))
        for decision in decisions:
            self.alerter.send("HDFS anomaly platform alert", decision.reason)
        return decisions
