import hashlib
import re
from datetime import UTC, datetime

from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig

from app.schemas import ParsedLog, RawLog

BLOCK_RE = re.compile(r"(blk_-?\d+)")
HOST_RE = re.compile(r"\b(?:dn|node|host)[-_]?\d+\b", re.IGNORECASE)


class HDFSLogParser:
    def __init__(self) -> None:
        config = TemplateMinerConfig()
        config.drain_depth = 4
        config.drain_sim_th = 0.4
        self._miner = TemplateMiner(config=config)

    def parse(self, raw: RawLog) -> ParsedLog:
        result = self._miner.add_log_message(raw.message)
        template = str(result["template_mined"])
        cluster_id = int(result["cluster_id"])
        event_id = f"E{cluster_id:03d}"
        timestamp = raw.timestamp or datetime.now(UTC)
        host = raw.host or self._extract_host(raw.message)
        block_id = self.extract_block_id(raw.message)
        sequence_key = block_id or f"{host}:{timestamp:%Y%m%d%H%M}"
        sequence_id = hashlib.sha1(sequence_key.encode("utf-8")).hexdigest()[:16]
        params = [str(item) for item in result.get("parameter_list", [])]
        return ParsedLog(
            raw=raw.message,
            template=template,
            template_id=f"T{cluster_id:03d}",
            event_id=event_id,
            timestamp=timestamp,
            host=host,
            block_id=block_id,
            sequence_id=sequence_id,
            parameters=params,
        )

    @staticmethod
    def extract_block_id(message: str) -> str | None:
        match = BLOCK_RE.search(message)
        return match.group(1) if match else None

    @staticmethod
    def _extract_host(message: str) -> str:
        match = HOST_RE.search(message)
        return match.group(0) if match else "unknown-host"
