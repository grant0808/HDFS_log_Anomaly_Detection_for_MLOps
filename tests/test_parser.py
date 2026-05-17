from app.parser import HDFSLogParser
from app.schemas import RawLog


def test_hdfs_parser_extracts_block_and_ids() -> None:
    parser = HDFSLogParser()
    parsed = parser.parse(RawLog(message="PacketResponder 1 for block blk_388650 terminating", host="dn1"))

    assert parsed.block_id == "blk_388650"
    assert parsed.event_id.startswith("E")
    assert parsed.template_id.startswith("T")
