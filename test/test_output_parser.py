"""Tests for OutputParser — smart output parsing waterfall (JSON → text pattern → KV → plain)."""

from datetime import datetime, timezone

from runtools.runcore.output import OutputLine, parse_timestamp
from runtools.runcore.util.parser import KVParser
from runtools.runjob.output import OutputParser


def test_json_line_full():
    parser = OutputParser()
    line = OutputLine('{"timestamp": "2024-03-15T10:30:00Z", "level": "INFO", "message": "started", "user": "alice"}', 1)
    result = parser(line)
    assert result.message == "started"
    assert result.timestamp == datetime(2024, 3, 15, 10, 30, 0, tzinfo=timezone.utc)
    assert result.level == "INFO"
    assert result.fields == {"user": "alice"}


def test_json_msg_key():
    parser = OutputParser()
    line = OutputLine('{"msg": "hello", "lvl": "DEBUG"}', 1)
    result = parser(line)
    assert result.message == "hello"
    assert result.level == "DEBUG"


def test_json_severity_key():
    parser = OutputParser()
    line = OutputLine('{"message": "event", "severity": "WARNING"}', 1)
    result = parser(line)
    assert result.level == "WARNING"


def test_json_no_message_key():
    parser = OutputParser()
    raw = '{"level": "INFO", "count": 42}'
    line = OutputLine(raw, 1)
    result = parser(line)
    assert result.message == raw  # raw JSON preserved
    assert result.level == "INFO"
    assert result.fields == {"count": 42}


def test_json_not_dict():
    parser = OutputParser()
    line = OutputLine('[1, 2, 3]', 1)
    result = parser(line)
    assert result is line


def test_json_invalid():
    """Invalid JSON falls through to text pattern/KV, returns unchanged if nothing matches."""
    parser = OutputParser()
    line = OutputLine('{not valid json}', 1)
    result = parser(line)
    assert result is line


def test_text_pattern_timestamp_level():
    parser = OutputParser()
    line = OutputLine('2024-03-15T10:30:00.123Z INFO Processing request', 1)
    result = parser(line)
    assert result.timestamp == parse_timestamp("2024-03-15T10:30:00.123Z")
    assert result.level == "INFO"
    assert result.message == "Processing request"


def test_text_pattern_timestamp_level_logger_dotted():
    parser = OutputParser()
    line = OutputLine('2024-03-15T10:30:00Z INFO com.example.App - Processing request', 1)
    result = parser(line)
    assert result.timestamp == datetime(2024, 3, 15, 10, 30, 0, tzinfo=timezone.utc)
    assert result.level == "INFO"
    assert result.logger == "com.example.App"
    assert result.message == "Processing request"


def test_text_pattern_logger_bracket():
    parser = OutputParser()
    line = OutputLine('2024-03-15 10:30:00,123 WARNING [my.logger] Something happened', 1)
    result = parser(line)
    assert result.timestamp == parse_timestamp("2024-03-15 10:30:00,123")
    assert result.level == "WARNING"
    assert result.logger == "my.logger"
    assert result.message == "Something happened"


def test_text_pattern_warn_preserved():
    parser = OutputParser()
    line = OutputLine('2024-03-15T10:30:00Z WARN Something', 1)
    result = parser(line)
    assert result.level == "WARN"


def test_python_log_format():
    parser = OutputParser()
    line = OutputLine('WARNING:my.module:Something happened', 1)
    result = parser(line)
    assert result.level == "WARNING"
    assert result.logger == "my.module"
    assert result.message == "Something happened"


def test_kv_fallback():
    parser = OutputParser(kv_parser=KVParser())
    line = OutputLine('rt_event=[downloading] rt_completed=[5]', 1)
    result = parser(line)
    assert result.message == ''
    assert result.fields == {'rt_event': 'downloading', 'rt_completed': '5'}
    assert result.is_tracking_only


def test_kv_mixed_text_and_fields():
    parser = OutputParser(kv_parser=KVParser())
    line = OutputLine('Processing site_key=[ABC] total=[100]', 1)
    result = parser(line)
    assert result.message == 'Processing'
    assert result.fields == {'site_key': 'ABC', 'total': '100'}


def test_kv_after_text_pattern():
    parser = OutputParser(kv_parser=KVParser())
    line = OutputLine('2024-03-15T10:30:00Z INFO Processing site_key=[ABC] total=[100]', 1)
    result = parser(line)
    assert result.timestamp == datetime(2024, 3, 15, 10, 30, 0, tzinfo=timezone.utc)
    assert result.level == "INFO"
    assert result.message == "Processing"
    assert result.fields == {'site_key': 'ABC', 'total': '100'}


def test_plain_text_unchanged():
    parser = OutputParser()
    line = OutputLine('just some output', 1)
    result = parser(line)
    assert result is line


def test_skip_already_parsed_timestamp():
    parser = OutputParser()
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    line = OutputLine('{"level": "DEBUG"}', 1, timestamp=ts)
    result = parser(line)
    assert result is line


def test_skip_already_parsed_level():
    parser = OutputParser()
    line = OutputLine('some text', 1, level="INFO")
    result = parser(line)
    assert result is line


def test_skip_already_parsed_fields():
    parser = OutputParser()
    line = OutputLine('some text', 1, fields={"key": "val"})
    result = parser(line)
    assert result is line


def test_json_with_thread():
    parser = OutputParser()
    line = OutputLine('{"message": "hello", "thread_name": "worker-1"}', 1)
    result = parser(line)
    assert result.message == "hello"
    assert result.thread == "worker-1"


def test_text_pattern_no_logger_single_word():
    """Single-word token after level is NOT captured as logger (requires 2+ dotted segments)."""
    parser = OutputParser()
    line = OutputLine('2024-03-15T10:30:00Z INFO Starting the process now', 1)
    result = parser(line)
    assert result.logger is None
    assert result.message == "Starting the process now"
