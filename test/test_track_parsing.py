"""Tests for the output-to-status tracking architecture.

The architecture:
1. ParsingProcessor parses text into OutputLine.fields
2. StatusTracker.new_output() reads rt_-prefixed fields for tracking
"""
from datetime import datetime

from runtools.runcore.output import OutputLine
from runtools.runcore.util import KVParser, iso_date_time_parser
from runtools.runjob.output import ParsingProcessor
from runtools.runjob.track import StatusTracker, field_based_handler


def test_parsing_preprocessor_extracts_fields():
    """ParsingProcessor extracts fields from text using parsers."""
    preprocessor = ParsingProcessor([KVParser()])

    line = OutputLine('rt_event=[downloading] rt_completed=[5]', 1)
    processed = preprocessor(line)

    assert processed.fields == {'rt_event': 'downloading', 'rt_completed': '5'}
    assert processed.message == line.message


def test_parsing_preprocessor_skips_if_fields_exist():
    """ParsingProcessor doesn't overwrite existing fields (structured logging)."""
    preprocessor = ParsingProcessor([KVParser()])

    line = OutputLine('some message', 1, fields={'rt_event': 'upload'})
    processed = preprocessor(line)

    assert processed.fields == {'rt_event': 'upload'}


def test_parsing_preprocessor_returns_unchanged_if_no_match():
    """ParsingProcessor returns original line if parsers find nothing."""
    preprocessor = ParsingProcessor([KVParser()])

    line = OutputLine('no key-value pairs here', 1)
    processed = preprocessor(line)

    assert processed is line


def test_status_tracker_event_from_fields():
    """StatusTracker extracts event from rt_event field."""
    tracker = StatusTracker(output_handler=field_based_handler)

    tracker(OutputLine('no fields', 1))
    assert tracker.to_status().last_event is None

    tracker(OutputLine('msg', 2, fields={'other': 'value'}))
    assert tracker.to_status().last_event is None

    tracker(OutputLine('msg', 3, fields={'rt_event': 'downloading'}))
    assert tracker.to_status().last_event.message == 'downloading'


def test_status_tracker_operation_from_fields():
    """StatusTracker extracts operation progress from rt_ fields."""
    tracker = StatusTracker()

    tracker(OutputLine('msg', 1, fields={
        'rt_operation': 'processing',
        'rt_completed': 10,
        'rt_total': 100,
        'rt_unit': 'files'
    }))

    op = tracker.to_status().operations[0]
    assert op.name == 'processing'
    assert op.completed == 10
    assert op.total == 100
    assert op.unit == 'files'


def test_status_tracker_result_from_fields():
    """StatusTracker extracts result from rt_result field."""
    tracker = StatusTracker()

    tracker(OutputLine('msg', 1, fields={'rt_result': 'success'}))
    assert tracker.to_status().result.message == 'success'


def test_end_to_end_text_parsing():
    """Full flow: text → ParsingProcessor → StatusTracker."""
    preprocessor = ParsingProcessor([KVParser()])
    tracker = StatusTracker()

    def process(text, ordinal):
        line = OutputLine(text, ordinal)
        processed = preprocessor(line)
        tracker(processed)

    process('rt_event=[downloading]', 1)
    assert tracker.to_status().last_event.message == 'downloading'

    process('rt_operation=[processing] rt_completed=[5] rt_total=[10]', 2)
    op = tracker.to_status().operations[0]
    assert op.name == 'processing'
    assert op.completed == 5


def test_kv_parser_aliases():
    """KVParser aliases convert parsed keys to rt_ tracking names."""
    preprocessor = ParsingProcessor([KVParser(aliases={'count': 'rt_completed'})])
    tracker = StatusTracker()

    line = OutputLine('rt_operation=[download] count=[50] rt_total=[100]', 1)
    processed = preprocessor(line)
    tracker(processed)

    op = tracker.to_status().operations[0]
    assert op.completed == 50


def test_timestamp_parsing():
    """Timestamp parser extracts datetime from text."""
    preprocessor = ParsingProcessor([iso_date_time_parser('rt_timestamp'), KVParser()])
    tracker = StatusTracker()

    line = OutputLine('2020-10-01 10:30:30 rt_event=[started]', 1)
    processed = preprocessor(line)
    tracker(processed)

    event = tracker.to_status().last_event
    assert event.message == 'started'
    assert event.timestamp == datetime(2020, 10, 1, 10, 30, 30)


def test_multiple_parsers():
    """Multiple parsers can be chained."""
    preprocessor = ParsingProcessor([
        KVParser(value_split=":"),
        KVParser(field_split="&"),
    ])
    tracker = StatusTracker()

    line = OutputLine('task:mytask', 1)
    processed = preprocessor(line)
    assert processed.fields.get('task') == 'mytask'


def test_operation_lifecycle():
    """Operations track progress and can be marked finished."""
    tracker = StatusTracker()

    tracker(OutputLine('msg', 1, fields={
        'rt_operation': 'encoding', 'rt_completed': 5, 'rt_total': 10
    }))
    assert not tracker.to_status().operations[0].finished

    tracker(OutputLine('msg', 2, fields={
        'rt_operation': 'encoding', 'rt_completed': 10, 'rt_total': 10
    }))
    assert tracker.to_status().operations[0].finished


def test_operation_failed():
    """Operations can be marked as failed via the rt_failed field."""
    tracker = StatusTracker()

    tracker(OutputLine('msg', 1, fields={
        'rt_operation': 'uploading', 'rt_completed': 50, 'rt_total': 100
    }))
    op = tracker.to_status().operations[0]
    assert not op.finished
    assert not op.failed

    tracker(OutputLine('msg', 2, fields={
        'rt_operation': 'uploading', 'rt_failed': 'connection timeout'
    }))
    op = tracker.to_status().operations[0]
    assert op.finished
    assert op.failed
    assert op.result == 'connection timeout'


def test_failed_takes_precedence_over_result():
    """When both rt_failed and rt_result are present, rt_failed wins."""
    tracker = StatusTracker()

    tracker(OutputLine('msg', 1, fields={
        'rt_operation': 'upload', 'rt_failed': 'error', 'rt_result': 'done'
    }))
    op = tracker.to_status().operations[0]
    assert op.failed
    assert op.result == 'error'


def test_zero_total_means_zero_work():
    """Total of 0 means zero work exists, not unknown."""
    tracker = StatusTracker()

    tracker(OutputLine('msg', 1, fields={
        'rt_operation': 'noop', 'rt_completed': 0, 'rt_total': 0
    }))
    op = tracker.to_status().operations[0]
    assert op.total == 0
    assert op.completed == 0
    assert op.finished
