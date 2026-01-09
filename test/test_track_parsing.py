"""Tests for the new output-to-status tracking architecture.

The new architecture:
1. ParsingPreprocessor parses text into OutputLine.fields
2. StatusTracker.new_output() reads fields directly
"""
from datetime import datetime

from runtools.runcore.output import OutputLine
from runtools.runcore.util import KVParser, iso_date_time_parser
from runtools.runjob.output import ParsingPreprocessor
from runtools.runjob.track import StatusTracker


def test_parsing_preprocessor_extracts_fields():
    """ParsingPreprocessor extracts fields from text using parsers."""
    preprocessor = ParsingPreprocessor([KVParser()])

    line = OutputLine('event=[downloading] completed=[5]', 1)
    processed = preprocessor(line)

    assert processed.fields == {'event': 'downloading', 'completed': '5'}
    assert processed.message == line.message  # Original message preserved


def test_parsing_preprocessor_skips_if_fields_exist():
    """ParsingPreprocessor doesn't overwrite existing fields (struct logging)."""
    preprocessor = ParsingPreprocessor([KVParser()])

    # Simulate structured logging - fields already set
    line = OutputLine('some message', 1, fields={'event': 'upload'})
    processed = preprocessor(line)

    assert processed.fields == {'event': 'upload'}  # Unchanged


def test_parsing_preprocessor_returns_unchanged_if_no_match():
    """ParsingPreprocessor returns original line if parsers find nothing."""
    preprocessor = ParsingPreprocessor([KVParser()])

    line = OutputLine('no key-value pairs here', 1)
    processed = preprocessor(line)

    assert processed is line  # Same object, no change


def test_status_tracker_event_from_fields():
    """StatusTracker extracts event from OutputLine fields."""
    tracker = StatusTracker()

    # No fields - no event
    tracker.new_output(OutputLine('no fields', 1))
    assert tracker.to_status().last_event is None

    # Fields without 'event' - no event
    tracker.new_output(OutputLine('msg', 2, fields={'other': 'value'}))
    assert tracker.to_status().last_event is None

    # Fields with 'event' - event set
    tracker.new_output(OutputLine('msg', 3, fields={'event': 'downloading'}))
    assert tracker.to_status().last_event.message == 'downloading'


def test_status_tracker_operation_from_fields():
    """StatusTracker extracts operation progress from OutputLine fields."""
    tracker = StatusTracker()

    tracker.new_output(OutputLine('msg', 1, fields={
        'event': 'processing',
        'completed': 10,
        'total': 100,
        'unit': 'files'
    }))

    op = tracker.to_status().operations[0]
    assert op.name == 'processing'
    assert op.completed == 10
    assert op.total == 100
    assert op.unit == 'files'


def test_status_tracker_result_from_fields():
    """StatusTracker extracts result from OutputLine fields."""
    tracker = StatusTracker()

    tracker.new_output(OutputLine('msg', 1, fields={'result': 'success'}))
    assert tracker.to_status().result.message == 'success'


def test_end_to_end_text_parsing():
    """Full flow: text → ParsingPreprocessor → StatusTracker."""
    preprocessor = ParsingPreprocessor([KVParser()])
    tracker = StatusTracker()

    def process(text, ordinal):
        line = OutputLine(text, ordinal)
        processed = preprocessor(line)
        tracker.new_output(processed)

    process('event=[downloading]', 1)
    assert tracker.to_status().last_event.message == 'downloading'

    process('event=[processing] completed=[5] total=[10]', 2)
    op = tracker.to_status().operations[0]
    assert op.name == 'processing'
    assert op.completed == 5


def test_kv_parser_aliases():
    """KVParser aliases convert parsed keys to canonical names."""
    preprocessor = ParsingPreprocessor([KVParser(aliases={'count': 'completed'})])
    tracker = StatusTracker()

    line = OutputLine('event=[download] count=[50] total=[100]', 1)
    processed = preprocessor(line)
    tracker.new_output(processed)

    op = tracker.to_status().operations[0]
    assert op.completed == 50  # 'count' was aliased to 'completed'


def test_timestamp_parsing():
    """Timestamp parser extracts datetime from text."""
    preprocessor = ParsingPreprocessor([iso_date_time_parser('timestamp'), KVParser()])
    tracker = StatusTracker()

    line = OutputLine('2020-10-01 10:30:30 event=[started]', 1)
    processed = preprocessor(line)
    tracker.new_output(processed)

    event = tracker.to_status().last_event
    assert event.message == 'started'
    assert event.timestamp == datetime(2020, 10, 1, 10, 30, 30)


def test_multiple_parsers():
    """Multiple parsers can be chained."""
    preprocessor = ParsingPreprocessor([
        KVParser(value_split=":"),  # Parses "task:value"
        KVParser(field_split="&"),  # Parses "key1=v1&key2=v2"
    ])
    tracker = StatusTracker()

    line = OutputLine('task:mytask', 1)
    processed = preprocessor(line)
    # 'task' should be parsed but it's not a recognized field for StatusTracker
    assert processed.fields.get('task') == 'mytask'


def test_operation_lifecycle():
    """Operations track progress and can be marked finished."""
    tracker = StatusTracker()

    # Start operation
    tracker.new_output(OutputLine('msg', 1, fields={
        'event': 'encoding', 'completed': 5, 'total': 10
    }))
    assert not tracker.to_status().operations[0].finished

    # Complete operation
    tracker.new_output(OutputLine('msg', 2, fields={
        'event': 'encoding', 'completed': 10, 'total': 10
    }))
    assert tracker.to_status().operations[0].finished


def test_event_deactivates_finished_operation():
    """New event deactivates finished operations."""
    tracker = StatusTracker()

    # Complete an operation
    tracker.new_output(OutputLine('msg', 1, fields={
        'event': 'op1', 'completed': 10, 'total': 10
    }))
    assert tracker.to_status().operations[0].is_active

    # New event should deactivate completed operations
    tracker.new_output(OutputLine('msg', 2, fields={'event': 'next_event'}))
    assert not tracker.to_status().operations[0].is_active
