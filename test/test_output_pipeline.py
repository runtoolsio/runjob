"""Publication order of ``OutputPipeline`` under concurrent producers (e.g. stdout and stderr reader threads)."""
import logging
from threading import Event, Thread

from runtools.runjob.capture import StdLogOutputCapture
from runtools.runjob.output import OutputPipeline


def test_concurrent_producers_publish_in_ordinal_order():
    first_processing, release_first = Event(), Event()

    def pause_first_line(line):
        if line.ordinal == 1:
            first_processing.set()
            assert release_first.wait(5)
        return line

    pipeline = OutputPipeline([pause_first_line])
    published = []
    pipeline.add_observer(lambda line: published.append(line.ordinal))

    first = Thread(target=pipeline.new_output, args=('first',))
    first.start()
    assert first_processing.wait(5)
    second = Thread(target=pipeline.new_output, args=('second',))
    second.start()
    second.join(0.1)  # must wait behind the first line rather than overtake it
    release_first.set()
    first.join(5)
    second.join(5)

    assert published == [1, 2]


def test_logging_from_inside_the_pipeline_does_not_wait_for_the_capture_handler():
    pipeline = OutputPipeline()
    captured = []
    pipeline.add_observer(lambda line: captured.append(line.message))
    pipeline.add_observer(lambda line: logging.getLogger('runtools.test').warning('observer warning'))

    with StdLogOutputCapture()(pipeline, capture_filter=lambda: True):
        handler = logging.getLogger().handlers[-1]
        handler.acquire()  # held elsewhere: the forwarding path must never need it
        try:
            producer = Thread(target=pipeline.new_output, args=('line',), daemon=True)
            producer.start()
            producer.join(5)
            deadlocked = producer.is_alive()
        finally:
            handler.release()

    assert not deadlocked
    assert captured == ['line']  # the observer's own warning is dropped, not re-captured


def test_capture_honours_filter_replacement_records():
    pipeline = OutputPipeline()
    captured = []
    pipeline.add_observer(lambda line: captured.append(line.message))

    def redact(record):
        replacement = logging.makeLogRecord(record.__dict__)
        replacement.msg, replacement.args = 'redacted', ()
        return replacement

    with StdLogOutputCapture()(pipeline, capture_filter=lambda: True):
        logging.getLogger().handlers[-1].addFilter(redact)
        logging.getLogger('runtools.test').warning('secret')

    assert captured == ['redacted']
