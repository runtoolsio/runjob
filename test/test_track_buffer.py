import pytest
from runtools.runcore.output import OutputLine, Mode
from runtools.runjob.output import InMemoryTailBuffer


def test_max_bytes():
    # Each line ~5 bytes ("lineX"), buffer allows ~10 bytes — fits 2 lines
    buf = InMemoryTailBuffer(max_bytes=10)
    buf.add_line(OutputLine("line1", 1))
    buf.add_line(OutputLine("line2", 2))
    buf.add_line(OutputLine("line3", 3))

    lines = buf.get_lines()
    assert len(lines) == 2
    assert [line.message for line in lines] == ["line2", "line3"]


def test_negative_max_bytes():
    with pytest.raises(ValueError):
        InMemoryTailBuffer(max_bytes=-1)


def test_single_large_line_always_kept():
    buf = InMemoryTailBuffer(max_bytes=5)
    buf.add_line(OutputLine("a very long line that exceeds the limit", 1))

    lines = buf.get_lines()
    assert len(lines) == 1
    assert lines[0].message == "a very long line that exceeds the limit"


def test_get_lines_with_max_lines():
    output = InMemoryTailBuffer()
    output.add_line(OutputLine("line1", 1))
    output.add_line(OutputLine("line2", 2))
    output.add_line(OutputLine("line3", 3))

    head_lines = output.get_lines(mode=Mode.HEAD, max_lines=2)
    assert len(head_lines) == 2
    assert [line.message for line in head_lines] == ["line1", "line2"]

    tail_lines = output.get_lines(mode=Mode.TAIL, max_lines=2)
    assert len(tail_lines) == 2
    assert [line.message for line in tail_lines] == ["line2", "line3"]


def test_get_all_lines():
    output = InMemoryTailBuffer()
    output.add_line(OutputLine("line1", 1))
    output.add_line(OutputLine("line2", 2))

    lines = output.get_lines(max_lines=0)
    assert len(lines) == 2
    assert [line.message for line in lines] == ["line1", "line2"]


def test_negative_max_lines():
    output = InMemoryTailBuffer()
    with pytest.raises(ValueError):
        output.get_lines(max_lines=-1)
