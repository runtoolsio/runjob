import pytest
from runtools.runcore.output import OutputLine, Mode
from runtools.runjob.output import InMemoryTailBuffer


def test_max_capacity():
    output = InMemoryTailBuffer(max_capacity=2)
    output.add_line(OutputLine("line1", False))
    output.add_line(OutputLine("line2", False))
    output.add_line(OutputLine("line3", False))

    lines = output.get_lines()
    assert len(lines) == 2
    assert [line.text for line in lines] == ["line2", "line3"]


def test_get_lines_with_max_lines():
    output = InMemoryTailBuffer()
    output.add_line(OutputLine("line1", False))
    output.add_line(OutputLine("line2", False))
    output.add_line(OutputLine("line3", False))

    head_lines = output.get_lines(mode=Mode.HEAD, max_lines=2)
    assert len(head_lines) == 2
    assert [line.text for line in head_lines] == ["line1", "line2"]

    tail_lines = output.get_lines(mode=Mode.TAIL, max_lines=2)
    assert len(tail_lines) == 2
    assert [line.text for line in tail_lines] == ["line2", "line3"]


def test_get_all_lines():
    output = InMemoryTailBuffer()
    output.add_line(OutputLine("line1", False))
    output.add_line(OutputLine("line2", False))

    lines = output.get_lines(max_lines=0)
    assert len(lines) == 2
    assert [line.text for line in lines] == ["line1", "line2"]


def test_negative_max_capacity():
    with pytest.raises(ValueError):
        InMemoryTailBuffer(max_capacity=-1)


def test_negative_max_lines():
    output = InMemoryTailBuffer()
    with pytest.raises(ValueError):
        output.get_lines(max_lines=-1)
