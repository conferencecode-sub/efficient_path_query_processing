import time

from recap_compiler.profiling import TimingBreakdown, timed_stage


def test_timed_stage_records_a_positive_duration():
    breakdown = TimingBreakdown()
    with timed_stage(breakdown, "sleep a bit"):
        time.sleep(0.01)
    assert len(breakdown.stages) == 1
    name, ms = breakdown.stages[0]
    assert name == "sleep a bit"
    assert ms >= 10  # slept 10ms, allow only for timer overhead, not undershoot


def test_stages_recorded_in_order_and_total_is_their_sum():
    breakdown = TimingBreakdown()
    with timed_stage(breakdown, "first"):
        pass
    with timed_stage(breakdown, "second"):
        pass
    assert [name for name, _ in breakdown.stages] == ["first", "second"]
    assert breakdown.total_ms == sum(ms for _, ms in breakdown.stages)


def test_same_stage_name_can_appear_more_than_once():
    breakdown = TimingBreakdown()
    with timed_stage(breakdown, "execute query"):
        pass
    with timed_stage(breakdown, "execute query"):
        pass
    assert [name for name, _ in breakdown.stages] == ["execute query", "execute query"]


def test_as_rows_reports_percentage_of_total():
    breakdown = TimingBreakdown()
    breakdown.stages = [("a", 25.0), ("b", 75.0)]
    rows = breakdown.as_rows()
    assert rows == [
        {"stage": "a", "ms": 25.0, "% of total": 25.0},
        {"stage": "b", "ms": 75.0, "% of total": 75.0},
    ]


def test_records_a_stage_even_if_the_block_raises():
    breakdown = TimingBreakdown()
    try:
        with timed_stage(breakdown, "will fail"):
            raise ValueError("boom")
    except ValueError:
        pass
    assert len(breakdown.stages) == 1
    assert breakdown.stages[0][0] == "will fail"


def test_empty_breakdown_total_is_zero_and_as_rows_does_not_divide_by_zero():
    breakdown = TimingBreakdown()
    assert breakdown.total_ms == 0
    assert breakdown.as_rows() == []
