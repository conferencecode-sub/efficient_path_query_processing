"""Stage-by-stage timing breakdown for a single compile+run.

Not part of the requirements spec -- a diagnostic/demo convenience so a
caller (the demo script, the workbench UI) can show where time actually
goes: parsing the regex, loading the graph, validating the aggregate,
generating SQL, executing it, and so on. Deliberately separate from
`execution.Telemetry`, which measures only the generated query's
own execution -- `TimingBreakdown` is the wider picture around it, and a
query's `Telemetry.runtime_ms` is typically folded in as one of its stages.
"""
from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass, field


@dataclass
class TimingBreakdown:
    """An ordered list of (stage_name, milliseconds) pairs. Stages are
    recorded in the order they're timed; the same name may appear more than
    once (e.g. "execute query" for both the optimized and standard runs)."""

    stages: list[tuple[str, float]] = field(default_factory=list)

    @property
    def total_ms(self) -> float:
        return sum(ms for _, ms in self.stages)

    def as_rows(self) -> list[dict]:
        """One row per stage, with each stage's share of the total -- ready
        to hand to `pandas.DataFrame` or print as a table."""
        total = self.total_ms or 1e-9  # guard against an all-zero breakdown
        return [
            {"stage": name, "ms": ms, "% of total": 100 * ms / total}
            for name, ms in self.stages
        ]


@contextmanager
def timed_stage(breakdown: TimingBreakdown, name: str):
    """Records how long the wrapped block took under `name` in `breakdown`.

    Usage:
        breakdown = TimingBreakdown()
        with timed_stage(breakdown, "B: regex -> NFA"):
            nfa = compile_regex_to_nfa(regex)
    """
    start = time.perf_counter()
    try:
        yield
    finally:
        breakdown.stages.append((name, (time.perf_counter() - start) * 1000))
