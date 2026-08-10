"""Error taxonomy (Section 7 of new_compiler_requirements/compiler_reqs.md).

Every stage raises one of these instead of letting a raw engine/library
exception reach the query author. Each carries a `locus` -- the offending
input location (a column name, a regex position, a function name, ...) -- so
the message is actionable without a stack trace.
"""
from __future__ import annotations


class RecapCompilerError(Exception):
    """Base class for every user-facing compiler error."""

    category = "E-UNKNOWN"

    def __init__(self, message: str, locus: str | None = None):
        self.message = message
        self.locus = locus
        located = f" (at {locus})" if locus is not None else ""
        super().__init__(f"[{self.category}] {message}{located}")


class IngestionError(RecapCompilerError):
    """E-INPUT: missing src/dst/label, unreadable file, type-inference conflict."""

    category = "E-INPUT"


class RegexError(RecapCompilerError):
    """E-REGEX: unbalanced parenthesis, unknown operator, empty language."""

    category = "E-REGEX"


class RefError(RecapCompilerError):
    """E-REF: aggregate references an unknown column, undeclared dictionary
    key, or a state variable in a factorized body."""

    category = "E-REF"


class AggregateTypeError(RecapCompilerError):
    """E-TYPE: viability predicate not Boolean; update returns wrong shape."""

    category = "E-TYPE"


class UnsupportedError(RecapCompilerError):
    """E-UNSUPPORTED: function body outside the inlinable SQL sublanguage."""

    category = "E-UNSUPPORTED"


class ExecutionError(RecapCompilerError):
    """E-EXEC: DuckDB runtime error; length bound causes resource exhaustion."""

    category = "E-EXEC"
