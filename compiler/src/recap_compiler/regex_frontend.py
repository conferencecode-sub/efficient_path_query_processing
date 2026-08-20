"""Stage B: regex frontend, Thompson's construction.

Compiles a label regex into an epsilon-free NFA. Thompson's-construction and
epsilon-removal are delegated to pyformlang -- per the requirements doc's
mechanization note (Section 5.B, Section 8), this step is non-novel and a
library removes an avenue of reviewer doubt about correctness.

IMPORTANT: pyformlang's `Regex` only implements concatenation, union (`|`),
Kleene star (`*`), parentheses, and `$`/`epsilon` -- *not* `+`, `?`, or
bounded repetition (verified directly against its source and against
`EpsilonNFA.accepts`, since `+`/`?` silently parse without error but don't
apply the operator: `Regex("a+").to_epsilon_nfa().accepts(["a", "a"])` is
False). The compiler supports all six regex operators, so `+`, `?`, and `{m,n}` are
expanded here into pyformlang's native subset before parsing:
  atom+     -> (atom atom*)
  atom?     -> (atom|$)
  atom{m,n} -> m required copies concatenated with (n-m) copies of (atom|$)
  atom{m,}  -> m required copies concatenated with atom*

A label containing a regex metacharacter (`|()*+?{}$.`) or whitespace (e.g.
`North America`) would otherwise confuse the parser -- worse, silently
rather than with an error: unquoted, `North America` parses as *two*
concatenated tokens, `North` then `America` (a space is pyformlang's
concatenation operator), matching a nonexistent two-edge sequence instead
of the one real edge labeled `North America`. Double-quote such a label
(`("North America"|Asia)+`) to have it treated as a single atomic token
regardless of what characters it contains -- see `_extract_quoted_labels`.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from pyformlang.regular_expression import Regex
from pyformlang.regular_expression.regex_objects import MisformedRegexError

from .errors import RegexError

# Matches a bounded-repetition bound, or a single `+` or `?`; `.search()`
# finds the leftmost one so nested/chained operators expand outside-in.
_REPEAT_OP = re.compile(r"\{(\d+)(,(\d*))?\}|\+|\?")

_QUOTED_LABEL = re.compile(r'"([^"]*)"')
# Alnum/underscore only, so both `_atom_span`'s bare-token scan and
# pyformlang's own tokenizer treat a placeholder exactly like a plain
# label such as `Europe` -- never split, never confused for an operator.
_PLACEHOLDER_TEMPLATE = "__RECAP_QUOTED_LABEL_{}__"


def _extract_quoted_labels(pattern: str) -> tuple[str, dict[str, str]]:
    """Replaces every `"..."` span in `pattern` with a synthetic
    placeholder token before any further parsing, so a label containing a
    regex metacharacter or whitespace is shielded from being interpreted
    as one -- it's substituted back into the NFA's transition labels only
    after Thompson's construction has already run on the placeholder.
    Returns the rewritten pattern and a `{placeholder: original_label}`
    map (empty if `pattern` has no quotes)."""
    if pattern.count('"') % 2 != 0:
        raise RegexError(f"unbalanced quote (\") in pattern: {pattern!r}", locus=pattern)

    placeholders: dict[str, str] = {}

    def _replace(match: re.Match) -> str:
        placeholder = _PLACEHOLDER_TEMPLATE.format(len(placeholders))
        placeholders[placeholder] = match.group(1)
        return placeholder

    return _QUOTED_LABEL.sub(_replace, pattern), placeholders


@dataclass(frozen=True)
class NFA:
    """An epsilon-free NFA as pyformlang produces it. May have more than one
    start state; Stage C (transitions.py) normalizes that to a single q0,
    since the SQL template assumes exactly one."""

    states: frozenset
    start_states: frozenset
    accepting_states: frozenset
    transitions: tuple  # tuple[tuple[state, label: str, state], ...]


def _atom_span(pattern: str, end: int) -> int:
    """`pattern[end]` is the position of a postfix operator (`+`, `?`, or a
    bound's `{`); return the start index of the atom it applies to -- either
    a balanced parenthesized group or a single bare token immediately
    preceding it."""
    if end == 0:
        raise RegexError("postfix operator has no preceding atom", locus=str(end))
    if pattern[end - 1] == ")":
        depth = 0
        i = end - 1
        while i >= 0:
            if pattern[i] == ")":
                depth += 1
            elif pattern[i] == "(":
                depth -= 1
                if depth == 0:
                    return i
            i -= 1
        raise RegexError("unbalanced parenthesis before postfix operator", locus=str(end))

    i = end - 1
    while i >= 0 and (pattern[i].isalnum() or pattern[i] == "_"):
        i -= 1
    if i + 1 == end:
        raise RegexError("postfix operator has no preceding atom", locus=str(end))
    return i + 1


def _optional(atom: str) -> str:
    return f"({atom}|$)"


def _expand_postfix_operators(pattern: str) -> str:
    """Rewrites every `atom+`, `atom?`, and `atom{m}`/`atom{m,n}`/`atom{m,}`
    into pyformlang-native concatenation/union/Kleene-star/epsilon.
    Repeats until none remain, so chained operators (e.g. an optional group
    made of a bounded-repetition atom) expand correctly outside-in."""
    while True:
        match = _REPEAT_OP.search(pattern)
        if match is None:
            return pattern

        start_atom = _atom_span(pattern, match.start())
        atom = pattern[start_atom:match.start()]
        op = match.group(0)

        if op == "+":
            replacement = f"({atom} {atom}*)"
        elif op == "?":
            replacement = _optional(atom)
        else:
            lower = int(match.group(1))
            if match.group(2) is None:
                upper = lower  # `{m}` == exactly m copies
            elif match.group(3) == "":
                upper = None  # `{m,}` == unbounded upper
            else:
                upper = int(match.group(3))
            if upper is not None and upper < lower:
                raise RegexError(
                    f"invalid bound {{{lower},{upper}}}: upper bound below lower bound",
                    locus=str(match.start()))

            required = " ".join([atom] * lower)
            tail = f"{atom}*" if upper is None else " ".join([_optional(atom)] * (upper - lower))
            expansion = " ".join(part for part in (required, tail) if part)
            replacement = f"({expansion})" if expansion else ""

        pattern = pattern[:start_atom] + replacement + pattern[match.end():]


def compile_regex_to_nfa(pattern: str, *, minimize: bool = False) -> NFA:
    """Parse `pattern` over the edge-label alphabet, build an
    epsilon-NFA via Thompson's construction, and eliminate epsilon
    transitions. Raises RegexError (E-REGEX) on a malformed pattern.

    Determinization/minimization "may be offered as
    an optional pass but shall not be the default": non-determinism is
    handled natively by the recursive join, and *not* collapsing states is
    what keeps the NFA compatible with wavefront/segment-style planners
    (R4.O2) -- exactly the property `alternative_explorations/
    navigation_style_experiment.md` exercises. So `minimize=False` here is
    the required default, not just a cautious one; pass `minimize=True`
    only when that compatibility genuinely doesn't matter for the query at
    hand (e.g. a benchmark measuring standard bottom-up evaluation only).
    pyformlang's `.minimize()` determinizes internally, so no separate
    `.to_deterministic()` call is needed."""
    dequoted, quoted_labels = _extract_quoted_labels(pattern)
    expanded = _expand_postfix_operators(dequoted)
    try:
        parsed = Regex(expanded)
        epsilon_nfa = parsed.to_epsilon_nfa()
        nfa = epsilon_nfa.remove_epsilon_transitions()
        if minimize:
            nfa = nfa.minimize()
    except MisformedRegexError as exc:
        raise RegexError(str(exc), locus=pattern) from exc

    def _label(symbol) -> str:
        text = str(symbol.value)
        return quoted_labels.get(text, text)

    return NFA(
        states=frozenset(nfa.states),
        start_states=frozenset(nfa.start_states),
        accepting_states=frozenset(nfa.final_states),
        transitions=tuple((frm, _label(sym), to) for frm, sym, to in nfa),
    )
