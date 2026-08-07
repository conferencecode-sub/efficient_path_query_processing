import pytest

from recap_compiler.errors import RegexError
from recap_compiler.regex_frontend import NFA, compile_regex_to_nfa


def _accepts(nfa: NFA, labels: list[str]) -> bool:
    """Simulates the (possibly multi-start) NFA directly, so tests check
    actual language membership rather than just transition counts."""
    current = set(nfa.start_states)
    for label in labels:
        current = {to for (frm, lbl, to) in nfa.transitions if frm in current and lbl == label}
        if not current:
            return False
    return bool(current & nfa.accepting_states)


def test_exposes_states_start_and_accepting():
    nfa = compile_regex_to_nfa("a")
    assert isinstance(nfa.states, frozenset) and nfa.states
    assert isinstance(nfa.start_states, frozenset) and nfa.start_states
    assert isinstance(nfa.accepting_states, frozenset) and nfa.accepting_states
    assert isinstance(nfa.transitions, tuple) and nfa.transitions


def test_concatenation():
    nfa = compile_regex_to_nfa("a b")
    assert _accepts(nfa, ["a", "b"])
    assert not _accepts(nfa, ["a"])
    assert not _accepts(nfa, ["b", "a"])


def test_union():
    nfa = compile_regex_to_nfa("a|b")
    assert _accepts(nfa, ["a"])
    assert _accepts(nfa, ["b"])
    assert not _accepts(nfa, ["c"])


def test_kleene_star():
    nfa = compile_regex_to_nfa("a*")
    assert _accepts(nfa, [])
    assert _accepts(nfa, ["a"])
    assert _accepts(nfa, ["a", "a", "a"])
    assert not _accepts(nfa, ["b"])


def test_kleene_plus():
    nfa = compile_regex_to_nfa("a+")
    assert not _accepts(nfa, [])
    assert _accepts(nfa, ["a"])
    assert _accepts(nfa, ["a", "a"])


def test_optional():
    nfa = compile_regex_to_nfa("a?")
    assert _accepts(nfa, [])
    assert _accepts(nfa, ["a"])
    assert not _accepts(nfa, ["a", "a"])


@pytest.mark.parametrize("pattern,accepted,rejected", [
    ("a{2}", [["a", "a"]], [["a"], ["a", "a", "a"]]),
    ("a{2,3}", [["a", "a"], ["a", "a", "a"]], [["a"], ["a", "a", "a", "a"]]),
    ("a{2,}", [["a", "a"], ["a", "a", "a", "a"]], [["a"], []]),
    ("a{0,2}", [[], ["a"], ["a", "a"]], [["a", "a", "a"]]),
])
def test_bounded_repetition_bare_token(pattern, accepted, rejected):
    nfa = compile_regex_to_nfa(pattern)
    for labels in accepted:
        assert _accepts(nfa, labels), f"{pattern} should accept {labels}"
    for labels in rejected:
        assert not _accepts(nfa, labels), f"{pattern} should reject {labels}"


def test_bounded_repetition_over_parenthesized_group():
    nfa = compile_regex_to_nfa("(a|b){1,2}")
    assert _accepts(nfa, ["a"])
    assert _accepts(nfa, ["b"])
    assert _accepts(nfa, ["a", "b"])
    assert _accepts(nfa, ["b", "b"])
    assert not _accepts(nfa, [])
    assert not _accepts(nfa, ["a", "b", "a"])


def test_bounded_repetition_invalid_bound_raises():
    with pytest.raises(RegexError):
        compile_regex_to_nfa("a{3,1}")


def test_malformed_regex_raises_regex_error():
    with pytest.raises(RegexError) as exc_info:
        compile_regex_to_nfa("(a|b")
    assert exc_info.value.category == "E-REGEX"


def test_q1_style_regex_accepts_grow_then_fraud_only():
    # Matches ReCAP/q1's actual query: (transfer|purchase|sale)+ (phishing|scam)+
    nfa = compile_regex_to_nfa("(transfer|purchase|sale)+ (phishing|scam)+")
    assert _accepts(nfa, ["transfer", "phishing"])
    assert _accepts(nfa, ["sale", "sale", "scam", "scam"])
    assert not _accepts(nfa, ["phishing"])  # no grow prefix
    assert not _accepts(nfa, ["transfer"])  # no fraud suffix
    assert not _accepts(nfa, ["phishing", "transfer"])  # wrong order
