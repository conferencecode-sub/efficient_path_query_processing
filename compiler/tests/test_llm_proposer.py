"""Tests for Module J's reintroduced LLM proposer -- a hosted Claude model
drafts a full per-transition-pair General selective aggregate. No test here
makes a real network call: `client` is injected as a small fake exposing
just `.messages.create(...)`, returning a canned tool-use response, the
same shape the real Anthropic SDK returns."""
import pytest

from recap_compiler.errors import ProposerParseError, ProposerUnavailableError
from recap_compiler.llm_proposer import propose_general_aggregate


class _FakeToolUseBlock:
    def __init__(self, input_dict):
        self.type = "tool_use"
        self.input = input_dict


class _FakeTextBlock:
    def __init__(self, text):
        self.type = "text"
        self.text = text


class _FakeResponse:
    def __init__(self, content):
        self.content = content


class _FakeMessages:
    def __init__(self, response=None, exc=None):
        self._response = response
        self._exc = exc
        self.last_kwargs = None

    def create(self, **kwargs):
        self.last_kwargs = kwargs
        if self._exc is not None:
            raise self._exc
        return self._response


class _FakeClient:
    def __init__(self, response=None, exc=None):
        self.messages = _FakeMessages(response=response, exc=exc)


_PAIRS = [(0, 1), (1, 1)]
_LABELS = {(0, 1): {"purchase"}, (1, 1): {"purchase"}}
_EDGE_COLUMNS = {"amount": "DOUBLE"}


def _draft_response(*, per_pair, is_viable_d_final="TRUE", is_viable_d_final_confident=True,
                     init_d="{last_amount: NULL}", finalize_d="D"):
    return _FakeResponse([_FakeToolUseBlock({
        "init_d": init_d,
        "finalize_d": finalize_d,
        "is_viable_d_final": is_viable_d_final,
        "is_viable_d_final_confident": is_viable_d_final_confident,
        "per_pair": per_pair,
    })])


def test_confident_draft_used_verbatim():
    per_pair = [
        {"from_state": 0, "to_state": 1, "update_d": "{last_amount: e.amount}",
         "is_viable_d": "TRUE", "is_viable_d_confident": True},
        {"from_state": 1, "to_state": 1, "update_d": "{last_amount: e.amount}",
         "is_viable_d": "e.amount <= D.last_amount", "is_viable_d_confident": True},
    ]
    client = _FakeClient(response=_draft_response(per_pair=per_pair))
    result = propose_general_aggregate(
        constraint_description="amounts must never increase", transition_pairs=_PAIRS,
        labels_by_pair=_LABELS, edge_columns=_EDGE_COLUMNS, client=client)

    assert result.init_d == "{last_amount: NULL}"
    assert result.update_d == {(0, 1): "{last_amount: e.amount}", (1, 1): "{last_amount: e.amount}"}
    assert result.is_viable_d == {(0, 1): "TRUE", (1, 1): "e.amount <= D.last_amount"}
    assert result.unconfident_pairs == frozenset()
    assert result.is_viable_d_final_unconfident is False
    # The tool_choice must force this exact tool -- no free-text fallback to parse.
    assert client.messages.last_kwargs["tool_choice"] == {"type": "tool", "name": "propose_selective_aggregate"}


def test_unconfident_pair_overridden_to_true_but_raw_attempt_kept():
    per_pair = [
        {"from_state": 0, "to_state": 1, "update_d": "{last_amount: e.amount}",
         "is_viable_d": "e.amount <= 500", "is_viable_d_confident": False},
        {"from_state": 1, "to_state": 1, "update_d": "{last_amount: e.amount}",
         "is_viable_d": "TRUE", "is_viable_d_confident": True},
    ]
    client = _FakeClient(response=_draft_response(per_pair=per_pair))
    result = propose_general_aggregate(
        constraint_description="amounts must stay small", transition_pairs=_PAIRS,
        labels_by_pair=_LABELS, edge_columns=_EDGE_COLUMNS, client=client)

    assert result.is_viable_d[(0, 1)] == "TRUE"  # fail-safe override, not the model's real attempt
    assert result.raw_is_viable_d[(0, 1)] == "e.amount <= 500"  # kept, unvetted, for the UI to show
    assert result.unconfident_pairs == frozenset({(0, 1)})
    assert result.is_viable_d[(1, 1)] == "TRUE"  # this one really was confident


def test_unconfident_is_viable_d_final_overridden_too():
    per_pair = [
        {"from_state": 0, "to_state": 1, "update_d": "D", "is_viable_d": "TRUE", "is_viable_d_confident": True},
    ]
    client = _FakeClient(response=_draft_response(
        per_pair=per_pair, is_viable_d_final="D.total >= 1000", is_viable_d_final_confident=False))
    result = propose_general_aggregate(
        constraint_description="total at least 1000", transition_pairs=[(0, 1)],
        labels_by_pair={(0, 1): {"purchase"}}, edge_columns=_EDGE_COLUMNS, client=client)

    assert result.is_viable_d_final == "TRUE"
    assert result.raw_is_viable_d_final == "D.total >= 1000"
    assert result.is_viable_d_final_unconfident is True


def test_hallucinated_pair_ignored_not_erroring():
    per_pair = [
        {"from_state": 0, "to_state": 1, "update_d": "D", "is_viable_d": "TRUE", "is_viable_d_confident": True},
        {"from_state": 9, "to_state": 9, "update_d": "D", "is_viable_d": "TRUE", "is_viable_d_confident": True},
    ]
    client = _FakeClient(response=_draft_response(per_pair=per_pair))
    result = propose_general_aggregate(
        constraint_description="anything", transition_pairs=_PAIRS, labels_by_pair=_LABELS,
        edge_columns=_EDGE_COLUMNS, client=client)

    assert set(result.update_d) == {(0, 1)}  # (9, 9) silently dropped, (1, 1) just missing from the draft


def test_missing_field_in_per_pair_item_raises_parse_error():
    per_pair = [{"from_state": 0, "to_state": 1, "update_d": "D"}]  # missing is_viable_d/confidence
    client = _FakeClient(response=_draft_response(per_pair=per_pair))
    with pytest.raises(ProposerParseError):
        propose_general_aggregate(constraint_description="x", transition_pairs=_PAIRS,
                                   labels_by_pair=_LABELS, edge_columns=_EDGE_COLUMNS, client=client)


def test_model_not_calling_tool_raises_parse_error():
    client = _FakeClient(response=_FakeResponse([_FakeTextBlock("I'd rather not.")]))
    with pytest.raises(ProposerParseError):
        propose_general_aggregate(constraint_description="x", transition_pairs=_PAIRS,
                                   labels_by_pair=_LABELS, edge_columns=_EDGE_COLUMNS, client=client)


def test_empty_constraint_description_raises_before_any_call():
    client = _FakeClient(response=_draft_response(per_pair=[]))
    with pytest.raises(ProposerParseError):
        propose_general_aggregate(constraint_description="   ", transition_pairs=_PAIRS,
                                   labels_by_pair=_LABELS, edge_columns=_EDGE_COLUMNS, client=client)
    assert client.messages.last_kwargs is None  # never called the model at all


def test_sdk_call_failure_wrapped_as_proposer_unavailable():
    client = _FakeClient(exc=RuntimeError("connection reset"))
    with pytest.raises(ProposerUnavailableError):
        propose_general_aggregate(constraint_description="x", transition_pairs=_PAIRS,
                                   labels_by_pair=_LABELS, edge_columns=_EDGE_COLUMNS, client=client)


def test_missing_api_key_raises_proposer_unavailable(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(ProposerUnavailableError):
        propose_general_aggregate(constraint_description="x", transition_pairs=_PAIRS,
                                   labels_by_pair=_LABELS, edge_columns=_EDGE_COLUMNS, client=None)
