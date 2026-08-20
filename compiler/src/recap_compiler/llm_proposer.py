"""Module J (optional): LLM-assisted General (non-factorized) selective
-aggregate authoring.

A first version of this module (local Ollama models)
was built, live-tested, and then removed entirely per explicit user
decision on 2026-08-11 -- see `CHECKLIST.md`'s "Module J -- built,
live-tested, then removed" entry for the full history, including two real
model-accuracy findings (both false negatives, never a false positive) and
why local inference on this machine's hardware was the dominant
bottleneck (40s-8min per draft). This version was reintroduced
2026-08-18 with two deliberate differences, both explicit user decisions:

- **Backend:** a hosted Claude API model instead of local Ollama --
  removes the hardware bottleneck entirely, at the cost of a network
  dependency, an API key, and a per-call charge.
- **Scope:** a *full per-transition-pair* draft. General mode's own table
  has one `update_d`/`is_viable_d` row per `(from_state, to_state)`
  transition pair (Figure 5's per-transition boxes) -- this module drafts
  all of them at once, seeing the whole automaton shape, rather than one
  factorized body applied uniformly regardless of state.

**Follow-up (2026-08-20): the local-Ollama backend is back, as a toggle
next to the hosted one, not a replacement** -- per explicit user request
for a no-authentication option. Both backends share every line of prompt
construction, the JSON schema, and the fail-safe/parsing logic below;
`backend="ollama"` only swaps out *how the raw draft dict is obtained*
(`_call_ollama`, a local HTTP POST to `http://localhost:11434/api/chat`
using Ollama's own `format=<json schema>` structured-output feature --
confirmed working directly against this machine's already-pulled
`qwen2.5:3b-instruct` before wiring it in) for `_call_anthropic`'s
`tool_choice`-forced tool call. The known cost from the original
version -- 40s-8min per draft on this machine's hardware, and weaker
accuracy from the smaller/faster models -- is unchanged and not
re-litigated here; this just gives the author the choice instead of
deciding it for them.

Same non-negotiable safety property as the first version: **no privileged
path for LLM output vs. hand-written input.** Whatever this module
produces lands in the exact same text boxes/table cells a human would type
into, and goes through the exact same reference validation and optimizer
equivalence checks before it ever touches real data -- this
module's only job is to produce a plausible *first draft*, not a trusted
answer.

**Fail-safe kept from the first version, adapted to the per-pair scope:**
`is_viable_d` (per pair) and `is_viable_d_final` are pruning predicates --
an unsound one can make the query silently *undercount* real results,
which a user reviewing the output has no way to notice on its own (there's
nothing visibly wrong with a query that returns too few correct-looking
rows). `update_d`/`init_d`/`finalize_d` carry no such risk -- a wrong one
just computes the wrong value, which review/testing catches the same way
any hand-written mistake would. So only `is_viable_d`/`is_viable_d_final`
get a per-item confidence self-report from the model; whenever it reports
low confidence, `ProposedAggregate` overrides that one item to `TRUE`
(never prune) while still keeping the model's original, unvetted attempt
available (`raw_is_viable_d`/`raw_is_viable_d_final`) for the UI to show
clearly flagged, so the user can manually promote it if they judge it
sound -- visibility instead of concealment, same as before.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Callable

from .errors import ProposerParseError, ProposerUnavailableError
from .selective_aggregate import TransitionPair

DEFAULT_MODEL = "claude-sonnet-5"
DEFAULT_OLLAMA_MODEL = "qwen2.5:7b-instruct-q4_K_M"
DEFAULT_OLLAMA_HOST = "http://localhost:11434"
_OLLAMA_HOST_ENV = "OLLAMA_HOST"

_TOOL_NAME = "propose_selective_aggregate"

_FUNCTION_SPECS = """\
A selective aggregate is up to five SQL expression bodies over a
dictionary D, matching DuckDB expression syntax exactly (no other
language):

- init_d(): the dictionary's value at the anchor (path length 0), before
  any edge is taken. Nothing is in scope (no D, no e) -- literals/constants
  only. Its keys and their types are inferred from this struct literal, so
  every key the aggregate ever tracks must appear here.
- update_d(D, from_state, to_state, e): the dictionary's new value after
  taking one more edge. `D.<key>` reads the previous hop's dictionary,
  `e.<column>` reads a property of the edge just taken, bare `from_state`/
  `to_state` are the NFA transition just taken. Must return a struct
  literal `{key: expr, ...}` covering every declared key -- a key you don't
  want to change should just be echoed back as `D.<key>`.
- is_viable_d(D, from_state, to_state, e): TRUE if taking this edge (from
  from_state to to_state) keeps the path viable; FALSE prunes it
  immediately. Same D/e/from_state/to_state scope as update_d. This is a
  pruning predicate -- an unsound one silently drops real results with no
  visible symptom, so only use it for a check that provably can only get
  *more* restrictive as the path grows (a monotone bound, a non-negative
  running sum against an upper limit, etc.) -- never for a check that
  could pass now and fail later, or vice versa.
- is_viable_d_final(D): a one-time additional check on a *completed*
  path's final D, on top of is_viable_d having held at every hop -- e.g. a
  total that can only be evaluated once the path is done. Same soundness
  caveat as is_viable_d.
- finalize_d(D): what a matched path reports for D in the result set --
  usually just D itself, but may project down to a subset of interest.
"""

_GUIDING_EXAMPLE = """\
Guiding example (paper's own Figure 5, Q_B): regex `Domestic+ Foreign`
compiles to transition pairs {(1,2,Domestic), (2,2,Domestic), (2,3,Foreign)},
q0=1, accepting={3}. The aggregate tracks last_time and edge_ids (a trail):

- init_d(): {last_time: NULL, edge_ids: []}
- update_d is the *same* on every pair (it doesn't depend on state):
  {last_time: e.time, edge_ids: list_append(D.edge_ids, e.id)}
- is_viable_d *does* differ per pair -- staying inside the Domestic+ run
  (2->2) allows a wider time gap than the one final hop into Foreign
  (2->3), and every pair also forbids revisiting an edge already in the
  trail:
    (1,2): NOT list_contains(D.edge_ids, e.id)
    (2,2): NOT list_contains(D.edge_ids, e.id) AND abs(e.time - D.last_time) <= 2
    (2,3): NOT list_contains(D.edge_ids, e.id) AND abs(e.time - D.last_time) <= 3
- is_viable_d_final(D): TRUE (no additional final check needed here)
- finalize_d(D): D
"""

_TOOL_SCHEMA = {
    "name": _TOOL_NAME,
    "description": "Draft a General (non-factorized) selective aggregate: "
                    "init_d, one update_d/is_viable_d pair per transition, "
                    "is_viable_d_final, and finalize_d.",
    "input_schema": {
        "type": "object",
        "properties": {
            "init_d": {"type": "string", "description": "init_d()'s body -- a struct literal."},
            "finalize_d": {"type": "string", "description": "finalize_d(D)'s body."},
            "is_viable_d_final": {"type": "string", "description": "is_viable_d_final(D)'s body."},
            "is_viable_d_final_confident": {
                "type": "boolean",
                "description": "True only if is_viable_d_final is a provably sound (never "
                                "undercounts) check. False if unsure -- it will be overridden "
                                "to TRUE for safety and shown to the user separately.",
            },
            "per_pair": {
                "type": "array",
                "description": "One entry per (from_state, to_state) transition pair given in the prompt.",
                "items": {
                    "type": "object",
                    "properties": {
                        "from_state": {"type": "integer"},
                        "to_state": {"type": "integer"},
                        "update_d": {"type": "string"},
                        "is_viable_d": {"type": "string"},
                        "is_viable_d_confident": {
                            "type": "boolean",
                            "description": "True only if this pair's is_viable_d is a provably "
                                            "sound (never undercounts) check. False if unsure.",
                        },
                    },
                    "required": ["from_state", "to_state", "update_d", "is_viable_d",
                                 "is_viable_d_confident"],
                },
            },
        },
        "required": ["init_d", "finalize_d", "is_viable_d_final",
                     "is_viable_d_final_confident", "per_pair"],
    },
}


@dataclass(frozen=True)
class ProposedAggregate:
    """A drafted General selective aggregate, fail-safe already applied.

    `raw_is_viable_d`/`raw_is_viable_d_final` always hold the model's
    original attempt (even where confident) -- the UI's job, not this
    module's, is to decide how prominently to show the unconfident subset.
    `unconfident_pairs` and `is_viable_d_final_unconfident` mark exactly
    which entries in `is_viable_d`/`is_viable_d_final` were overridden."""

    init_d: str
    update_d: dict[TransitionPair, str]
    is_viable_d: dict[TransitionPair, str]
    is_viable_d_final: str
    finalize_d: str
    raw_is_viable_d: dict[TransitionPair, str] = field(default_factory=dict)
    raw_is_viable_d_final: str = "TRUE"
    unconfident_pairs: frozenset[TransitionPair] = frozenset()
    is_viable_d_final_unconfident: bool = False


def _build_prompt(*, constraint_description: str, transition_pairs: list[TransitionPair],
                   labels_by_pair: dict[TransitionPair, set[str]],
                   edge_columns: dict[str, str]) -> str:
    pairs_lines = "\n".join(
        f"  ({frm}, {to}): labels {{{', '.join(sorted(labels_by_pair.get((frm, to), ())))}}}"
        for frm, to in sorted(transition_pairs)
    )
    columns_lines = "\n".join(f"  e.{col} ({sql_type})" for col, sql_type in edge_columns.items())
    return f"""\
{_FUNCTION_SPECS}
{_GUIDING_EXAMPLE}
Now draft a selective aggregate for this automaton and constraint.

Transition pairs (draft update_d and is_viable_d for every one of these,
and only these):
{pairs_lines}

Edge properties available as e.<column>:
{columns_lines}

Constraint, in the user's own words:
{constraint_description}

Call {_TOOL_NAME} with your draft."""


def _client_from_env(api_key: str | None = None):
    """`api_key` lets a caller (e.g. the workbench's own optional inline
    field) hand in a key directly, for a user who'd rather paste it into
    the running app than export an environment variable first -- falls
    back to `ANTHROPIC_API_KEY` when not given, unchanged from before."""
    api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ProposerUnavailableError(
            "no Anthropic API key given -- set ANTHROPIC_API_KEY, or paste one directly "
            "into the workbench, before drafting with the LLM "
            "(https://console.anthropic.com/settings/keys)", locus="llm_proposer")
    try:
        import anthropic
    except ImportError as exc:
        raise ProposerUnavailableError(
            "the 'anthropic' package isn't installed -- `pip install anthropic`",
            locus="llm_proposer") from exc
    return anthropic.Anthropic(api_key=api_key)


def _call_anthropic(client, model: str, prompt: str) -> dict:
    """Forces the hosted model to call `_TOOL_NAME`, so the draft is
    already structured JSON -- no free-text parsing. Raises
    `ProposerUnavailableError` on any SDK/network failure,
    `ProposerParseError` if the model answered without calling the tool."""
    try:
        response = client.messages.create(
            model=model, max_tokens=4096,
            tools=[_TOOL_SCHEMA], tool_choice={"type": "tool", "name": _TOOL_NAME},
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as exc:  # the SDK's own exception hierarchy -- not worth import-coupling to
        raise ProposerUnavailableError(f"LLM call failed: {exc}", locus="llm_proposer") from exc

    tool_use = next((block for block in response.content if getattr(block, "type", None) == "tool_use"), None)
    if tool_use is None:
        raise ProposerParseError("model did not call the expected tool", locus="llm_proposer")
    return tool_use.input


def _call_ollama(model: str, prompt: str, *, host: str | None = None) -> dict:
    """Local counterpart to `_call_anthropic`: no API key, no network
    beyond localhost, no per-call charge -- just a POST to an already-
    running `ollama serve`. Uses Ollama's own `format=<json schema>`
    structured-output feature (passing `_TOOL_SCHEMA`'s own
    `input_schema` straight through -- it's already a plain JSON Schema
    object) rather than tool-calling, since that's what this project's
    own first version of this module relied on (see CHECKLIST.md's
    Module J history) and it's confirmed to work directly against this
    machine's already-pulled models. Deliberately stdlib-only
    (`urllib`), so the free/no-auth path doesn't pull in a new
    dependency either."""
    host = host or os.environ.get(_OLLAMA_HOST_ENV, DEFAULT_OLLAMA_HOST)
    payload = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "format": _TOOL_SCHEMA["input_schema"],
        "stream": False,
    }).encode("utf-8")
    request = urllib.request.Request(
        f"{host.rstrip('/')}/api/chat", data=payload,
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=600) as response:
            body = json.loads(response.read())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise ProposerUnavailableError(
            f"Ollama at {host} rejected the request ({exc.code}): {detail} -- if this names a "
            f"model, pull it first with `ollama pull {model}`", locus="llm_proposer") from exc
    except urllib.error.URLError as exc:
        raise ProposerUnavailableError(
            f"could not reach a local Ollama server at {host} -- is `ollama serve` running? "
            f"({exc})", locus="llm_proposer") from exc

    content = body.get("message", {}).get("content", "")
    try:
        return json.loads(content)
    except json.JSONDecodeError as exc:
        raise ProposerParseError(
            f"Ollama's response wasn't valid JSON despite the format constraint: {exc}",
            locus="llm_proposer") from exc


def _validate_pair_item(item: dict, *, valid_pairs: set[TransitionPair]) -> TransitionPair | None:
    pair = (item.get("from_state"), item.get("to_state"))
    if pair not in valid_pairs:
        return None  # a hallucinated pair -- ignore rather than error, same as an unedited row
    for required in ("update_d", "is_viable_d", "is_viable_d_confident"):
        if required not in item:
            raise ProposerParseError(
                f"drafted pair {pair} is missing required field {required!r}", locus="llm_proposer")
    return pair


def propose_general_aggregate(*, constraint_description: str,
                               transition_pairs: list[TransitionPair],
                               labels_by_pair: dict[TransitionPair, set[str]],
                               edge_columns: dict[str, str],
                               backend: str = "anthropic",
                               client=None, model: str | None = None,
                               api_key: str | None = None,
                               ollama_host: str | None = None,
                               ollama_call: Callable[[str, str], dict] | None = None) -> ProposedAggregate:
    """Drafts a full General selective aggregate, given the user's
    plain-English `constraint_description`, the automaton's own
    transition pairs (with their labels), and the edge columns available
    as `e.<column>`.

    `backend` picks how the draft is actually obtained -- both share every
    other line here (prompt, JSON schema, fail-safe overriding):
    - `"anthropic"` (default): a hosted Claude model, forced via
      `tool_choice` to answer with structured JSON. Needs an Anthropic API
      key and the `anthropic` package (`pip install 'recap-compiler[llm]'`).
      `api_key`, if given, is used directly instead of reading
      `ANTHROPIC_API_KEY` from the environment (ignored once `client` is
      also given). `client` is injectable (any object exposing
      `.messages.create(...)` the way `anthropic.Anthropic()` does) so
      tests don't need a real network call or API key.
    - `"ollama"`: a local model via an already-running `ollama serve` --
      no API key, no network beyond localhost, no per-call charge, but
      subject to this machine's own hardware being the bottleneck (see
      the module docstring). `ollama_host` overrides the default
      `http://localhost:11434` (or the `OLLAMA_HOST` env var).
      `ollama_call` is injectable the same way `client` is for the
      Anthropic backend (a `(model, prompt) -> dict` callable) so tests
      don't need a real Ollama server."""
    if not constraint_description.strip():
        raise ProposerParseError("constraint description is empty -- describe the "
                                  "constraint before drafting", locus="llm_proposer")
    if backend not in ("anthropic", "ollama"):
        raise ValueError(f"unknown backend {backend!r} -- expected 'anthropic' or 'ollama'")

    prompt = _build_prompt(constraint_description=constraint_description,
                            transition_pairs=transition_pairs, labels_by_pair=labels_by_pair,
                            edge_columns=edge_columns)
    if backend == "anthropic":
        if client is None:
            client = _client_from_env(api_key)
        draft = _call_anthropic(client, model or DEFAULT_MODEL, prompt)
    else:
        call = ollama_call or _call_ollama
        draft = call(model or DEFAULT_OLLAMA_MODEL, prompt, host=ollama_host)

    for required in ("init_d", "finalize_d", "is_viable_d_final",
                      "is_viable_d_final_confident", "per_pair"):
        if required not in draft:
            raise ProposerParseError(f"drafted response is missing required field {required!r}",
                                      locus="llm_proposer")

    valid_pairs = set(transition_pairs)
    update_d: dict[TransitionPair, str] = {}
    is_viable_d: dict[TransitionPair, str] = {}
    raw_is_viable_d: dict[TransitionPair, str] = {}
    unconfident_pairs: set[TransitionPair] = set()
    for item in draft["per_pair"]:
        pair = _validate_pair_item(item, valid_pairs=valid_pairs)
        if pair is None:
            continue
        update_d[pair] = item["update_d"]
        raw_is_viable_d[pair] = item["is_viable_d"]
        if item["is_viable_d_confident"]:
            is_viable_d[pair] = item["is_viable_d"]
        else:
            is_viable_d[pair] = "TRUE"
            unconfident_pairs.add(pair)

    is_viable_d_final_confident = bool(draft["is_viable_d_final_confident"])
    return ProposedAggregate(
        init_d=draft["init_d"],
        update_d=update_d,
        is_viable_d=is_viable_d,
        is_viable_d_final=draft["is_viable_d_final"] if is_viable_d_final_confident else "TRUE",
        finalize_d=draft["finalize_d"],
        raw_is_viable_d=raw_is_viable_d,
        raw_is_viable_d_final=draft["is_viable_d_final"],
        unconfident_pairs=frozenset(unconfident_pairs),
        is_viable_d_final_unconfident=not is_viable_d_final_confident,
    )
