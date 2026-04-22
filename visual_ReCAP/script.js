const modeButtons = document.querySelectorAll(".mode-toggle");
const naiveEditor = document.getElementById("naive-editor");
const advancedEditor = document.getElementById("advanced-editor");
const activeModeLabel = document.getElementById("active-mode-label");
const activeRegexLabel = document.getElementById("active-regex-label");
const editorTitle = document.getElementById("editor-title");
const editorBadge = document.getElementById("editor-badge");
const dataDrawer = document.getElementById("data-drawer");
const toggleDataButton = document.getElementById("toggle-data");
const datasetHead = document.getElementById("dataset-head");
const datasetBody = document.getElementById("dataset-body");
const lowerBound = document.getElementById("lower-bound");
const upperBound = document.getElementById("upper-bound");
const startNodeInput = document.getElementById("start-node");
const regexForm = document.getElementById("regex-form");
const regexInput = document.getElementById("regex-input");
const nfaVisualization = document.getElementById("nfa-visualization");
const nfaCaption = document.getElementById("nfa-caption");
const nfaStatus = document.getElementById("nfa-status");
const pythonEditor = document.getElementById("python-editor");
const pythonOutput = document.getElementById("python-output");
const runPythonButton = document.getElementById("run-python");
const runtimeStatus = document.getElementById("runtime-status");
const advancedRuntimeStatus = document.getElementById("advanced-runtime-status");
const advancedPythonOutput = document.getElementById("advanced-python-output");
const runAdvancedPythonButton = document.getElementById("run-advanced-python");
const advancedInitD = document.getElementById("advanced-init-d");
const advancedUpdateD = document.getElementById("advanced-update-d");
const advancedIsViableD = document.getElementById("advanced-is-viable-d");
const advancedFinalizeD = document.getElementById("advanced-finalize-d");
const advancedIsViableDFinal = document.getElementById("advanced-is-viable-d-final");
const runRecapButton = document.getElementById("run-recap");
const recapResult = document.getElementById("recap-result");
const runStatus = document.getElementById("run-status");
const runStatusText = document.getElementById("run-status-text");
const caseButtons = document.querySelectorAll(".case-button");

let pyodideRuntime = null;
let datasetLoaded = false;
const sampleDInput = [
  {
    edge_id: 1,
    id: 1,
    timestamp_ms: 1642523219000,
    time: 1642523219000,
    src: 266,
    dst: 421,
    amount: 778.197389885983,
    label: "purchase",
    location_region: "Asia",
    purchase_pattern: "focused",
    age_group: "established",
    risk_score: 31.25,
    anomaly: "low_risk",
    day: 16,
  },
  {
    edge_id: 23,
    id: 23,
    timestamp_ms: 1655389614000,
    time: 1655389614000,
    src: 52,
    dst: 981,
    amount: 487.14214850355944,
    label: "purchase",
    location_region: "Asia",
    purchase_pattern: "focused",
    age_group: "established",
    risk_score: 26.25,
    anomaly: "low_risk",
    day: 14,
  },
  {
    edge_id: 30,
    id: 30,
    timestamp_ms: 1650175457000,
    time: 1650175457000,
    src: 281,
    dst: 1147,
    amount: 494.9013365203876,
    label: "purchase",
    location_region: "Africa",
    purchase_pattern: "focused",
    age_group: "established",
    risk_score: 35.4375,
    anomaly: "low_risk",
    day: 6,
  },
];
const advancedSampleEdges = [...sampleDInput];
const advancedSampleTransitions = [
  ["q0", "q1"],
  ["q1", "q1"],
  ["q1", "qF"],
];

function setMode(mode) {
  const isAdvanced = mode === "advanced";

  modeButtons.forEach((button) => {
    button.classList.toggle("active", button.dataset.mode === mode);
  });

  naiveEditor.classList.toggle("active", !isAdvanced);
  advancedEditor.classList.toggle("active", isAdvanced);
  activeModeLabel.textContent = isAdvanced ? "Advanced ReCAP" : "Naive baseline";
  editorTitle.textContent = isAdvanced
    ? "Selective Aggregate Functions"
    : "Python Constraint Sandbox";
  editorBadge.textContent = isAdvanced
    ? "5-function ReCAP surface"
    : "D accumulates edge tuples";
}

modeButtons.forEach((button) => {
  button.addEventListener("click", () => setMode(button.dataset.mode));
});

toggleDataButton.addEventListener("click", () => {
  const isCollapsed = dataDrawer.classList.toggle("collapsed");
  toggleDataButton.textContent = isCollapsed
    ? "Reveal dataset sample"
    : "Hide dataset sample";

  if (!isCollapsed && !datasetLoaded) {
    loadDatasetPreview();
  }
});

function syncBounds() {
  if (Number(lowerBound.value) > Number(upperBound.value)) {
    upperBound.value = lowerBound.value;
  }
}

lowerBound.addEventListener("input", syncBounds);
upperBound.addEventListener("input", () => {
  if (Number(upperBound.value) < Number(lowerBound.value)) {
    lowerBound.value = upperBound.value;
  }
});

function escapeHtml(value) {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function parseRegex(regex) {
  const trimmed = regex.trim();
  const parts = trimmed.match(/[A-Za-z0-9_]+(?:[+*?])?/g) || [];
  return parts.map((part) => {
    const hasQuantifier = /[+*?]$/.test(part);
    const label = hasQuantifier ? part.slice(0, -1) : part;
    const quantifier = hasQuantifier ? part.at(-1) : "";
    return { label: label || "token", quantifier };
  });
}

function buildNfaModel(regex) {
  const tokens = parseRegex(regex);
  const usableTokens = tokens.length ? tokens : [{ label: "edge", quantifier: "" }];
  const finalState = usableTokens.length;
  const nodes = Array.from({ length: finalState + 1 }, (_, id) => ({
    id,
    type: id === 0 ? "initial" : id === finalState ? "accepting" : "normal",
  }));
  const edges = [];

  usableTokens.forEach((token, index) => {
    edges.push({
      from_state: index,
      to_state: index + 1,
      label: token.label,
      kind: "forward",
    });

    if (token.quantifier === "+") {
      edges.push({
        from_state: index + 1,
        to_state: index + 1,
        label: token.label,
        kind: "loop",
      });
    }

    if (token.quantifier === "*") {
      edges.push({
        from_state: index + 1,
        to_state: index + 1,
        label: token.label,
        kind: "loop",
      });
      edges.push({
        from_state: index,
        to_state: index + 1,
        label: "epsilon",
        kind: "epsilon",
      });
    }

    if (token.quantifier === "?") {
      edges.push({
        from_state: index,
        to_state: index + 1,
        label: "epsilon",
        kind: "epsilon",
      });
    }
  });

  return { nodes, edges, tokens: usableTokens };
}

function buildNfaSvg(regex) {
  const { nodes, edges } = buildNfaModel(regex);
  const stateCount = nodes.length;
  const width = Math.max(720, 220 + stateCount * 180);
  const height = 320;
  const startX = 140;
  const gap = 180;
  const y = 170;

  const defs = `
    <defs>
      <linearGradient id="glow" x1="0%" y1="0%" x2="100%" y2="0%">
        <stop offset="0%" stop-color="#f7b267" />
        <stop offset="100%" stop-color="#7bdff2" />
      </linearGradient>
      <marker
        id="arrow"
        viewBox="0 0 10 10"
        refX="8"
        refY="5"
        markerWidth="8"
        markerHeight="8"
        orient="auto-start-reverse"
      >
        <path d="M 0 0 L 10 5 L 0 10 z" fill="#f7f2ea"></path>
      </marker>
    </defs>
  `;

  let shapes = `
    <path
      d="M 40 ${y} C 70 ${y}, 90 ${y}, ${startX - 40} ${y}"
      class="edge-line"
      marker-end="url(#arrow)"
    />
    <text x="88" y="${y - 16}" text-anchor="middle" class="edge-label">start</text>
  `;

  nodes.forEach((node) => {
    const x = startX + node.id * gap;
    const isFinal = node.type === "accepting";

    shapes += `
      <circle cx="${x}" cy="${y}" r="40" class="node" />
      ${isFinal ? `<circle cx="${x - 8}" cy="${y}" r="32" class="node inner-final" />` : ""}
      <text x="${x}" y="${y + 7}" text-anchor="middle" class="node-label">q${node.id}</text>
    `;
  });

  edges.forEach((edge, edgeIndex) => {
    const fromX = startX + edge.from_state * gap;
    const toX = startX + edge.to_state * gap;
    const label = escapeHtml(edge.label);

    if (edge.from_state === edge.to_state) {
      shapes += `
        <path
          d="M ${fromX - 26} ${y - 8} C ${fromX - 72} ${y - 96}, ${fromX + 72} ${y - 96}, ${fromX + 26} ${y - 8}"
          class="edge-line dashed"
          marker-end="url(#arrow)"
        />
        <text x="${fromX}" y="${y - 104}" text-anchor="middle" class="edge-label">${label} loop</text>
      `;
      return;
    }

    const isEpsilon = edge.kind === "epsilon";
    const curveOffset = isEpsilon ? -66 : 0;
    const labelY = isEpsilon ? y - 82 : y - 22;

    shapes += `
      <path
        d="M ${fromX + 40} ${y} C ${fromX + 78} ${y + curveOffset}, ${toX - 78} ${y + curveOffset}, ${toX - 40} ${y}"
        class="edge-line ${isEpsilon ? "dashed" : ""}"
        marker-end="url(#arrow)"
      />
      <text x="${(fromX + toX) / 2}" y="${labelY}" text-anchor="middle" class="edge-label">${label}</text>
    `;
  });

  return `
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Generated NFA diagram">
      ${defs}
      ${shapes}
    </svg>
  `;
}

function buildNfaTables(regex) {
  const { edges } = buildNfaModel(regex);
  const edgeRows = edges
    .map(
      (edge) => `
        <tr>
          <td>${edge.from_state}</td>
          <td>${edge.to_state}</td>
          <td>${escapeHtml(edge.label)}</td>
        </tr>
      `,
    )
    .join("");

  return `
    <div class="nfa-table-grid">
      <div class="nfa-mini-table">
        <div class="editor-title">nfa.csv</div>
        <table>
          <thead><tr><th>from_state</th><th>to_state</th><th>label</th></tr></thead>
          <tbody>${edgeRows}</tbody>
        </table>
      </div>
    </div>
  `;
}

function buildCaseFunction(target) {
  const { edges } = buildNfaModel(regexInput.value);
  const branches = edges
    .map((edge) => {
      const dummyReturn = target === "update_d" ? "D" : "True";
      return `    if P_nfa_state == ${edge.from_state} and T_to_state == ${edge.to_state}:
        # transition label: ${edge.label}
        return ${dummyReturn}`;
    })
    .join("\n");

  if (target === "update_d") {
    return `def update_d(D, P_nfa_state, T_to_state, Edge):
${branches}
    return D`;
  }

  return `def is_viable_d(D, P_nfa_state, T_to_state, Edge):
${branches}
    return True`;
}

function buildWithoutCaseFunction(target) {
  if (target === "update_d") {
    return `def update_d(D, P_nfa_state, T_to_state, Edge):
    D["edge_ids"] = D["edge_ids"] + [Edge["id"]]
    D["last_time"] = Edge["time"]
    D["last_transition"] = (P_nfa_state, T_to_state)
    return D`;
  }

  return `def is_viable_d(D, P_nfa_state, T_to_state, Edge):
    return True`;
}

function setCaseMode(target, mode) {
  const textarea = target === "update_d" ? advancedUpdateD : advancedIsViableD;
  textarea.value = mode === "with" ? buildCaseFunction(target) : buildWithoutCaseFunction(target);

  caseButtons.forEach((button) => {
    if (button.dataset.caseTarget === target) {
      button.classList.toggle("active", button.dataset.caseMode === mode);
    }
  });
}

function renderNfa(regex) {
  const normalized = regex.trim() || "Domestic+Foreign";
  nfaVisualization.innerHTML = buildNfaSvg(normalized);
  document.getElementById("nfa-table-panel").innerHTML = buildNfaTables(normalized);
  activeRegexLabel.textContent = normalized;
  nfaStatus.textContent = "Prototype NFA generated";
  nfaCaption.innerHTML = `Regex-derived automaton preview for <code>${escapeHtml(normalized)}</code>.`;
}

regexInput.addEventListener("input", () => {
  const nextValue = regexInput.value.trim() || "Domestic+Foreign";
  activeRegexLabel.textContent = nextValue;
});

regexForm.addEventListener("submit", (event) => {
  event.preventDefault();
  generateNfaAndRender(regexInput.value);
});

caseButtons.forEach((button) => {
  button.addEventListener("click", () => {
    setCaseMode(button.dataset.caseTarget, button.dataset.caseMode);
  });
});

renderNfa(regexInput.value);

async function ensurePyRuntime() {
  if (pyodideRuntime) {
    return pyodideRuntime;
  }

  if (typeof loadPyodide !== "function") {
    throw new Error("Pyodide could not be loaded. Check your internet connection.");
  }

  runtimeStatus.textContent = "Loading Python runtime...";
  pyodideRuntime = await loadPyodide();
  runtimeStatus.textContent = "Python runtime ready";
  return pyodideRuntime;
}

async function runPythonCode() {
  pythonOutput.textContent = "Running Python...";

  try {
    const pyodide = await ensurePyRuntime();
    pyodide.globals.set("D_INPUT", pyodide.toPy(sampleDInput));
    const wrappedCode = `
class DProxy(list):
    def __getitem__(self, key):
        if isinstance(key, str):
            return [edge[key] for edge in list(self)]
        return super().__getitem__(key)

${pythonEditor.value
  .split("\n")
  .map((line) => `${line}`)
  .join("\n")}
D = DProxy(D_INPUT.to_py())
_codex_result = is_viable_d_final(D)
assert isinstance(_codex_result, bool), "is_viable_d_final(D) must return True or False"
str(_codex_result)
`;

    const result = await pyodide.runPythonAsync(wrappedCode);
    pythonOutput.textContent = `is_viable_d_final(D) returned ${result}`;
  } catch (error) {
    runtimeStatus.textContent = "Python runtime unavailable";
    pythonOutput.textContent = `Python error:\n${error.message}`;
  }
}

runPythonButton.addEventListener("click", () => {
  runPythonCode();
});

async function runAdvancedPythonCode() {
  advancedPythonOutput.textContent = "Running advanced Python...";

  try {
    const pyodide = await ensurePyRuntime();
    pyodide.globals.set("EDGE_SEQUENCE", pyodide.toPy(advancedSampleEdges));
    pyodide.globals.set("TRANSITIONS", pyodide.toPy(advancedSampleTransitions));

    const advancedCode = [
      advancedInitD.value,
      advancedUpdateD.value,
      advancedIsViableD.value,
      advancedFinalizeD.value,
      advancedIsViableDFinal.value,
    ].join("\n\n");

    const wrappedCode = `
${advancedCode}

D = init_d()
for transition, Edge in zip(TRANSITIONS, EDGE_SEQUENCE):
    P_nfa_state, T_to_state = transition
    viable = is_viable_d(D, P_nfa_state, T_to_state, Edge)
    assert isinstance(viable, bool), "is_viable_d(...) must return True or False"
    if not viable:
        break
    D = update_d(D, P_nfa_state, T_to_state, Edge)

D = finalize_d(D)
_codex_result = is_viable_d_final(D)
assert isinstance(_codex_result, bool), "is_viable_d_final(D) must return True or False"
str(_codex_result)
`;

    const result = await pyodide.runPythonAsync(wrappedCode);
    advancedRuntimeStatus.textContent = "Advanced runtime ready";
    advancedPythonOutput.textContent = `Advanced flow returned ${result}`;
  } catch (error) {
    advancedRuntimeStatus.textContent = "Advanced runtime unavailable";
    advancedPythonOutput.textContent = `Python error:\n${error.message}`;
  }
}

runAdvancedPythonButton.addEventListener("click", () => {
  runAdvancedPythonCode();
});

if (typeof loadPyodide !== "function") {
  runtimeStatus.textContent = "Interpreter unavailable until Pyodide loads";
  advancedRuntimeStatus.textContent = "Interpreter unavailable until Pyodide loads";
}

async function loadDatasetPreview() {
  try {
    const response = await fetch("/api/dataset-preview?limit=10");
    const payload = await response.json();

    datasetHead.innerHTML = `<tr>${payload.columns.map((column) => `<th>${column}</th>`).join("")}</tr>`;
    datasetBody.innerHTML = payload.rows
      .map(
        (row) => `<tr>${payload.columns.map((column) => `<td>${row[column]}</td>`).join("")}</tr>`,
      )
      .join("");
    datasetLoaded = true;
  } catch (error) {
    datasetBody.innerHTML = `<tr><td colspan="7">Could not load LG.csv preview: ${error.message}</td></tr>`;
  }
}

async function generateNfaAndRender(regex) {
  renderNfa(regex);
  nfaStatus.textContent = "Generating nfa.csv and nfa_nodes.csv...";

  try {
    const response = await fetch("/api/generate-nfa", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ regex }),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "NFA generation failed");
    }
    nfaStatus.textContent = `Generated ${payload.edges.length} transition rows`;
  } catch (error) {
    nfaStatus.textContent = `NFA generation failed: ${error.message}`;
  }
}

async function runRecap() {
  setRunUiState(true, "Executing ReCAP...");
  recapResult.textContent = "Running ReCAP...";

  const payload = {
    mode: document.querySelector(".mode-toggle.active")?.dataset.mode || "naive",
    regex: regexInput.value,
    startNode: startNodeInput.value,
    lowerBound: lowerBound.value,
    upperBound: upperBound.value,
    naiveCode: pythonEditor.value,
    advancedCode: {
      init_d: advancedInitD.value,
      update_d: advancedUpdateD.value,
      is_viable_d: advancedIsViableD.value,
      finalize_d: advancedFinalizeD.value,
      is_viable_d_final: advancedIsViableDFinal.value,
    },
  };

  try {
    const response = await fetch("/api/run-recap", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const result = await response.json();
    if (!response.ok || !result.ok) {
      throw new Error(result.error || result.message || "Run failed");
    }
    recapResult.textContent = result.message;
  } catch (error) {
    setRunUiState(false);
    recapResult.textContent = `Run failed: ${error.message}`;
    return;
  }
  setRunUiState(false);
}

function setRunUiState(running, message = "Executing ReCAP...") {
  runStatus.classList.toggle("hidden", !running);
  runStatusText.textContent = message;
  runRecapButton.disabled = running;
}

window.__runRecap = runRecap;

generateNfaAndRender(regexInput.value);
