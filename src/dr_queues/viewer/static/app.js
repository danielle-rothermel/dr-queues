const DEFAULT_REFRESH_RATE_MS = 2000;
const REFRESH_RATE_STORAGE_KEY = "dr-queues-viewer.refreshRateMs";
const REFRESH_RATE_OPTIONS_MS = new Set([0, 1000, 2000, 5000, 10000]);

const state = {
  runId: null,
  pollId: null,
  refreshRateMs: DEFAULT_REFRESH_RATE_MS,
  refreshInFlight: false,
  visible: true,
};

const nodes = {
  runInput: document.querySelector("#run-id-input"),
  loadRunButton: document.querySelector("#load-run-button"),
  refreshRateSelect: document.querySelector("#refresh-rate-select"),
  refreshRunsButton: document.querySelector("#refresh-runs-button"),
  runList: document.querySelector("#run-list"),
  message: document.querySelector("#message"),
  overview: document.querySelector("#overview"),
  stageRail: document.querySelector("#stage-rail"),
  blockedCount: document.querySelector("#blocked-count"),
  blockedJobs: document.querySelector("#blocked-jobs"),
  workerCount: document.querySelector("#worker-count"),
  workers: document.querySelector("#workers"),
  holdCount: document.querySelector("#hold-count"),
  holds: document.querySelector("#holds"),
  attemptCount: document.querySelector("#attempt-count"),
  attempts: document.querySelector("#attempts"),
  eventCount: document.querySelector("#event-count"),
  events: document.querySelector("#events"),
};

async function api(path) {
  const response = await fetch(path);
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}));
    throw new Error(detail.detail || `${response.status} ${response.statusText}`);
  }
  return response.json();
}

async function init() {
  state.refreshRateMs = readStoredRefreshRate();
  nodes.refreshRateSelect.value = String(state.refreshRateMs);
  nodes.loadRunButton.addEventListener("click", () => {
    loadRun(nodes.runInput.value.trim());
  });
  nodes.runInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      loadRun(nodes.runInput.value.trim());
    }
  });
  nodes.refreshRateSelect.addEventListener("change", () => {
    setRefreshRate(Number.parseInt(nodes.refreshRateSelect.value, 10));
  });
  nodes.refreshRunsButton.addEventListener("click", refreshRuns);
  document.addEventListener("visibilitychange", () => {
    state.visible = document.visibilityState === "visible";
    if (state.visible && state.runId) {
      refreshSnapshot();
      startPolling();
      return;
    }
    stopPolling();
  });

  await refreshRuns();
  const config = await api("/api/config");
  if (config.run_id) {
    loadRun(config.run_id);
  }
}

async function refreshRuns() {
  try {
    const runs = await api("/api/runs");
    renderRunList(runs);
    if (!state.runId && runs.length > 0) {
      nodes.runInput.value = runs[0].run_id;
    }
    clearMessage();
  } catch (error) {
    showMessage(error.message);
    renderRunList([]);
  }
}

function loadRun(runId) {
  if (!runId) {
    showMessage("Enter a run id to load.");
    return;
  }
  state.runId = runId;
  nodes.runInput.value = runId;
  refreshSnapshot();
  startPolling();
}

function startPolling() {
  stopPolling();
  if (!state.visible || !state.runId || state.refreshRateMs === 0) {
    return;
  }
  state.pollId = window.setInterval(refreshSnapshot, state.refreshRateMs);
}

function stopPolling() {
  if (state.pollId !== null) {
    window.clearInterval(state.pollId);
    state.pollId = null;
  }
}

async function refreshSnapshot() {
  if (!state.runId || state.refreshInFlight) {
    return;
  }
  state.refreshInFlight = true;
  try {
    const snapshot = await api(`/api/runs/${encodeURIComponent(state.runId)}/snapshot`);
    renderSnapshot(snapshot);
    clearMessage();
  } catch (error) {
    showMessage(error.message);
  } finally {
    state.refreshInFlight = false;
  }
}

function setRefreshRate(refreshRateMs) {
  if (!REFRESH_RATE_OPTIONS_MS.has(refreshRateMs)) {
    refreshRateMs = DEFAULT_REFRESH_RATE_MS;
  }
  state.refreshRateMs = refreshRateMs;
  nodes.refreshRateSelect.value = String(refreshRateMs);
  window.localStorage.setItem(REFRESH_RATE_STORAGE_KEY, String(refreshRateMs));
  if (refreshRateMs > 0 && state.runId && state.visible) {
    refreshSnapshot();
  }
  startPolling();
}

function readStoredRefreshRate() {
  const savedValue = window.localStorage.getItem(REFRESH_RATE_STORAGE_KEY);
  const refreshRateMs = Number.parseInt(savedValue || "", 10);
  if (REFRESH_RATE_OPTIONS_MS.has(refreshRateMs)) {
    return refreshRateMs;
  }
  return DEFAULT_REFRESH_RATE_MS;
}

function renderRunList(runs) {
  nodes.runList.replaceChildren();
  if (runs.length === 0) {
    nodes.runList.append(empty("No runs found."));
    return;
  }
  for (const run of runs) {
    const button = document.createElement("button");
    button.className = "run-card";
    if (run.run_id === state.runId) {
      button.classList.add("is-active");
    }
    button.type = "button";
    button.addEventListener("click", () => loadRun(run.run_id));
    const id = document.createElement("strong");
    id.textContent = run.run_id;
    const meta = document.createElement("span");
    meta.textContent = `${run.pipeline_id} · ${run.health} · ${run.terminal_jobs}/${run.expected_jobs}`;
    button.append(id, meta);
    nodes.runList.append(button);
  }
}

function renderSnapshot(snapshot) {
  renderOverview(snapshot.summary);
  renderStageRail(snapshot.status.stages);
  renderBlockedJobs(snapshot.blocked_jobs);
  renderWorkers(snapshot.status.workers);
  renderHolds(snapshot.active_holds);
  renderAttempts(snapshot.recent_attempts);
  renderEvents(snapshot.recent_events);
}

function renderOverview(summary) {
  const metrics = [
    ["Health", summary.health, `chip health-${summary.health}`],
    ["Terminal", `${summary.terminal_jobs}/${summary.expected_jobs}`, ""],
    ["Workers", `${summary.active_workers} active`, ""],
    ["Stale", String(summary.stale_workers), ""],
    ["Partitions", String(summary.partitions.length), ""],
  ];
  nodes.overview.replaceChildren(
    progressStrip(
      "Overall progress",
      summary.terminal_jobs,
      summary.expected_jobs,
      "overall-progress",
    ),
    ...metrics.map(([label, value, className]) => metric(label, value, className)),
  );
}

function renderStageRail(stages) {
  const title = document.createElement("div");
  title.className = "panel-header";
  const h2 = document.createElement("h2");
  h2.textContent = "Stage rail";
  title.append(h2);

  const track = document.createElement("div");
  track.className = "rail-track";
  for (const stage of stages) {
    track.append(stageCard(stage));
  }
  nodes.stageRail.replaceChildren(title, track);
}

function stageCard(stage) {
  const card = document.createElement("article");
  card.className = "stage-card";
  const title = document.createElement("h3");
  title.textContent = stage.stage;
  card.append(title);
  card.append(
    progressStrip(
      "Finished",
      stage.completed_jobs,
      stage.expected_jobs,
      "stage-progress",
    ),
  );
  card.append(
    row("completed", `${stage.completed_jobs}/${stage.expected_jobs}`, "count-row"),
    row("in flight", String(stage.in_flight_jobs), "count-row"),
    row("input ready", String(stage.input_queue.ready_messages), "queue-line"),
    row("output ready", String(stage.output_queue.ready_messages), "queue-line"),
  );
  const chips = document.createElement("div");
  chips.className = "state-chips";
  for (const [status, count] of Object.entries(stage.job_state_counts || {})) {
    if (count > 0) {
      chips.append(chip(`${status}: ${count}`, `status-${status}`));
    }
  }
  card.append(chips);
  return card;
}

function renderBlockedJobs(jobs) {
  nodes.blockedCount.textContent = String(jobs.length);
  renderTable(nodes.blockedJobs, jobs, [
    ["job", (job) => job.job_id],
    ["stage", (job) => job.stage],
    ["status", (job) => job.status],
    ["partition", (job) => job.partition_key],
    ["attempts", (job) => String(job.attempt_count)],
    ["detail", (job) => job.failure_detail || ""],
  ]);
}

function renderWorkers(workers) {
  nodes.workerCount.textContent = String(workers.length);
  renderTable(nodes.workers, workers, [
    ["stage", (worker) => worker.stage],
    ["status", (worker) => worker.status],
    ["pid", (worker) => String(worker.pid)],
    ["workers", (worker) => String(worker.workers)],
    ["selectors", selectorsForWorker],
  ]);
}

function renderHolds(holds) {
  nodes.holdCount.textContent = String(holds.length);
  renderTable(nodes.holds, holds, [
    ["selectors", (hold) => selectorsText(hold.selectors)],
    ["until", (hold) => hold.blocked_until || "manual"],
    ["reason", (hold) => hold.reason || ""],
  ]);
}

function renderAttempts(attempts) {
  nodes.attemptCount.textContent = String(attempts.length);
  renderTable(nodes.attempts, attempts, [
    ["job", (attempt) => attempt.job_id],
    ["stage", (attempt) => attempt.stage],
    ["attempt", (attempt) => String(attempt.attempt_number)],
    ["action", (attempt) => attempt.action],
    ["error", (attempt) => `${attempt.error_type}: ${attempt.error_message}`],
  ]);
}

function renderEvents(events) {
  nodes.eventCount.textContent = String(events.length);
  nodes.events.replaceChildren();
  if (events.length === 0) {
    nodes.events.append(empty("No events recorded."));
    return;
  }
  for (const event of events) {
    const rowNode = document.createElement("div");
    rowNode.className = "event-row";
    const time = document.createElement("span");
    time.className = "mono";
    time.textContent = shortTime(event.timestamp);
    const body = document.createElement("div");
    body.textContent = `${event.event} · ${event.stage} · ${event.job_id}`;
    rowNode.append(time, body);
    nodes.events.append(rowNode);
  }
}

function renderTable(container, rows, columns) {
  container.replaceChildren();
  if (rows.length === 0) {
    container.append(empty("Nothing to show."));
    return;
  }
  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const headerRow = document.createElement("tr");
  for (const [label] of columns) {
    const th = document.createElement("th");
    th.textContent = label;
    headerRow.append(th);
  }
  thead.append(headerRow);
  const tbody = document.createElement("tbody");
  for (const item of rows) {
    const tr = document.createElement("tr");
    for (const [, read] of columns) {
      const td = document.createElement("td");
      td.textContent = read(item);
      tr.append(td);
    }
    tbody.append(tr);
  }
  table.append(thead, tbody);
  container.append(table);
}

function metric(label, value, className) {
  const node = document.createElement("div");
  node.className = "metric";
  const labelNode = document.createElement("span");
  labelNode.textContent = label;
  const valueNode = document.createElement("strong");
  valueNode.textContent = value;
  if (className) {
    valueNode.className = className;
  }
  node.append(labelNode, valueNode);
  return node;
}

function progressStrip(label, completed, expected, className) {
  const percent = progressPercent(completed, expected);
  const node = document.createElement("div");
  node.className = `progress-strip ${className}`;

  const header = document.createElement("div");
  header.className = "progress-header";
  const labelNode = document.createElement("span");
  labelNode.textContent = label;
  const percentNode = document.createElement("strong");
  percentNode.textContent = `${percent}%`;
  header.append(labelNode, percentNode);

  const track = document.createElement("div");
  track.className = "progress-track";
  track.setAttribute("role", "progressbar");
  track.setAttribute("aria-label", label);
  track.setAttribute("aria-valuemin", "0");
  track.setAttribute("aria-valuemax", "100");
  track.setAttribute("aria-valuenow", String(percent));

  const fill = document.createElement("div");
  fill.className = "progress-fill";
  fill.style.width = `${percent}%`;
  track.append(fill);

  node.append(header, track);
  return node;
}

function progressPercent(completed, expected) {
  if (expected <= 0) {
    return 0;
  }
  const percent = Math.round((completed / expected) * 100);
  return Math.max(0, Math.min(100, percent));
}

function row(label, value, className) {
  const node = document.createElement("div");
  node.className = className;
  const labelNode = document.createElement("span");
  labelNode.textContent = label;
  const valueNode = document.createElement("strong");
  valueNode.textContent = value;
  node.append(labelNode, valueNode);
  return node;
}

function chip(text, className) {
  const node = document.createElement("span");
  node.className = `chip ${className}`;
  node.textContent = text;
  return node;
}

function empty(text) {
  const node = document.createElement("div");
  node.className = "empty";
  node.textContent = text;
  return node;
}

function selectorsForWorker(worker) {
  const include = selectorsText(worker.include_selectors || []);
  const exclude = selectorsText(worker.exclude_selectors || []);
  if (include && exclude) {
    return `include ${include}; exclude ${exclude}`;
  }
  if (include) {
    return `include ${include}`;
  }
  if (exclude) {
    return `exclude ${exclude}`;
  }
  return "";
}

function selectorsText(selectors) {
  return selectors.map((selector) => `${selector.key}=${selector.value}`).join(", ");
}

function shortTime(value) {
  if (!value) {
    return "";
  }
  return value.replace("T", " ").replace(/\.\d+/, "").replace(/\+00:00$/, "Z");
}

function showMessage(text) {
  nodes.message.hidden = false;
  nodes.message.textContent = text;
}

function clearMessage() {
  nodes.message.hidden = true;
  nodes.message.textContent = "";
}

init();
