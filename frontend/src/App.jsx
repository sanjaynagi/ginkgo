import React, { useEffect, useMemo, useState } from "react";

function apiFetch(path, options) {
  return fetch(path, options).then(async (response) => {
    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.error || `Request failed: ${response.status}`);
    }
    return data;
  });
}

function usePathname() {
  const [pathname, setPathname] = useState(window.location.pathname);

  useEffect(() => {
    const onPopstate = () => setPathname(window.location.pathname);
    window.addEventListener("popstate", onPopstate);
    return () => window.removeEventListener("popstate", onPopstate);
  }, []);

  function navigate(nextPath) {
    if (nextPath === window.location.pathname) return;
    window.history.pushState({}, "", nextPath);
    setPathname(nextPath);
  }

  return { pathname, navigate };
}

function formatDuration(seconds) {
  if (seconds == null) return "—";
  if (seconds < 1) return `${seconds.toFixed(2)}s`;
  if (seconds < 10) return `${seconds.toFixed(1)}s`;
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  return `${(seconds / 3600).toFixed(1)}h`;
}

function formatTimestamp(value) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

function relativeWorkflowPath(workflowPath, projectRoot) {
  if (!workflowPath) return "";
  const value = String(workflowPath);
  if (!projectRoot) return value;
  const root = String(projectRoot).replace(/\/+$/, "");
  return value.startsWith(`${root}/`) ? value.slice(root.length + 1) : value;
}

function shortRunHash(runId) {
  if (!runId) return "unknown";
  const parts = String(runId).split("_");
  return parts[parts.length - 1] || runId;
}

function statusTone(status) {
  return {
    succeeded: "success",
    cached: "cached",
    running: "running",
    failed: "failed",
    pending: "pending",
  }[status] || "pending";
}

function MetricCard({ label, value, subvalue, accent = "teal" }) {
  return (
    <div className={`metric-card accent-${accent}`}>
      <div className="metric-label">{label}</div>
      <div className="metric-value">{value}</div>
      {subvalue ? <div className="metric-subvalue">{subvalue}</div> : null}
    </div>
  );
}

function Badge({ status }) {
  return <span className={`badge tone-${statusTone(status)}`}>{status}</span>;
}

function truncateLabel(value, max = 22) {
  if (!value) return "unknown";
  return value.length <= max ? value : `${value.slice(0, max - 1)}…`;
}

function taskBaseName(value) {
  if (!value) return "unknown";
  const parts = String(value).split(".");
  return parts[parts.length - 1] || value;
}

function RunList({ runs, latestRunId, onOpenRun, onOpenCache, onOpenRunDialog, live }) {
  return (
    <section className="panel panel-table">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Run History</p>
          <h2>Recent runs</h2>
          <div className="muted-copy">Ordered by execution time</div>
        </div>
        <div className="header-actions">
          <span className={`live-dot ${live ? "active" : ""}`}>{live ? "Live" : "Idle"}</span>
          <button className="primary-button" onClick={onOpenRunDialog}>Run workflow</button>
          <button className="ghost-button" onClick={onOpenCache}>Cache</button>
          {latestRunId ? (
            <button className="ghost-button" onClick={() => onOpenRun(latestRunId)}>
              Open latest
            </button>
          ) : null}
        </div>
      </div>
      {runs.length === 0 ? (
        <div className="empty-state">
          <h3>No runs yet</h3>
          <p>Run a workflow with <code>ginkgo run workflow.py</code> and it will appear here.</p>
        </div>
      ) : (
        <div className="table-shell">
          <table className="modern-table">
            <thead>
              <tr>
                <th>Run</th>
                <th>Status</th>
                <th>Workflow</th>
                <th>Tasks</th>
                <th>Cached</th>
                <th>Failed</th>
                <th>Duration</th>
                <th>Started</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((run) => (
                <tr key={run.run_id} onClick={() => onOpenRun(run.run_id)}>
                  <td className="strong">
                    <div>{shortRunHash(run.run_id)}</div>
                    <div className="micro-copy">Executed {formatTimestamp(run.started_at)}</div>
                  </td>
                  <td><Badge status={run.status} /></td>
                  <td>{run.workflow}</td>
                  <td>{run.task_count}</td>
                  <td>{run.cached_count}</td>
                  <td>{run.failed_count}</td>
                  <td>{formatDuration(run.duration_seconds)}</td>
                  <td>{formatTimestamp(run.started_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function TaskDrawer({ taskDetail, taskLog, onClose }) {
  const [tab, setTab] = useState("overview");

  useEffect(() => {
    setTab("overview");
  }, [taskDetail?.task_key]);

  if (!taskDetail) return null;
  const task = taskDetail.task;
  const title = taskBaseName(task.task || taskDetail.task_key);
  return (
    <aside className="task-drawer">
      <div className="task-drawer-header">
        <div>
          <p className="eyebrow">Task Detail</p>
          <h3>{title}</h3>
          <div className="task-drawer-subtitle">{task.task || taskDetail.task_key}</div>
        </div>
        <button className="ghost-button" onClick={onClose}>Close</button>
      </div>
      <div className="task-stat-row">
        <div className="task-stat-chip">
          <span>Status</span>
          <Badge status={task.status} />
        </div>
        <div className="task-stat-chip">
          <span>Env</span>
          <strong>{task.env || "local"}</strong>
        </div>
        <div className="task-stat-chip">
          <span>Cached</span>
          <strong>{String(task.cached)}</strong>
        </div>
        <div className="task-stat-chip">
          <span>Exit</span>
          <strong>{task.exit_code ?? "—"}</strong>
        </div>
      </div>

      <div className="task-tabs" role="tablist" aria-label="Task detail sections">
        {[
          ["overview", "Overview"],
          ["io", "Inputs / Output"],
          ["hashes", "Hashes"],
          ["logs", "Logs"],
        ].map(([key, label]) => (
          <button
            key={key}
            className={`task-tab ${tab === key ? "active" : ""}`}
            onClick={() => setTab(key)}
            role="tab"
            aria-selected={tab === key}
          >
            {label}
          </button>
        ))}
      </div>

      {tab === "overview" ? (
        <div className="drawer-stack">
          <section className="drawer-section drawer-card">
            <h4>Summary</h4>
            <dl className="task-meta-list">
              <div><dt>Task key</dt><dd>{taskDetail.task_key}</dd></div>
              <div><dt>Started</dt><dd>{task.started_at ? formatTimestamp(task.started_at) : "—"}</dd></div>
              <div><dt>Finished</dt><dd>{task.finished_at ? formatTimestamp(task.finished_at) : "—"}</dd></div>
              <div><dt>Log file</dt><dd>{task.log || "—"}</dd></div>
              <div><dt>Cache key</dt><dd><code>{task.cache_key || "—"}</code></dd></div>
              <div><dt>Error</dt><dd>{task.error || "—"}</dd></div>
            </dl>
          </section>
        </div>
      ) : null}

      {tab === "io" ? (
        <div className="drawer-stack split-stack">
          <section className="drawer-section drawer-card">
            <h4>Inputs</h4>
            <pre>{JSON.stringify(task.inputs ?? {}, null, 2)}</pre>
          </section>
          <section className="drawer-section drawer-card">
            <h4>Output</h4>
            <pre>{JSON.stringify(task.output ?? {}, null, 2)}</pre>
          </section>
        </div>
      ) : null}

      {tab === "hashes" ? (
        <div className="drawer-stack">
          <section className="drawer-section drawer-card">
            <h4>Input Hashes</h4>
            <pre>{JSON.stringify(task.input_hashes ?? {}, null, 2)}</pre>
          </section>
        </div>
      ) : null}

      {tab === "logs" ? (
        <div className="drawer-stack">
          <section className="drawer-section drawer-card">
            <h4>Log Tail</h4>
            <pre>{(taskDetail.log_tail || []).join("\n") || "No log tail available."}</pre>
          </section>
          <section className="drawer-section drawer-card">
            <h4>Full Log</h4>
            <pre className="log-block">{taskLog || "No log content available."}</pre>
          </section>
        </div>
      ) : null}
    </aside>
  );
}

function DagView({ tasks, onOpenTask, activeTaskKey }) {
  const [zoom, setZoom] = useState(1);
  const graph = useMemo(() => {
    const layers = new Map();
    let maxLayer = 0;
    for (const task of tasks) {
      const deps = task.dependency_ids || [];
      const layer = deps.length
        ? Math.max(...deps.map((id) => layers.get(id) ?? 0)) + 1
        : 0;
      layers.set(task.node_id, layer);
      maxLayer = Math.max(maxLayer, layer);
    }

    const columns = Array.from({ length: maxLayer + 1 }, (_, layer) => layer);
    const grouped = new Map(columns.map((layer) => [layer, []]));
    for (const task of tasks) {
      const layer = layers.get(task.node_id) ?? 0;
      grouped.get(layer).push(task);
    }
    for (const columnTasks of grouped.values()) {
      columnTasks.sort((left, right) => (left.node_id ?? 0) - (right.node_id ?? 0));
    }

    const nodes = [];
    const nodeLookup = new Map();
    columns.forEach((layer) => {
      const columnTasks = grouped.get(layer) || [];
      columnTasks.forEach((task, index) => {
        const node = {
          ...task,
          layer,
          x: 54 + layer * 230,
          y: 74 + index * 114,
        };
        nodes.push(node);
        nodeLookup.set(task.node_id, node);
      });
    });

    const links = [];
    for (const node of nodes) {
      for (const depId of node.dependency_ids || []) {
        const sourceNode = nodeLookup.get(depId);
        if (sourceNode) {
          links.push({
            key: `${depId}-${node.node_id}`,
            x1: sourceNode.x + 182,
            y1: sourceNode.y + 34,
            x2: node.x,
            y2: node.y + 34,
            dynamic: false,
          });
        }
      }
      for (const depId of node.dynamic_dependency_ids || []) {
        const sourceNode = nodeLookup.get(depId);
        if (sourceNode) {
          links.push({
            key: `dyn-${node.node_id}-${depId}`,
            x1: node.x + 182,
            y1: node.y + 34,
            x2: sourceNode.x,
            y2: sourceNode.y + 34,
            dynamic: true,
          });
        }
      }
    }
    const laneHeight = Math.max(...columns.map((layer) => 128 + (grouped.get(layer)?.length || 0) * 114), 220);
    const width = Math.max(720, columns.length * 230 + 64);
    const height = Math.max(280, laneHeight);
    return { nodes, links, width, height, columns };
  }, [tasks]);

  if (!tasks.length) {
    return <div className="empty-state compact-empty"><p>No task graph data available yet.</p></div>;
  }

  return (
    <section className="panel dag-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Task Graph</p>
          <h3>Execution stages</h3>
        </div>
        <div className="dag-toolbar">
          <div className="dag-legend">
            <span><i className="legend-dot tone-success" /> Succeeded</span>
            <span><i className="legend-dot tone-running" /> Running</span>
            <span><i className="legend-dot tone-failed" /> Failed</span>
            <span><i className="legend-line" /> Dependency</span>
            <span><i className="legend-line dynamic" /> Dynamic</span>
          </div>
          <div className="zoom-controls">
            <button className="ghost-button" onClick={() => setZoom((value) => Math.max(0.7, value - 0.1))}>−</button>
            <span>{Math.round(zoom * 100)}%</span>
            <button className="ghost-button" onClick={() => setZoom((value) => Math.min(1.5, value + 0.1))}>+</button>
          </div>
        </div>
      </div>
      <div className="dag-shell">
        <div className="dag-scroll">
          <svg
            className="dag-canvas"
            viewBox={`0 0 ${graph.width} ${graph.height}`}
            style={{ transform: `scale(${zoom})`, transformOrigin: "top left" }}
          >
            {graph.columns.map((layer) => (
              <g key={`lane-${layer}`}>
                <rect className="dag-lane" x={26 + layer * 230} y={26} width={204} height={graph.height - 40} rx={24} ry={24} />
                <text x={42 + layer * 230} y={50} className="dag-lane-label">Stage {layer + 1}</text>
              </g>
            ))}
            {graph.links.map((link) => (
              <path
                key={link.key}
                d={`M ${link.x1} ${link.y1} C ${link.x1 + 32} ${link.y1}, ${link.x2 - 32} ${link.y2}, ${link.x2} ${link.y2}`}
                className={`dag-link ${link.dynamic ? "dynamic" : ""}`}
              />
            ))}
            {graph.nodes.map((node) => (
              <g
                key={node.task_key}
                transform={`translate(${node.x}, ${node.y})`}
                onClick={() => onOpenTask(node.task_key)}
                className={`dag-node tone-${statusTone(node.status)} ${activeTaskKey === node.task_key ? "active" : ""}`}
              >
                <rect rx="22" ry="22" width="182" height="68" />
                <text x="16" y="24">{truncateLabel(node.task_name, 24)}</text>
                <text x="16" y="43" className="dag-node-sub">{node.task_key}</text>
                <text x="16" y="58" className="dag-node-sub status">{node.status}</text>
              </g>
            ))}
          </svg>
        </div>
      </div>
    </section>
  );
}

function CacheBrowser({ entries, onDelete, onClearAll, clearing }) {
  return (
    <section className="panel panel-table">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Cache Browser</p>
          <h2>Content-addressed entries</h2>
        </div>
        <div className="header-actions">
          <button
            className="ghost-button danger-button"
            onClick={onClearAll}
            disabled={entries.length === 0 || clearing}
          >
            {clearing ? "Clearing..." : "Clear cache"}
          </button>
        </div>
      </div>
      {entries.length === 0 ? (
        <div className="empty-state">
          <h3>No cache entries</h3>
          <p>Cache results will appear here after you run workflows.</p>
        </div>
      ) : (
        <div className="table-shell">
          <table className="modern-table">
            <thead>
              <tr>
                <th>Task</th>
                <th>Cache Key</th>
                <th>Size</th>
                <th>Age</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {entries.map((entry) => (
                <tr key={entry.cache_key}>
                  <td className="strong">{entry.task}</td>
                  <td><code>{entry.cache_key}</code></td>
                  <td>{entry.size}</td>
                  <td>{entry.age}</td>
                  <td className="action-cell">
                    <button className="ghost-button danger-button" onClick={() => onDelete(entry.cache_key)}>
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function RunWorkflowModal({ open, workflows, initialWorkflow, busy, onClose, onSubmit }) {
  const [workflow, setWorkflow] = useState(initialWorkflow || "");
  const [configLines, setConfigLines] = useState("");
  const [jobs, setJobs] = useState("");
  const [cores, setCores] = useState("");

  useEffect(() => {
    if (!open) return;
    setWorkflow(initialWorkflow || workflows[0] || "");
    setConfigLines("");
    setJobs("");
    setCores("");
  }, [open, initialWorkflow, workflows]);

  if (!open) return null;

  function submit(event) {
    event.preventDefault();
    onSubmit({
      workflow,
      config_paths: configLines
        .split("\n")
        .map((line) => line.trim())
        .filter(Boolean),
      jobs: jobs ? Number(jobs) : undefined,
      cores: cores ? Number(cores) : undefined,
    });
  }

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal-card" onClick={(event) => event.stopPropagation()}>
        <div className="panel-header modal-header">
          <div>
            <p className="eyebrow">Run Workflow</p>
            <h3>Launch a new Ginkgo run</h3>
            <p className="muted-copy">Pick a workflow file and optional config overlays.</p>
          </div>
          <button className="ghost-button" onClick={onClose}>Close</button>
        </div>
        <form className="run-form" onSubmit={submit}>
          <label className="form-field">
            <span>Workflow</span>
            <input
              list="workflow-options"
              value={workflow}
              onChange={(event) => setWorkflow(event.target.value)}
              placeholder="workflow.py"
              required
            />
            <datalist id="workflow-options">
              {workflows.map((item) => <option key={item} value={item} />)}
            </datalist>
          </label>

          <label className="form-field">
            <span>Config paths</span>
            <textarea
              value={configLines}
              onChange={(event) => setConfigLines(event.target.value)}
              placeholder={"ginkgo.toml\nconfigs/experiment.toml"}
              rows={4}
            />
            <small>One file per line.</small>
          </label>

          <div className="form-grid">
            <label className="form-field">
              <span>Jobs</span>
              <input
                type="number"
                min="1"
                value={jobs}
                onChange={(event) => setJobs(event.target.value)}
                placeholder="Auto"
              />
            </label>
            <label className="form-field">
              <span>Cores</span>
              <input
                type="number"
                min="1"
                value={cores}
                onChange={(event) => setCores(event.target.value)}
                placeholder="Auto"
              />
            </label>
          </div>

          <div className="modal-actions">
            <button type="button" className="ghost-button" onClick={onClose}>Cancel</button>
            <button type="submit" className="primary-button" disabled={busy}>
              {busy ? "Starting..." : "Run workflow"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

function RunDetail({ run, onOpenTask, activeTaskKey }) {
  const tasks = run.tasks || [];
  const summary = useMemo(() => {
    const counts = { succeeded: 0, cached: 0, failed: 0, running: 0, pending: 0 };
    for (const task of tasks) {
      counts[task.status] = (counts[task.status] || 0) + 1;
    }
    return counts;
  }, [tasks]);

  return (
    <section className="run-detail">
      <div className="detail-hero">
        <div className="hero-copy">
          <p className="eyebrow">Run Detail</p>
          <h2>{run.run_id}</h2>
          <p className="muted-copy">
            {run.manifest.workflow} · started {formatTimestamp(run.manifest.started_at)}
          </p>
          <div className="hero-pills">
            <span className="hero-pill">Jobs {run.manifest.jobs ?? "auto"}</span>
            <span className="hero-pill">Cores {run.manifest.cores ?? "auto"}</span>
            <span className="hero-pill status-pill">
              <Badge status={run.manifest.status || "unknown"} />
            </span>
          </div>
        </div>
        <div className="hero-visual hero-summary">
          <MetricCard
            label="Workflow"
            value={run.manifest.workflow?.split("/").pop() || "unknown"}
            subvalue={`Started ${formatTimestamp(run.manifest.started_at)}`}
            accent="gold"
          />
        </div>
      </div>

      <div className="metrics-grid">
        <MetricCard label="Tasks" value={tasks.length} subvalue="Total recorded tasks" />
        <MetricCard label="Succeeded" value={summary.succeeded || 0} subvalue="Executed successfully" accent="emerald" />
        <MetricCard label="Cached" value={summary.cached || 0} subvalue="Served from cache" accent="blue" />
        <MetricCard label="Failed" value={summary.failed || 0} subvalue="Need attention" accent="rose" />
      </div>

      <DagView tasks={tasks} onOpenTask={onOpenTask} activeTaskKey={activeTaskKey} />

      <div className="content-grid">
        <section className="panel panel-table">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Tasks</p>
              <h3>Execution ledger</h3>
            </div>
            <div className="muted-copy">Click a task to inspect inputs and logs</div>
          </div>
          <div className="table-shell">
            <table className="modern-table">
              <thead>
                <tr>
                  <th>Task</th>
                  <th>Status</th>
                  <th>Env</th>
                  <th>Cached</th>
                  <th>Exit</th>
                </tr>
              </thead>
              <tbody>
                {tasks.map((task) => (
                  <tr
                    key={task.task_key}
                    className={task.task_key === activeTaskKey ? "active-row" : ""}
                    onClick={() => onOpenTask(task.task_key)}
                  >
                    <td className="strong">
                      <div>{task.task_name}</div>
                      <div className="micro-copy">{task.task_key}</div>
                    </td>
                    <td><Badge status={task.status} /></td>
                    <td>{task.env || "local"}</td>
                    <td>{String(task.cached)}</td>
                    <td>{task.exit_code ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>

        <section className="panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Config</p>
              <h3>Resolved params</h3>
            </div>
          </div>
          <pre className="code-block">{JSON.stringify(run.params || {}, null, 2)}</pre>
        </section>
      </div>
    </section>
  );
}

export function App() {
  const { pathname, navigate } = usePathname();
  const [meta, setMeta] = useState(null);
  const [runs, setRuns] = useState([]);
  const [cacheEntries, setCacheEntries] = useState([]);
  const [workflows, setWorkflows] = useState([]);
  const [runDetail, setRunDetail] = useState(null);
  const [taskDetail, setTaskDetail] = useState(null);
  const [taskLog, setTaskLog] = useState("");
  const [error, setError] = useState(null);
  const [notice, setNotice] = useState(null);
  const [loading, setLoading] = useState(true);
  const [refreshTick, setRefreshTick] = useState(0);
  const [live, setLive] = useState(false);
  const [runDialogOpen, setRunDialogOpen] = useState(false);
  const [launchingRun, setLaunchingRun] = useState(false);
  const [clearingCache, setClearingCache] = useState(false);

  const route = useMemo(() => {
    const parts = pathname.split("/").filter(Boolean);
    if (parts[0] === "cache") {
      return { page: "cache" };
    }
    if (parts[0] === "runs" && parts[1] && parts[2] === "tasks" && parts[3]) {
      return { page: "task", runId: parts[1], taskKey: parts[3] };
    }
    if (parts[0] === "runs" && parts[1]) {
      return { page: "run", runId: parts[1] };
    }
    return { page: "home" };
  }, [pathname]);

  useEffect(() => {
    const source = new EventSource("/api/events");
    source.addEventListener("meta", () => {
      setLive(true);
      setRefreshTick((tick) => tick + 1);
    });
    source.onerror = () => {
      setLive(false);
    };
    return () => source.close();
  }, []);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      setLoading(true);
      setError(null);
      try {
        const [metaData, runsData, cacheData, workflowsData] = await Promise.all([
          apiFetch("/api/meta"),
          apiFetch("/api/runs"),
          apiFetch("/api/cache"),
          apiFetch("/api/workflows"),
        ]);
        if (cancelled) return;
        setMeta(metaData);
        setRuns(runsData.runs || []);
        setCacheEntries(cacheData.entries || []);
        setWorkflows(workflowsData.workflows || []);

        if (route.page === "cache") {
          setRunDetail(null);
          setTaskDetail(null);
          setTaskLog("");
          return;
        }

        const targetRunId =
          route.page === "home" ? metaData.selected_run_id || metaData.latest_run_id : route.runId;
        if (!targetRunId) {
          setRunDetail(null);
          setTaskDetail(null);
          setTaskLog("");
          return;
        }

        const runData = await apiFetch(`/api/runs/${targetRunId}`);
        if (cancelled) return;
        setRunDetail(runData);

        if (route.page === "task" && route.taskKey) {
          const [taskData, logData] = await Promise.all([
            apiFetch(`/api/runs/${targetRunId}/tasks/${route.taskKey}`),
            apiFetch(`/api/runs/${targetRunId}/tasks/${route.taskKey}/log`),
          ]);
          if (cancelled) return;
          setTaskDetail(taskData);
          setTaskLog(logData.content || "");
        } else {
          setTaskDetail(null);
          setTaskLog("");
        }
      } catch (err) {
        if (!cancelled) {
          setError(err.message || String(err));
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }
    load();
    return () => {
      cancelled = true;
    };
  }, [route.page, route.runId, route.taskKey, refreshTick]);

  function openRun(runId) {
    navigate(`/runs/${runId}`);
  }

  function openTask(taskKey) {
    if (!runDetail) return;
    navigate(`/runs/${runDetail.run_id}/tasks/${taskKey}`);
  }

  function closeTask() {
    if (!runDetail) return;
    navigate(`/runs/${runDetail.run_id}`);
  }

  async function deleteCacheEntry(cacheKey) {
    setError(null);
    setNotice(null);
    try {
      await apiFetch(`/api/cache/${cacheKey}`, { method: "DELETE" });
      setNotice(`Deleted cache entry ${cacheKey}.`);
      setRefreshTick((tick) => tick + 1);
    } catch (err) {
      setError(err.message || String(err));
    }
  }

  async function clearCache() {
    if (!window.confirm("Clear all cache entries?")) return;
    setClearingCache(true);
    setError(null);
    setNotice(null);
    try {
      const response = await apiFetch("/api/cache", { method: "DELETE" });
      setNotice(
        response.deleted > 0
          ? `Cleared ${response.deleted} cache entries.`
          : "Cache is already empty."
      );
      setRefreshTick((tick) => tick + 1);
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setClearingCache(false);
    }
  }

  function openRunDialog(prefillWorkflow) {
    if (prefillWorkflow) {
      const clean = prefillWorkflow.replace(/^\.\//, "");
      if (!workflows.includes(clean)) {
        setWorkflows((current) => [clean, ...current]);
      }
    }
    setRunDialogOpen(true);
    setError(null);
  }

  async function launchWorkflow(payload) {
    setLaunchingRun(true);
    setError(null);
    setNotice(null);
    try {
      const response = await apiFetch("/api/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      setNotice(`Started ${response.workflow} (pid ${response.pid}).`);
      setRunDialogOpen(false);
      setRefreshTick((tick) => tick + 1);
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setLaunchingRun(false);
    }
  }

  const initialWorkflow = runDetail?.manifest?.workflow
    ? relativeWorkflowPath(runDetail.manifest.workflow, meta?.project_root)
    : workflows[0] || "";

  return (
    <div className="app-shell">
      <div className="backdrop backdrop-one" />
      <div className="backdrop backdrop-two" />
      <header className="topbar">
        <div>
          <p className="eyebrow">Ginkgo UI</p>
          <h1>Ginkgo UI</h1>
          <p className="topbar-copy">
            Run history, task lineage, cache state, and task logs in one local interface.
          </p>
        </div>
        <div className="topbar-actions">
          <button className="primary-button" onClick={() => openRunDialog(initialWorkflow)}>
            Run workflow
          </button>
          <button className="ghost-button" onClick={() => navigate("/cache")}>View cache</button>
          <div className="topbar-meta">
            <span className="meta-chip">Runs root</span>
            <code>{meta?.runs_root || ".ginkgo/runs"}</code>
          </div>
        </div>
      </header>

      {error ? <div className="error-banner">✖ {error}</div> : null}
      {notice ? <div className="notice-banner">✓ {notice}</div> : null}

      <main className="main-grid">
        <RunList
          runs={runs}
          latestRunId={meta?.latest_run_id}
          onOpenRun={openRun}
          onOpenCache={() => navigate("/cache")}
          onOpenRunDialog={() => openRunDialog(initialWorkflow)}
          live={live}
        />
        {loading ? (
          <section className="panel loading-panel">
            <div className="spinner-ring" />
            <p>Loading Ginkgo UI…</p>
          </section>
        ) : route.page === "cache" ? (
          <CacheBrowser
            entries={cacheEntries}
            onDelete={deleteCacheEntry}
            onClearAll={clearCache}
            clearing={clearingCache}
          />
        ) : runDetail ? (
          <RunDetail
            run={runDetail}
            onOpenTask={openTask}
            activeTaskKey={route.page === "task" ? route.taskKey : null}
          />
        ) : (
          <section className="panel empty-state">
            <h3>No run selected</h3>
            <p>Choose a run from the history to inspect its task graph and provenance.</p>
          </section>
        )}
      </main>

      <TaskDrawer taskDetail={taskDetail} taskLog={taskLog} onClose={closeTask} />
      <RunWorkflowModal
        open={runDialogOpen}
        workflows={workflows}
        initialWorkflow={initialWorkflow}
        busy={launchingRun}
        onClose={() => setRunDialogOpen(false)}
        onSubmit={launchWorkflow}
      />
    </div>
  );
}
