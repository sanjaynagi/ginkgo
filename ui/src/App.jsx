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

function parseRoute(pathname) {
  const parts = pathname.split("/").filter(Boolean);
  if (parts.length === 0) return { page: "home" };
  if (parts[0] === "workspaces" && parts.length === 1) return { page: "workspaces" };
  if (parts[0] === "workspaces" && parts[1] && parts[2] === "cache") {
    return { page: "cache", workspaceId: parts[1] };
  }
  if (
    parts[0] === "workspaces" &&
    parts[1] &&
    parts[2] === "runs" &&
    parts[3] &&
    parts[4] === "tasks" &&
    parts[5]
  ) {
    return {
      page: "task",
      workspaceId: parts[1],
      runId: parts[3],
      taskKey: decodeURIComponent(parts[5]),
    };
  }
  if (parts[0] === "workspaces" && parts[1] && parts[2] === "runs" && parts[3]) {
    return { page: "run", workspaceId: parts[1], runId: parts[3] };
  }
  if (parts[0] === "workspaces" && parts[1] && parts[2] === "runs") {
    return { page: "runs", workspaceId: parts[1] };
  }
  return { page: "home" };
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

function formatDateTimeLarge(value) {
  if (!value) return { date: "—", time: "" };
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return { date: value, time: "" };
  const date = new Intl.DateTimeFormat(undefined, { dateStyle: "medium" }).format(d);
  const time = new Intl.DateTimeFormat(undefined, { timeStyle: "medium" }).format(d);
  return { date, time };
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

function statusIcon(status) {
  return {
    succeeded: "\u2713",
    cached: "\u21BA",
    running: "\u25D0",
    failed: "\u2716",
    pending: "\u25CB",
  }[status] || "\u25CB";
}

function GinkgoLeafIcon({ size = 48 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 48 48" fill="none" className="empty-icon">
      <path
        d="M24 4C16 4 8 12 8 24c0 8 4 14 8 17l1-1c-2-4-3-9-3-14 0-10 5-17 10-20v0c5 3 10 10 10 20 0 5-1 10-3 14l1 1c4-3 8-9 8-17C40 12 32 4 24 4z"
        fill="currentColor"
        opacity="0.9"
      />
      <path d="M24 14v28" stroke="currentColor" strokeWidth="1.5" opacity="0.5" />
      <path d="M24 22c-3 2-6 6-7 12" stroke="currentColor" strokeWidth="1" opacity="0.3" />
      <path d="M24 22c3 2 6 6 7 12" stroke="currentColor" strokeWidth="1" opacity="0.3" />
    </svg>
  );
}

function GinkgoLogo() {
  return (
    <svg width="22" height="22" viewBox="0 0 48 48" fill="none">
      <path
        d="M24 4C16 4 8 12 8 24c0 8 4 14 8 17l1-1c-2-4-3-9-3-14 0-10 5-17 10-20v0c5 3 10 10 10 20 0 5-1 10-3 14l1 1c4-3 8-9 8-17C40 12 32 4 24 4z"
        fill="#b9852b"
      />
    </svg>
  );
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

function parseTimestamp(value) {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function runElapsedSeconds(run, nowValue) {
  if (!run?.manifest?.started_at) return null;
  const started = parseTimestamp(run.manifest.started_at);
  if (!started) return null;
  const finished = parseTimestamp(run.manifest.finished_at);
  const end = finished ?? new Date(nowValue);
  return Math.max(0, (end.getTime() - started.getTime()) / 1000);
}

function summarizeRunTasks(run) {
  const tasks = run?.tasks || [];
  const counts = { succeeded: 0, cached: 0, failed: 0, running: 0, pending: 0 };
  for (const task of tasks) {
    counts[task.status] = (counts[task.status] || 0) + 1;
  }
  return counts;
}

function buildRunSummaryFromDetail(run) {
  const summary = summarizeRunTasks(run);
  return {
    workspace_id: run.workspace_id,
    workspace_label: run.workspace_label,
    project_root: run.project_root,
    run_id: run.run_id,
    workflow: relativeWorkflowPath(run.manifest?.workflow, run.project_root),
    workflow_path: run.manifest?.workflow,
    status: run.manifest?.status || "unknown",
    started_at: run.manifest?.started_at,
    finished_at: run.manifest?.finished_at,
    task_count: (run.tasks || []).length,
    failed_count: summary.failed || 0,
    cached_count: summary.cached || 0,
    succeeded_count: summary.succeeded || 0,
    duration_seconds: runElapsedSeconds(run, Date.now()),
  };
}

function upsertRunSummary(currentRuns, nextRun) {
  const nextSummary = buildRunSummaryFromDetail(nextRun);
  const merged = [...currentRuns];
  const index = merged.findIndex((run) => run.run_id === nextSummary.run_id);
  if (index >= 0) {
    merged[index] = nextSummary;
  } else {
    merged.unshift(nextSummary);
  }
  merged.sort((left, right) => String(right.started_at || "").localeCompare(String(left.started_at || "")));
  return merged;
}

function Sidebar({ route, activeWorkspaceId, navigate }) {
  return (
    <aside className="sidebar">
      <div className="sidebar-brand" onClick={() => navigate("/")}>
        <GinkgoLogo />
        <div>
          <strong>Ginkgo</strong>
        </div>
      </div>

      <div className="sidebar-section">
        <div className="sidebar-label">Navigation</div>
        <button
          className={`sidebar-link ${route.page === "home" || route.page === "runs" || route.page === "run" || route.page === "task" ? "active" : ""}`}
          onClick={() => (activeWorkspaceId ? navigate(`/workspaces/${activeWorkspaceId}/runs`) : navigate("/"))}
        >
          Runs
        </button>
        <button
          className={`sidebar-link ${route.page === "cache" ? "active" : ""}`}
          onClick={() => activeWorkspaceId && navigate(`/workspaces/${activeWorkspaceId}/cache`)}
          disabled={!activeWorkspaceId}
        >
          Cache
        </button>
        <div className="sidebar-divider" />
        <button
          className={`sidebar-link ${route.page === "workspaces" ? "active" : ""}`}
          onClick={() => navigate("/workspaces")}
        >
          Workspaces
        </button>
      </div>
    </aside>
  );
}

function Topbar({
  activeWorkspace,
  workspaces,
  loadingWorkspace,
  onActivateWorkspace,
  onLoadWorkspace,
  onOpenRunDialog,
}) {
  const [workspaceMenuOpen, setWorkspaceMenuOpen] = useState(false);

  useEffect(() => {
    setWorkspaceMenuOpen(false);
  }, [activeWorkspace?.workspace_id]);

  return (
    <header className="topbar">
      <div className="topbar-heading">
        <div className="workspace-switcher">
          <button
            className="workspace-switcher-button"
            onClick={() => setWorkspaceMenuOpen((open) => !open)}
          >
            <span className="topbar-workspace-name">
              {activeWorkspace ? activeWorkspace.label : "No workspace loaded"}
            </span>
            <span className="workspace-switcher-caret">▾</span>
          </button>
          {workspaceMenuOpen ? (
            <div className="workspace-switcher-menu">
              <div className="workspace-switcher-list">
                {workspaces.map((workspace) => (
                  <button
                    key={workspace.workspace_id}
                    className={`workspace-switcher-item ${workspace.is_active ? "active" : ""}`}
                    onClick={() => {
                      onActivateWorkspace(workspace.workspace_id);
                      setWorkspaceMenuOpen(false);
                    }}
                  >
                    <strong>{workspace.label}</strong>
                    <span>{workspace.run_count} runs</span>
                  </button>
                ))}
              </div>
              <button
                className="workspace-switcher-add"
                onClick={() => {
                  onLoadWorkspace();
                  setWorkspaceMenuOpen(false);
                }}
                disabled={loadingWorkspace}
              >
                <span className="workspace-switcher-plus">+</span>
                <span>{loadingWorkspace ? "Loading..." : "Load workspace"}</span>
              </button>
            </div>
          ) : null}
        </div>
      </div>
      <div className="topbar-actions">
        <button
          className="primary-button"
          onClick={onOpenRunDialog}
          disabled={!activeWorkspace}
        >
          Run workflow
        </button>
      </div>
    </header>
  );
}

function Breadcrumbs({ route, activeWorkspace, runDetail, navigate }) {
  return (
    <nav className="breadcrumbs">
      <button className="breadcrumb-link" onClick={() => navigate("/workspaces")}>Workspaces</button>
      {activeWorkspace ? (
        <>
          <span className="breadcrumb-sep">/</span>
          <button
            className="breadcrumb-link"
            onClick={() => navigate(`/workspaces/${activeWorkspace.workspace_id}/runs`)}
          >
            {activeWorkspace.label}
          </button>
        </>
      ) : null}
      {route.page === "cache" ? (
        <>
          <span className="breadcrumb-sep">/</span>
          <span className="breadcrumb-current">Cache</span>
        </>
      ) : null}
      {(route.page === "run" || route.page === "task") && runDetail ? (
        <>
          <span className="breadcrumb-sep">/</span>
          {route.page === "task" ? (
            <button
              className="breadcrumb-link"
              onClick={() => navigate(`/workspaces/${runDetail.workspace_id}/runs/${runDetail.run_id}`)}
            >
              {shortRunHash(runDetail.run_id)}
            </button>
          ) : (
            <span className="breadcrumb-current">{shortRunHash(runDetail.run_id)}</span>
          )}
        </>
      ) : null}
      {route.page === "task" && route.taskKey ? (
        <>
          <span className="breadcrumb-sep">/</span>
          <span className="breadcrumb-current">{route.taskKey}</span>
        </>
      ) : null}
    </nav>
  );
}

function WorkspaceOverview({ workspaces, activeWorkspaceId, onActivateWorkspace }) {
  return (
    <section className="panel">
      <div className="panel-header">
        <div>
          <h2 className="panel-title">Loaded workspaces</h2>
          <p className="panel-subtitle">Switch the active workspace to inspect runs and cache.</p>
        </div>
      </div>
      {workspaces.length === 0 ? (
        <div className="empty-state">
          <GinkgoLeafIcon />
          <h3>No workspaces loaded</h3>
          <p>Use the Load workspace button to add a Ginkgo project folder.</p>
        </div>
      ) : (
        <div className="workspace-grid">
          {workspaces.map((workspace) => (
            <button
              key={workspace.workspace_id}
              className={`workspace-card ${workspace.workspace_id === activeWorkspaceId ? "active" : ""}`}
              onClick={() => onActivateWorkspace(workspace.workspace_id)}
            >
              <div className="workspace-card-top">
                <strong>{workspace.label}</strong>
                {workspace.workspace_id === activeWorkspaceId ? <span>Active</span> : null}
              </div>
              <div className="workspace-card-path">{workspace.project_root}</div>
              <div className="workspace-card-meta">
                <span>{workspace.run_count} runs</span>
                <span>{workspace.workflow_count} workflows</span>
              </div>
            </button>
          ))}
        </div>
      )}
    </section>
  );
}

function RunList({ runs, workspaceLabel, latestRunId, onOpenRun, onOpenRunDialog, live }) {
  return (
    <section className="panel">
      <div className="panel-header">
        <div>
          <h2 className="panel-title">{workspaceLabel ? `${workspaceLabel} runs` : "Recent runs"}</h2>
          <p className="panel-subtitle">Ordered by execution time</p>
        </div>
        <div className="header-actions">
          <span className={`live-dot ${live ? "active" : ""}`}>{live ? "Live" : "Idle"}</span>
          <button className="primary-button" onClick={onOpenRunDialog}>Run workflow</button>
          {latestRunId ? (
            <button className="ghost-button" onClick={() => onOpenRun(latestRunId)}>
              Open latest
            </button>
          ) : null}
        </div>
      </div>
      {runs.length === 0 ? (
        <div className="empty-state">
          <GinkgoLeafIcon />
          <h3>No runs yet</h3>
          <p>Run a workflow from this workspace and it will appear here.</p>
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
                <tr
                  key={run.run_id}
                  className={run.status === "running" ? "live-row" : ""}
                  onClick={() => onOpenRun(run.run_id)}
                >
                  <td className="strong">
                    <div>{shortRunHash(run.run_id)}</div>
                    <div className="micro-copy">{formatTimestamp(run.started_at)}</div>
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
  const stdout = taskLog?.stdout ?? "";
  const stderr = taskLog?.stderr ?? "";
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
        <div className="drawer-stack log-split">
          <section className="drawer-section drawer-card log-pane">
            <h4>stdout</h4>
            <pre className="log-block">{stdout || "No stdout output."}</pre>
          </section>
          <section className="drawer-section drawer-card log-pane">
            <h4>stderr</h4>
            <pre className="log-block stderr-block">{stderr || "No stderr output."}</pre>
          </section>
        </div>
      ) : null}
    </aside>
  );
}

function DagProgress({ tasks }) {
  const total = tasks.length;
  if (total === 0) return null;
  const done = tasks.filter((t) => t.status === "succeeded" || t.status === "cached").length;
  const failed = tasks.filter((t) => t.status === "failed").length;
  const running = tasks.filter((t) => t.status === "running").length;
  const pctDone = (done / total) * 100;
  const pctFailed = (failed / total) * 100;
  const pctRunning = (running / total) * 100;
  return (
    <div className="dag-progress">
      <div className="dag-progress-bar">
        <div className="dag-progress-done" style={{ width: `${pctDone}%` }} />
        <div className="dag-progress-running" style={{ width: `${pctRunning}%` }} />
        <div className="dag-progress-failed" style={{ width: `${pctFailed}%` }} />
      </div>
      <span className="dag-progress-label">{done + failed}/{total} complete</span>
    </div>
  );
}

function DagView({ tasks, onOpenTask, activeTaskKey, isLive }) {
  const [zoom, setZoom] = useState(1);
  const [focusMode, setFocusMode] = useState("all");
  const scrollRef = React.useRef(null);
  const graphRef = React.useRef(null);

  function taskPriority(task) {
    return {
      running: 0,
      failed: 1,
      pending: 2,
      succeeded: 3,
      cached: 4,
    }[task.status] ?? 5;
  }

  const graph = useMemo(() => {
    const layers = new Map();
    for (const task of tasks) {
      layers.set(task.node_id, 0);
    }
    let changed = true;
    while (changed) {
      changed = false;
      for (const task of tasks) {
        const allDeps = [
          ...(task.dependency_ids || []),
          ...(task.dynamic_dependency_ids || []),
        ];
        if (allDeps.length === 0) continue;
        const needed = Math.max(...allDeps.map((id) => (layers.get(id) ?? 0) + 1));
        if (needed > layers.get(task.node_id)) {
          layers.set(task.node_id, needed);
          changed = true;
        }
      }
    }
    let maxLayer = 0;
    for (const layer of layers.values()) {
      maxLayer = Math.max(maxLayer, layer);
    }

    const columns = Array.from({ length: maxLayer + 1 }, (_, layer) => layer);
    const grouped = new Map(columns.map((layer) => [layer, []]));
    for (const task of tasks) {
      const layer = layers.get(task.node_id) ?? 0;
      grouped.get(layer).push(task);
    }
    for (const columnTasks of grouped.values()) {
      columnTasks.sort((left, right) => {
        const priority = taskPriority(left) - taskPriority(right);
        if (priority !== 0) return priority;
        return String(left.task_name || left.task || "").localeCompare(
          String(right.task_name || right.task || ""),
        );
      });
    }

    const nodeW = 148;
    const nodeH = 54;
    const colSpacing = 190;
    const rowSpacing = 74;

    const nodes = [];
    const nodeLookup = new Map();
    columns.forEach((layer) => {
      const columnTasks = grouped.get(layer) || [];
      columnTasks.forEach((task, index) => {
        const node = {
          ...task,
          layer,
          x: 36 + layer * colSpacing,
          y: 36 + index * rowSpacing,
        };
        nodes.push(node);
        nodeLookup.set(task.node_id, node);
      });
    });

    const halfH = nodeH / 2;
    const links = [];
    const doneStatuses = new Set(["succeeded", "cached"]);
    for (const node of nodes) {
      for (const depId of node.dependency_ids || []) {
        const sourceNode = nodeLookup.get(depId);
        if (sourceNode) {
          const srcDone = doneStatuses.has(sourceNode.status);
          const tgtRunning = node.status === "running";
          const bothDone = srcDone && doneStatuses.has(node.status);
          links.push({
            key: `${depId}-${node.node_id}`,
            x1: sourceNode.x + nodeW,
            y1: sourceNode.y + halfH,
            x2: node.x,
            y2: node.y + halfH,
            dynamic: false,
            activeFlow: srcDone && tgtRunning,
            completed: bothDone,
          });
        }
      }
      for (const depId of node.dynamic_dependency_ids || []) {
        const sourceNode = nodeLookup.get(depId);
        if (sourceNode) {
          links.push({
            key: `dyn-${node.node_id}-${depId}`,
            x1: node.x + nodeW,
            y1: node.y + halfH,
            x2: sourceNode.x,
            y2: sourceNode.y + halfH,
            dynamic: true,
            activeFlow: false,
            completed: false,
          });
        }
      }
    }
    const maxRows = Math.max(...columns.map((layer) => grouped.get(layer)?.length || 0), 1);
    const width = Math.max(480, columns.length * colSpacing + 48);
    const height = Math.max(160, maxRows * rowSpacing + 48);
    return { nodes, links, width, height };
  }, [tasks]);

  useEffect(() => {
    if (!scrollRef.current) return;
    const viewportWidth = scrollRef.current.clientWidth || graph.width;
    const nextZoom = Math.min(1, Math.max(0.6, viewportWidth / (graph.width + 120)));
    setZoom(nextZoom);
  }, [graph.width]);

  useEffect(() => {
    if (!scrollRef.current) return;
    if (focusMode === "all") return;
    const selector = focusMode === "running" ? ".dag-node.tone-running" : ".dag-node.tone-failed";
    const element = graphRef.current?.querySelector(selector);
    if (element) {
      element.scrollIntoView({ behavior: "smooth", block: "center", inline: "center" });
    }
  }, [focusMode, tasks]);

  if (!tasks.length) {
    return <div className="empty-state compact-empty"><p>No task graph data available yet.</p></div>;
  }

  return (
    <section className="panel dag-panel">
      <div className="panel-header">
        <div>
          <h3 className="panel-title">Task graph</h3>
          <p className="panel-subtitle">
            {isLive ? "Live execution stages" : "Execution stages"}
          </p>
        </div>
        <div className="dag-toolbar">
          <div className="dag-mode-toggle">
            {[
              ["all", "All"],
              ["running", "Running"],
              ["failed", "Failed"],
            ].map(([key, label]) => (
              <button
                key={key}
                className={`detail-tab ${focusMode === key ? "active" : ""}`}
                onClick={() => setFocusMode(key)}
              >
                {label}
              </button>
            ))}
          </div>
          <div className="dag-legend">
            <span><i className="legend-dot tone-pending" /> Pending</span>
            <span><i className="legend-dot tone-running" /> Running</span>
            <span><i className="legend-dot tone-success" /> Succeeded</span>
            <span><i className="legend-dot tone-failed" /> Failed</span>
            <span><i className="legend-line" /> Dependency</span>
            <span><i className="legend-line dynamic" /> Dynamic</span>
          </div>
          <div className="zoom-controls">
            <button className="ghost-button" onClick={() => setZoom(1)}>Reset</button>
            <button className="ghost-button" onClick={() => setZoom((value) => Math.max(0.7, value - 0.1))}>−</button>
            <span>{Math.round(zoom * 100)}%</span>
            <button className="ghost-button" onClick={() => setZoom((value) => Math.min(1.5, value + 0.1))}>+</button>
          </div>
        </div>
      </div>
      <DagProgress tasks={tasks} />
      <div className="dag-shell">
        <div className="dag-scroll" ref={scrollRef}>
          <svg
            ref={graphRef}
            className="dag-canvas"
            viewBox={`0 0 ${graph.width} ${graph.height}`}
            style={{ transform: `scale(${zoom})`, transformOrigin: "top left" }}
          >
            {graph.links.map((link) => (
              <path
                key={link.key}
                d={`M ${link.x1} ${link.y1} C ${link.x1 + 24} ${link.y1}, ${link.x2 - 24} ${link.y2}, ${link.x2} ${link.y2}`}
                className={[
                  "dag-link",
                  link.dynamic ? "dynamic" : "",
                  link.activeFlow ? "active-flow" : "",
                  link.completed ? "completed" : "",
                ].filter(Boolean).join(" ")}
              />
            ))}
            {graph.nodes.map((node) => (
              <g
                key={node.task_key}
                transform={`translate(${node.x}, ${node.y})`}
                onClick={() => onOpenTask(node.task_key)}
                className={`dag-node tone-${statusTone(node.status)} ${activeTaskKey === node.task_key ? "active" : ""}`}
              >
                <rect rx="10" ry="10" width="148" height="54" />
                <text x="12" y="20" className="dag-node-name">{truncateLabel(node.task_name, 18)}</text>
                <text x="12" y="37" className="dag-node-sub">{node.status}</text>
                <text x="126" y="32" className="dag-node-icon">{statusIcon(node.status)}</text>
              </g>
            ))}
          </svg>
        </div>
      </div>
    </section>
  );
}

function CacheBrowser({ entries, workspaceLabel, onDelete, onClearAll, clearing }) {
  return (
    <section className="panel">
      <div className="panel-header">
        <div>
          <h2 className="panel-title">{workspaceLabel ? `${workspaceLabel} cache` : "Cache browser"}</h2>
          <p className="panel-subtitle">Content-addressed entries</p>
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
          <GinkgoLeafIcon />
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
            <h3 className="panel-title">Run workflow</h3>
            <p className="panel-subtitle">Pick a workflow file and optional config overlays.</p>
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

function RunDetail({ run, projectRoot, onOpenTask, activeTaskKey, nowValue }) {
  const [detailTab, setDetailTab] = useState("graph");
  const tasks = run.tasks || [];
  const summary = useMemo(() => summarizeRunTasks(run), [run]);

  const started = formatDateTimeLarge(run.manifest.started_at);
  const workflowLabel = relativeWorkflowPath(run.manifest.workflow, projectRoot);
  const elapsedSeconds = runElapsedSeconds(run, nowValue);
  const isRunning = run.manifest?.status === "running";

  return (
    <section className="run-detail">
      <div className="detail-hero">
        <div className="hero-copy">
          <div className="hero-top-row">
            <span className="hero-workflow">{workflowLabel}</span>
            <Badge status={run.manifest.status || "unknown"} />
            {isRunning ? <span className="hero-live-pill">Live run</span> : null}
          </div>
          <div className="hero-datetime">
            <span className="hero-date">{started.date}</span>
            <span className="hero-time">{started.time}</span>
          </div>
          <div className="hero-pills">
            <span className="hero-pill">{run.workspace_label}</span>
            <span className="hero-pill">Jobs {run.manifest.jobs ?? "auto"}</span>
            <span className="hero-pill">Cores {run.manifest.cores ?? "auto"}</span>
            <span className="hero-pill">Elapsed {formatDuration(elapsedSeconds)}</span>
            <span className="hero-pill hero-run-id">{shortRunHash(run.run_id)}</span>
          </div>
        </div>
      </div>

      <div className="metrics-grid">
        <MetricCard label="Tasks" value={tasks.length} subvalue="Total recorded" />
        <MetricCard label="Succeeded" value={summary.succeeded || 0} subvalue="Executed OK" accent="emerald" />
        <MetricCard label="Cached" value={summary.cached || 0} subvalue="From cache" accent="blue" />
        <MetricCard
          label={isRunning ? "Running" : "Failed"}
          value={isRunning ? summary.running || 0 : summary.failed || 0}
          subvalue={isRunning ? "In flight now" : "Need attention"}
          accent={isRunning ? "gold" : "rose"}
        />
      </div>

      <div className="detail-tabs">
        {[
          ["graph", "Graph"],
          ["tasks", "Tasks"],
          ["config", "Config"],
        ].map(([key, label]) => (
          <button
            key={key}
            className={`detail-tab ${detailTab === key ? "active" : ""}`}
            onClick={() => setDetailTab(key)}
          >
            {label}
          </button>
        ))}
      </div>

      {detailTab === "graph" ? (
        <DagView
          tasks={tasks}
          onOpenTask={onOpenTask}
          activeTaskKey={activeTaskKey}
          isLive={isRunning}
        />
      ) : null}

      {detailTab === "tasks" ? (
        <section className="panel">
          <div className="panel-header">
            <div>
              <h3 className="panel-title">Tasks</h3>
              <p className="panel-subtitle">Click a task to inspect inputs and logs</p>
            </div>
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
      ) : null}

      {detailTab === "config" ? (
        <section className="panel">
          <div className="panel-header">
            <div>
              <h3 className="panel-title">Config</h3>
              <p className="panel-subtitle">Resolved params</p>
            </div>
          </div>
          <pre className="code-block">{JSON.stringify(run.params || {}, null, 2)}</pre>
        </section>
      ) : null}
    </section>
  );
}

export function App() {
  const { pathname, navigate } = usePathname();
  const route = useMemo(() => parseRoute(pathname), [pathname]);
  const [meta, setMeta] = useState(null);
  const [workspaces, setWorkspaces] = useState([]);
  const [runs, setRuns] = useState([]);
  const [cacheEntries, setCacheEntries] = useState([]);
  const [workflows, setWorkflows] = useState([]);
  const [runDetail, setRunDetail] = useState(null);
  const [taskDetail, setTaskDetail] = useState(null);
  const [taskLog, setTaskLog] = useState(null);
  const [error, setError] = useState(null);
  const [notice, setNotice] = useState(null);
  const [initialLoading, setInitialLoading] = useState(true);
  const [refreshTick, setRefreshTick] = useState(0);
  const [live, setLive] = useState(false);
  const [runDialogOpen, setRunDialogOpen] = useState(false);
  const [launchingRun, setLaunchingRun] = useState(false);
  const [clearingCache, setClearingCache] = useState(false);
  const [loadingWorkspace, setLoadingWorkspace] = useState(false);
  const [nowValue, setNowValue] = useState(Date.now());

  const isRefresh = React.useRef(false);
  const liveContextRef = React.useRef({});
  const activeWorkspaceId = meta?.active_workspace_id || null;
  const activeWorkspace = meta?.active_workspace || null;
  const targetWorkspaceId = route.workspaceId || activeWorkspaceId;
  const targetWorkspace = workspaces.find((workspace) => workspace.workspace_id === targetWorkspaceId) || activeWorkspace;

  useEffect(() => {
    liveContextRef.current = {
      route,
      targetWorkspaceId,
    };
  }, [route, targetWorkspaceId]);

  useEffect(() => {
    const protocol = window.location.protocol === "https:" ? "wss" : "ws";
    const socket = new WebSocket(`${protocol}://${window.location.host}/ws`);

    socket.onopen = () => setLive(true);
    socket.onerror = () => setLive(false);
    socket.onclose = () => setLive(false);
    socket.onmessage = (event) => {
      try {
        const message = JSON.parse(event.data);
        const { route: currentRoute, targetWorkspaceId: currentWorkspaceId } = liveContextRef.current;
        const payload = message.payload || {};

        if (message.type === "connected") {
          setLive(true);
          return;
        }

        if (message.type === "meta" && payload.meta) {
          setMeta(payload.meta);
          setWorkspaces(payload.meta.workspaces || []);
          return;
        }

        if (message.type === "runs_updated" && payload.workspace_id === currentWorkspaceId) {
          setRuns(payload.runs || []);
          return;
        }

        if (
          message.type === "run_updated" &&
          payload.workspace_id === currentWorkspaceId &&
          payload.run
        ) {
          setRuns((current) => upsertRunSummary(current, payload.run));
          if (
            currentRoute?.runId === payload.run.run_id &&
            currentRoute?.workspaceId === payload.workspace_id
          ) {
            setRunDetail(payload.run);
          }
          return;
        }

        if (
          message.type === "task_log_updated" &&
          currentRoute?.page === "task" &&
          currentRoute?.workspaceId === payload.workspace_id &&
          currentRoute?.runId === payload.run_id &&
          currentRoute?.taskKey === payload.task_key
        ) {
          setTaskLog({
            stdout: payload.stdout ?? "",
            stderr: payload.stderr ?? "",
          });
        }
      } catch (_error) {
        setLive(false);
      }
    };

    return () => socket.close();
  }, []);

  useEffect(() => {
    const handle = window.setInterval(() => setNowValue(Date.now()), 1000);
    return () => window.clearInterval(handle);
  }, []);

  useEffect(() => {
    isRefresh.current = false;
  }, [route.page, route.workspaceId, route.runId]);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      if (!isRefresh.current) {
        setInitialLoading(true);
      }
      setError(null);

      try {
        const metaData = await apiFetch("/api/meta");
        if (cancelled) return;

        const workspaceList = metaData.workspaces || [];
        setMeta(metaData);
        setWorkspaces(workspaceList);

        const workspaceId = route.workspaceId || metaData.active_workspace_id;
        if (!workspaceId) {
          setRuns([]);
          setCacheEntries([]);
          setWorkflows([]);
          setRunDetail(null);
          setTaskDetail(null);
          setTaskLog(null);
          return;
        }

        const [runsData, cacheData, workflowsData] = await Promise.all([
          apiFetch(`/api/workspaces/${workspaceId}/runs`),
          apiFetch(`/api/workspaces/${workspaceId}/cache`),
          apiFetch(`/api/workspaces/${workspaceId}/workflows`),
        ]);
        if (cancelled) return;
        setRuns(runsData.runs || []);
        setCacheEntries(cacheData.entries || []);
        setWorkflows(workflowsData.workflows || []);

        if (route.page === "workspaces" || route.page === "home" || route.page === "runs" || route.page === "cache") {
          setRunDetail(null);
          setTaskDetail(null);
          setTaskLog(null);
          return;
        }

        if (!route.runId) {
          setRunDetail(null);
          setTaskDetail(null);
          setTaskLog(null);
          return;
        }

        const runData = await apiFetch(`/api/workspaces/${workspaceId}/runs/${route.runId}`);
        if (cancelled) return;
        setRunDetail(runData);

        if (route.page === "task" && route.taskKey) {
          const [taskData, logData] = await Promise.all([
            apiFetch(`/api/workspaces/${workspaceId}/runs/${route.runId}/tasks/${route.taskKey}`),
            apiFetch(`/api/workspaces/${workspaceId}/runs/${route.runId}/tasks/${route.taskKey}/log`),
          ]);
          if (cancelled) return;
          setTaskDetail(taskData);
          setTaskLog({
            stdout: logData.stdout ?? logData.content ?? "",
            stderr: logData.stderr ?? "",
          });
        } else {
          setTaskDetail(null);
          setTaskLog(null);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err.message || String(err));
        }
      } finally {
        if (!cancelled) {
          setInitialLoading(false);
          isRefresh.current = true;
        }
      }
    }

    load();
    return () => {
      cancelled = true;
    };
  }, [route.page, route.workspaceId, route.runId, route.taskKey, refreshTick]);

  useEffect(() => {
    if (!meta || route.page !== "home") return;
    if (!meta.active_workspace_id) return;
    navigate(`/workspaces/${meta.active_workspace_id}/runs`);
  }, [meta, route.page, navigate]);

  async function loadWorkspace() {
    setLoadingWorkspace(true);
    setError(null);
    setNotice(null);
    try {
      const response = await apiFetch("/api/workspaces/load", { method: "POST", body: "{}" });
      setNotice(`Loaded workspace ${response.workspace.label}.`);
      navigate(`/workspaces/${response.workspace.workspace_id}/runs`);
      setRefreshTick((tick) => tick + 1);
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setLoadingWorkspace(false);
    }
  }

  async function activateWorkspace(workspaceId) {
    if (!workspaceId) return;
    setError(null);
    setNotice(null);
    try {
      await apiFetch("/api/workspaces/activate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ workspace_id: workspaceId }),
      });
      if (route.page === "cache") {
        navigate(`/workspaces/${workspaceId}/cache`);
      } else if (route.page === "workspaces" || route.page === "home" || route.page === "runs") {
        navigate(`/workspaces/${workspaceId}/runs`);
      } else {
        navigate(`/workspaces/${workspaceId}/runs`);
      }
      setRefreshTick((tick) => tick + 1);
    } catch (err) {
      setError(err.message || String(err));
    }
  }

  function openRun(runId) {
    if (!targetWorkspaceId) return;
    navigate(`/workspaces/${targetWorkspaceId}/runs/${runId}`);
  }

  function openTask(taskKey) {
    if (!runDetail) return;
    navigate(
      `/workspaces/${runDetail.workspace_id}/runs/${runDetail.run_id}/tasks/${encodeURIComponent(taskKey)}`,
    );
  }

  function closeTask() {
    if (!runDetail) return;
    navigate(`/workspaces/${runDetail.workspace_id}/runs/${runDetail.run_id}`);
  }

  async function deleteCacheEntry(cacheKey) {
    if (!targetWorkspaceId) return;
    setError(null);
    setNotice(null);
    try {
      await apiFetch(`/api/workspaces/${targetWorkspaceId}/cache/${cacheKey}`, { method: "DELETE" });
      setNotice(`Deleted cache entry ${cacheKey}.`);
      setRefreshTick((tick) => tick + 1);
    } catch (err) {
      setError(err.message || String(err));
    }
  }

  async function clearCache() {
    if (!targetWorkspaceId) return;
    if (!window.confirm("Clear all cache entries?")) return;
    setClearingCache(true);
    setError(null);
    setNotice(null);
    try {
      const response = await apiFetch(`/api/workspaces/${targetWorkspaceId}/cache`, { method: "DELETE" });
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

  function openRunDialog() {
    setRunDialogOpen(true);
    setError(null);
  }

  async function launchWorkflow(payload) {
    if (!targetWorkspaceId) return;
    setLaunchingRun(true);
    setError(null);
    setNotice(null);
    try {
      const response = await apiFetch(`/api/workspaces/${targetWorkspaceId}/run`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      setNotice(
        response.workspace_changed
          ? `Started ${response.workflow} in ${response.workspace_label} (pid ${response.pid}).`
          : `Started ${response.workflow} (pid ${response.pid}).`,
      );
      setRunDialogOpen(false);
      if (response.workspace_id) {
        navigate(`/workspaces/${response.workspace_id}/runs`);
      }
      setRefreshTick((tick) => tick + 1);
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setLaunchingRun(false);
    }
  }

  const initialWorkflow = runDetail?.manifest?.workflow
    ? relativeWorkflowPath(runDetail.manifest.workflow, targetWorkspace?.project_root)
    : workflows[0] || "";

  return (
    <div className="app-shell">
      <Sidebar
        route={route}
        activeWorkspaceId={activeWorkspaceId}
        navigate={navigate}
      />

      <div className="app-main">
        <Topbar
          activeWorkspace={targetWorkspace}
          workspaces={workspaces}
          live={live}
          loadingWorkspace={loadingWorkspace}
          onActivateWorkspace={activateWorkspace}
          onLoadWorkspace={loadWorkspace}
          onOpenRunDialog={openRunDialog}
        />

        <Breadcrumbs
          route={route}
          activeWorkspace={targetWorkspace}
          runDetail={runDetail}
          navigate={navigate}
        />

        {error ? <div className="error-banner">{error}</div> : null}
        {notice ? <div className="notice-banner">{notice}</div> : null}

        <main className="main-content">
          {initialLoading ? (
            <section className="panel loading-panel">
              <div className="spinner-ring" />
              <p>Loading…</p>
            </section>
          ) : route.page === "workspaces" ? (
            <WorkspaceOverview
              workspaces={workspaces}
              activeWorkspaceId={activeWorkspaceId}
              onActivateWorkspace={activateWorkspace}
            />
          ) : route.page === "cache" ? (
            <CacheBrowser
              entries={cacheEntries}
              workspaceLabel={targetWorkspace?.label}
              onDelete={deleteCacheEntry}
              onClearAll={clearCache}
              clearing={clearingCache}
            />
          ) : route.page === "run" || route.page === "task" ? (
            runDetail ? (
              <RunDetail
                run={runDetail}
                projectRoot={targetWorkspace?.project_root}
                onOpenTask={openTask}
                activeTaskKey={route.page === "task" ? route.taskKey : null}
                nowValue={nowValue}
              />
            ) : (
              <section className="panel empty-state">
                <GinkgoLeafIcon />
                <h3>No run selected</h3>
                <p>Choose a run from the current workspace to inspect its task graph and provenance.</p>
              </section>
            )
          ) : targetWorkspace ? (
            <RunList
              runs={runs}
              workspaceLabel={targetWorkspace.label}
              latestRunId={targetWorkspace.latest_run_id}
              onOpenRun={openRun}
              onOpenRunDialog={openRunDialog}
              live={live}
            />
          ) : (
            <section className="panel empty-state">
              <GinkgoLeafIcon />
              <h3>No workspace loaded</h3>
              <p>Use the Load workspace button to select a Ginkgo project folder.</p>
            </section>
          )}
        </main>

        <div className={`screen-live-indicator ${live ? "active" : ""}`}>
          {live ? "Live" : "Idle"}
        </div>
      </div>

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
