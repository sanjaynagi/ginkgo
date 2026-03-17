(function () {
  const PANEL_ID = "ginkgo-resource-monitor";
  const REFRESH_MS = 2000;

  function ensurePanel() {
    let panel = document.getElementById(PANEL_ID);
    if (panel) {
      return panel;
    }

    panel = document.createElement("aside");
    panel.id = PANEL_ID;
    panel.setAttribute("aria-live", "polite");
    panel.style.position = "fixed";
    panel.style.right = "18px";
    panel.style.bottom = "18px";
    panel.style.zIndex = "9999";
    panel.style.minWidth = "210px";
    panel.style.padding = "12px 14px";
    panel.style.borderRadius = "12px";
    panel.style.background = "rgba(12, 18, 22, 0.9)";
    panel.style.color = "#f8fafc";
    panel.style.boxShadow = "0 18px 40px rgba(15, 23, 42, 0.24)";
    panel.style.border = "1px solid rgba(148, 163, 184, 0.16)";
    panel.style.backdropFilter = "blur(10px)";
    panel.style.fontFamily = "Inter, sans-serif";
    panel.style.display = "none";
    document.body.appendChild(panel);
    return panel;
  }

  async function fetchJson(url) {
    const response = await fetch(url, { headers: { Accept: "application/json" } });
    if (!response.ok) {
      throw new Error("Failed to fetch " + url);
    }
    return response.json();
  }

  function currentRunIdFromPath() {
    const parts = window.location.pathname.split("/").filter(Boolean);
    if (parts[0] === "runs" && parts[1]) {
      return parts[1];
    }
    return null;
  }

  async function resolveRunId() {
    const fromPath = currentRunIdFromPath();
    if (fromPath) {
      return fromPath;
    }

    const meta = await fetchJson("/api/meta");
    return meta.selected_run_id || meta.latest_run_id || null;
  }

  function formatCpu(value) {
    if (typeof value !== "number") {
      return "--";
    }
    return value >= 100 ? value.toFixed(0) + "%" : value.toFixed(1) + "%";
  }

  function formatBytes(value) {
    if (typeof value !== "number") {
      return "--";
    }

    const units = ["B", "KiB", "MiB", "GiB", "TiB"];
    let size = value;
    let index = 0;
    while (size >= 1024 && index < units.length - 1) {
      size /= 1024;
      index += 1;
    }
    return (size >= 10 || index === 0 ? size.toFixed(0) : size.toFixed(1)) + " " + units[index];
  }

  function setLine(node, label, value) {
    const row = document.createElement("div");
    row.style.display = "flex";
    row.style.justifyContent = "space-between";
    row.style.gap = "12px";
    row.style.fontSize = "12px";
    row.style.lineHeight = "1.5";

    const left = document.createElement("span");
    left.textContent = label;
    left.style.opacity = "0.72";

    const right = document.createElement("strong");
    right.textContent = value;
    right.style.fontWeight = "600";

    row.appendChild(left);
    row.appendChild(right);
    node.appendChild(row);
  }

  function render(run) {
    const panel = ensurePanel();
    const resources = run.resources || (run.manifest && run.manifest.resources) || null;
    if (!resources || resources.status === "pending") {
      panel.style.display = "none";
      return;
    }

    const current = resources.current || {};
    const average = resources.average || {};
    const peak = resources.peak || {};
    const isLive = resources.status === "running";

    panel.replaceChildren();
    panel.style.display = "block";

    const title = document.createElement("div");
    title.style.display = "flex";
    title.style.justifyContent = "space-between";
    title.style.alignItems = "center";
    title.style.marginBottom = "8px";

    const heading = document.createElement("strong");
    heading.textContent = "Run usage";
    heading.style.fontSize = "12px";
    heading.style.letterSpacing = "0.06em";
    heading.style.textTransform = "uppercase";

    const badge = document.createElement("span");
    badge.textContent = isLive ? "Live" : "Summary";
    badge.style.fontSize = "11px";
    badge.style.padding = "2px 7px";
    badge.style.borderRadius = "999px";
    badge.style.background = isLive ? "rgba(20, 184, 166, 0.2)" : "rgba(148, 163, 184, 0.18)";
    badge.style.color = isLive ? "#99f6e4" : "#cbd5e1";

    title.appendChild(heading);
    title.appendChild(badge);
    panel.appendChild(title);

    setLine(panel, isLive ? "CPU" : "CPU avg", formatCpu(isLive ? current.cpu_percent : average.cpu_percent));
    setLine(panel, "CPU peak", formatCpu(peak.cpu_percent));
    setLine(panel, isLive ? "RSS" : "RSS avg", formatBytes(isLive ? current.rss_bytes : average.rss_bytes));
    setLine(panel, "RSS peak", formatBytes(peak.rss_bytes));
  }

  async function refresh() {
    try {
      const runId = await resolveRunId();
      if (!runId) {
        ensurePanel().style.display = "none";
        return;
      }
      const run = await fetchJson("/api/runs/" + encodeURIComponent(runId));
      render(run);
    } catch (_error) {
      ensurePanel().style.display = "none";
    }
  }

  const originalPushState = history.pushState;
  history.pushState = function () {
    const result = originalPushState.apply(this, arguments);
    window.dispatchEvent(new Event("ginkgo:navigate"));
    return result;
  };

  const originalReplaceState = history.replaceState;
  history.replaceState = function () {
    const result = originalReplaceState.apply(this, arguments);
    window.dispatchEvent(new Event("ginkgo:navigate"));
    return result;
  };

  window.addEventListener("popstate", refresh);
  window.addEventListener("ginkgo:navigate", refresh);
  refresh();
  window.setInterval(refresh, REFRESH_MS);
})();
