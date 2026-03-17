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
    panel.style.borderRadius = "14px";
    panel.style.background = "rgba(255, 250, 241, 0.94)";
    panel.style.color = "#2c241d";
    panel.style.boxShadow = "0 18px 40px rgba(35, 24, 10, 0.12)";
    panel.style.border = "1px solid rgba(71, 56, 37, 0.12)";
    panel.style.backdropFilter = "blur(10px)";
    panel.style.fontFamily = "'Avenir Next', 'Segoe UI', sans-serif";
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

  function routeContext() {
    const parts = window.location.pathname.split("/").filter(Boolean);
    if (parts[0] === "workspaces" && parts[1] && parts[2] === "runs" && parts[3]) {
      return { workspaceId: parts[1], runId: parts[3] };
    }
    return { workspaceId: null, runId: null };
  }

  async function resolveRunContext() {
    const fromRoute = routeContext();
    if (fromRoute.workspaceId && fromRoute.runId) {
      return fromRoute;
    }

    const meta = await fetchJson("/api/meta");
    if (!meta.active_workspace_id || !meta.latest_run_id) {
      return { workspaceId: null, runId: null };
    }
    return { workspaceId: meta.active_workspace_id, runId: meta.latest_run_id };
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
    left.style.opacity = "0.68";

    const right = document.createElement("strong");
    right.textContent = value;
    right.style.fontWeight = "700";

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
    badge.style.background = isLive ? "rgba(29, 143, 103, 0.14)" : "rgba(91, 77, 64, 0.08)";
    badge.style.color = isLive ? "#1d8f67" : "#5b4d40";

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
      const context = await resolveRunContext();
      if (!context.workspaceId || !context.runId) {
        ensurePanel().style.display = "none";
        return;
      }
      const run = await fetchJson(
        "/api/workspaces/" +
          encodeURIComponent(context.workspaceId) +
          "/runs/" +
          encodeURIComponent(context.runId),
      );
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
