// Ginkgo report — progressive enhancement islands.
//
// Everything here is optional: the report is fully readable with JS off.
// Adds:
//   - sidebar TOC scrollspy that highlights the current section
//   - sortable task table (click column headers to sort)

/* ----- Scrollspy ----- */

(function scrollspy() {
  const links = document.querySelectorAll("nav.sidebar ul a[href^='#']");
  if (!links.length) return;

  const entries = [];
  for (const link of links) {
    const id = decodeURIComponent(link.getAttribute("href").slice(1));
    const target = document.getElementById(id);
    if (target) entries.push({ target, link });
  }
  if (!entries.length) return;

  const linkList = entries.map((e) => e.link);

  function update() {
    // Reading line sits ~30% down the viewport. Active section is the one
    // whose heading is closest to — but no further below — that line.
    const line = window.innerHeight * 0.3;
    let best = entries[0];
    for (const entry of entries) {
      const top = entry.target.getBoundingClientRect().top;
      if (top <= line) best = entry;
    }
    for (const link of linkList) link.classList.remove("active");
    best.link.classList.add("active");
  }

  let scheduled = false;
  function schedule() {
    if (scheduled) return;
    scheduled = true;
    requestAnimationFrame(() => {
      scheduled = false;
      update();
    });
  }

  window.addEventListener("scroll", schedule, { passive: true });
  window.addEventListener("resize", schedule, { passive: true });
  update();
})();

/* ----- Sortable task table ----- */

(function sortable() {
  const tables = document.querySelectorAll("table.sortable");
  for (const table of tables) {
    const headers = table.tHead?.rows[0]?.cells;
    if (!headers) continue;
    for (let index = 0; index < headers.length; index++) {
      const th = headers[index];
      th.addEventListener("click", () => sortColumn(table, index, th));
    }
  }
})();

function sortColumn(table, columnIndex, th) {
  const tbody = table.tBodies[0];
  if (!tbody) return;
  const rows = Array.from(tbody.rows);

  const current = th.getAttribute("aria-sort");
  const direction = current === "ascending" ? "descending" : "ascending";
  for (const cell of table.tHead.rows[0].cells) {
    cell.removeAttribute("aria-sort");
  }
  th.setAttribute("aria-sort", direction);

  const factor = direction === "ascending" ? 1 : -1;
  rows.sort((a, b) => {
    const av = cellValue(a, columnIndex);
    const bv = cellValue(b, columnIndex);
    if (av === bv) return 0;
    if (typeof av === "number" && typeof bv === "number") {
      return (av - bv) * factor;
    }
    return String(av).localeCompare(String(bv)) * factor;
  });

  for (const row of rows) tbody.appendChild(row);
}

function cellValue(row, columnIndex) {
  const cell = row.cells[columnIndex];
  if (!cell) return "";
  const text = cell.textContent.trim();
  const parsed = parseMaybeNumeric(text);
  return parsed === null ? text.toLowerCase() : parsed;
}

function parseMaybeNumeric(text) {
  // "4.2s", "2m 14s", "8h 03m 12s", "14.2 MB", etc. Best-effort ordering only.
  const duration = parseDurationLike(text);
  if (duration !== null) return duration;
  const numeric = Number(text.replace(/[, ]/g, ""));
  if (!Number.isNaN(numeric) && text !== "") return numeric;
  return null;
}

function parseDurationLike(text) {
  const match = text.match(
    /^(?:(\d+)h\s*)?(?:(\d+)m\s*)?(?:(\d+(?:\.\d+)?)s)?$/i,
  );
  if (!match) return null;
  const [, hours, minutes, seconds] = match;
  if (!hours && !minutes && !seconds) return null;
  return (
    (Number(hours) || 0) * 3600
    + (Number(minutes) || 0) * 60
    + (Number(seconds) || 0)
  );
}
