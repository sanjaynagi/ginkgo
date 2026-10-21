"""A copy of a rendered marimo page that needs no network.

``marimo export html`` writes a page that marimo's frontend draws, and the page
loads that frontend from a CDN, so offline it shows nothing. The page does carry
every cell's code and outputs, as the session data the frontend would draw.
This module draws them as plain HTML instead: markdown, HTML and images as
marimo rendered them, tables as plain tables, and console output and errors as
text. Widgets that only the frontend can draw are named but left out.
"""

from __future__ import annotations

import html
import json
import re
from typing import Any

_MOUNT_CONFIG = "__MARIMO_MOUNT_CONFIG__"
_STATIC_FILES = re.compile(r'"__MARIMO_STATIC__".*?files:\s*Object\.freeze\(', re.S)
_UI_ELEMENT = re.compile(r"<marimo-ui-element\b[^>]*>(.*?)</marimo-ui-element>", re.S)
_TABLE_DATA = re.compile(r"<marimo-table\b[^>]*?\sdata-data='([^']*)'")
_TABLE_TOTAL = re.compile(r"<marimo-table\b[^>]*?\sdata-total-rows='(\d+)'")

#: Rows drawn per table. A table's data is carried in full, but a page that
#: draws thousands of rows is not one anybody reads.
TABLE_ROW_LIMIT = 200

#: Output types in the order a cell's first available one is drawn.
_PREFERRED_MIMES = (
    "text/html",
    "text/markdown",
    "image/svg+xml",
    "image/png",
    "image/jpeg",
    "image/gif",
    "application/vnd.marimo+error",
    "text/plain",
)

_STYLE = """
body { font: 14px/1.55 system-ui, sans-serif; color: #26231D; background: #FFFFFE;
  margin: 0 auto; padding: 16px 20px; max-width: 920px; }
.cell { margin: 0 0 18px; }
details.code > summary { cursor: pointer; font-size: 12px; color: #7F786C; }
pre { font: 12px/1.6 ui-monospace, monospace; background: #F7F5EF; border: 1px solid #E8E4D9;
  border-radius: 4px; padding: 10px 14px; overflow-x: auto; white-space: pre-wrap; }
pre.stderr, pre.error { color: #B04E48; border-color: #E0B4AE; }
img, svg { max-width: 100%; height: auto; }
table { border-collapse: collapse; font-size: 12.5px; margin: 6px 0; }
th, td { border: 1px solid #E8E4D9; padding: 3px 8px; text-align: left; }
th { background: #F7F5EF; }
p.note { font-size: 12px; color: #7F786C; font-style: italic; }
"""


def static_notebook_page(*, page: str, title: str) -> str | None:
    """Return a network-free HTML copy of a marimo export.

    Parameters
    ----------
    page : str
        The page ``marimo export html`` wrote.
    title : str
        Title for the copy, such as the task's label.

    Returns
    -------
    str | None
        A complete HTML document, or ``None`` when *page* is not a marimo
        export whose cells can be read.
    """
    notebook = _mount_value(page, key="notebook")
    session = _mount_value(page, key="session")
    if not isinstance(notebook, dict) or not isinstance(session, dict):
        return None
    files = _static_files(page)
    outputs = {cell.get("id"): cell for cell in session.get("cells", []) if isinstance(cell, dict)}

    parts = [
        "<!doctype html>",
        '<html lang="en"><head><meta charset="utf-8">',
        f"<title>{html.escape(title)}</title><style>{_STYLE}</style></head><body>",
    ]
    for cell in notebook.get("cells", []):
        if not isinstance(cell, dict):
            continue
        parts.append(_render_cell(code=cell, session=outputs.get(cell.get("id"), {}), files=files))
    parts.append("</body></html>")
    return "\n".join(parts)


# ----- Reading the export ---------------------------------------------------


def _mount_value(page: str, *, key: str) -> Any:
    """Return one top-level value of the page's mount config, or ``None``.

    The config is a JavaScript object literal, which some marimo versions end
    with a trailing comma, so it is not JSON as a whole. Each value is.
    """
    start = page.find(_MOUNT_CONFIG)
    if start < 0:
        return None
    match = re.compile(rf'"{key}":\s*').search(page, start)
    if match is None:
        return None
    try:
        return json.JSONDecoder().raw_decode(page, match.end())[0]
    except ValueError:
        return None


def _static_files(page: str) -> dict[str, str]:
    """Return the virtual files the export carries, by the URL outputs use for them."""
    match = _STATIC_FILES.search(page)
    if match is None:
        return {}
    try:
        files = json.JSONDecoder().raw_decode(page, match.end())[0]
    except ValueError:
        return {}
    return {str(url): str(data) for url, data in files.items()} if isinstance(files, dict) else {}


# ----- Drawing a cell ---------------------------------------------------------


def _render_cell(*, code: dict[str, Any], session: dict[str, Any], files: dict[str, str]) -> str:
    """Draw one cell: its code, folded, then its output and console."""
    parts = ['<section class="cell">']
    source = code.get("code")
    hide_code = (code.get("config") or {}).get("hide_code")
    if isinstance(source, str) and source.strip() and not hide_code:
        parts.append(
            f'<details class="code"><summary>code</summary><pre>{html.escape(source)}</pre>'
            "</details>"
        )
    for output in session.get("outputs") or []:
        parts.append(_render_output(output, files=files))
    for entry in session.get("console") or []:
        parts.append(_render_console(entry))
    parts.append("</section>")
    return "".join(parts)


def _render_output(output: Any, *, files: dict[str, str]) -> str:
    """Draw one cell output."""
    if not isinstance(output, dict):
        return ""
    if output.get("type") == "error":
        text = f"{output.get('ename') or 'error'}: {output.get('evalue', '')}"
        return f'<pre class="error">{html.escape(text)}</pre>'
    data = output.get("data")
    if not isinstance(data, dict):
        return ""
    for mime in _PREFERRED_MIMES:
        if mime in data:
            return _render_mime(mime=mime, value=data[mime], files=files)
    if data:
        mime = next(iter(data))
        return f'<p class="note">This output ({html.escape(mime)}) needs marimo to display.</p>'
    return ""


def _render_mime(*, mime: str, value: Any, files: dict[str, str]) -> str:
    """Draw an output of one type."""
    if mime in {"text/html", "text/markdown"}:
        return _static_html(str(value), files=files)
    if mime.startswith("image/"):
        text = str(value)
        if text.lstrip().startswith("<svg"):
            return text
        return f'<img src="{html.escape(files.get(text, text))}" alt="">'
    if mime == "application/vnd.marimo+error":
        errors = value if isinstance(value, list) else [value]
        lines = [
            str(error.get("msg") or error.get("type")) if isinstance(error, dict) else str(error)
            for error in errors
        ]
        return f'<pre class="error">{html.escape(chr(10).join(lines))}</pre>'
    text = str(value)
    return f"<pre>{html.escape(text)}</pre>" if text.strip() else ""


def _render_console(entry: Any) -> str:
    """Draw one stdout or stderr chunk, or a traceback marimo already highlighted."""
    if not isinstance(entry, dict):
        return ""
    text = str(entry.get("text") or "")
    if not text.strip():
        return ""
    if entry.get("mimetype") == "application/vnd.marimo+traceback":
        return f'<div class="traceback">{text}</div>'
    stream = "stderr" if entry.get("name") == "stderr" else "stdout"
    return f'<pre class="{stream}">{html.escape(text)}</pre>'


def _static_html(fragment: str, *, files: dict[str, str]) -> str:
    """Return marimo's HTML for an output with its widgets replaced by plain HTML."""
    for url, data in files.items():
        fragment = fragment.replace(url, data)
    return _UI_ELEMENT.sub(_replace_ui_element, fragment)


def _replace_ui_element(match: re.Match[str]) -> str:
    """Draw a marimo widget as plain HTML: a table's rows, or a note."""
    inner = match.group(1)
    data = _TABLE_DATA.search(inner)
    rows = _table_rows(data.group(1)) if data is not None else None
    if rows is None:
        return '<p class="note">An interactive element here needs marimo to display.</p>'
    total = _TABLE_TOTAL.search(inner)
    return _html_table(rows, total=int(total.group(1)) if total is not None else len(rows))


def _table_rows(attribute: str) -> list[dict[str, Any]] | None:
    """Decode a ``marimo-table``'s rows, or ``None`` when it holds only a link to them.

    The attribute holds the rows as JSON inside a JSON string.
    """
    try:
        rows = json.loads(html.unescape(attribute))
        if isinstance(rows, str):
            rows = json.loads(rows)
    except ValueError:
        return None
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        return None
    return rows


def _html_table(rows: list[dict[str, Any]], *, total: int) -> str:
    """Draw *rows* as a plain table, with a note when some are left out."""
    columns: list[str] = []
    for row in rows:
        columns.extend(key for key in row if key != "_marimo_row_id" and key not in columns)
    shown = rows[:TABLE_ROW_LIMIT]
    head = "".join(f"<th>{html.escape(str(column))}</th>" for column in columns)
    body = "".join(
        "<tr>"
        + "".join(f"<td>{html.escape(_cell_text(row.get(column)))}</td>" for column in columns)
        + "</tr>"
        for row in shown
    )
    note = ""
    if len(shown) < total:
        note = f'<p class="note">First {len(shown)} of {total} rows.</p>'
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>{note}"


def _cell_text(value: Any) -> str:
    """Return a table value as text."""
    return "" if value is None else str(value)
