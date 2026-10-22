"""Jinja-based renderer that turns :class:`ReportData` into a bundle.

The default output is a directory bundle containing ``index.html``, the
stylesheet, bundled fonts, a small islands JS module, and copied artifacts
(rendered pages, log files), with a network-free static copy beside each
marimo page. ``single_file=True`` produces one HTML document with the
stylesheet, fonts and script inlined and each rendered page embedded, as its
static copy when it is a marimo page; it links, but does not include, the logs.
"""

from __future__ import annotations

import base64
import json
import mimetypes
import shutil
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import jinja2

from ginkgo.query import TaskRow
from ginkgo.runtime.run_summary import RunSummary

from ginkgo_report.model import ReportData, build_report_data
from ginkgo_report.sizing import SizingPolicy


_TEMPLATES_DIR = Path(__file__).parent / "templates"
_STATIC_DIR = Path(__file__).parent / "static"

#: Written at the root of every exported report. Its presence marks the
#: directory as ginkgo-owned, so a later export may replace its contents.
_MARKER_NAME = ".ginkgo-report.json"

#: Everything a bundle export writes at its root. Re-rendering removes only
#: these, so files a user keeps beside a report survive it.
_BUNDLE_ENTRIES = (_MARKER_NAME, "index.html", "assets", "pages", "logs")

#: Carried in the head of every report page, so a single-file export can tell
#: its own earlier output from a user's file of the same name.
_GENERATOR_META = '<meta name="generator" content="ginkgo-report">'


@dataclass(frozen=True, kw_only=True)
class ExportResult:
    """Summary of a completed export.

    Parameters
    ----------
    index_path : Path
        The report's HTML page.
    single_file : bool
        Whether the report is one self-contained HTML file.
    omitted_links : tuple[str, ...]
        Relative paths of files the report links but did not write: the logs
        a single-file report leaves out.
    """

    index_path: Path
    single_file: bool
    omitted_links: tuple[str, ...] = ()


def export_report(
    *,
    summary: RunSummary,
    out: Path,
    workspace_label: str | None = None,
    policy: SizingPolicy | None = None,
    single_file: bool = False,
    managed_destination: bool = False,
    force: bool = False,
    measured_runs: Mapping[str, TaskRow] | None = None,
    include_code: bool = True,
) -> ExportResult:
    """Export a bundle or single-file report for one run.

    Parameters
    ----------
    summary : RunSummary
        The run to report on, loaded from the ledger.
    out : Path
        Destination. For the default bundle, the directory the report is
        written into. For ``single_file``, the HTML file itself; nothing else
        is written or removed.
    workspace_label : str | None
        Label for the workspace header. Inferred when omitted.
    policy : SizingPolicy | None
        Size caps applied at export time.
    single_file : bool
        When True, emit one HTML file with CSS, fonts, and script inlined and
        the rendered pages embedded. Logs are not, so their links are listed
        in ``ExportResult.omitted_links``.
    managed_destination : bool
        Set when ``out`` is a path ginkgo derived rather than one a user
        chose, under ``.ginkgo/reports/``. Such a path is ginkgo's by
        construction and is replaced without needing the ownership marker
        that later exports leave behind.
    force : bool
        Write into a destination that ginkgo did not write. A missing
        destination, an empty directory, or a previous export is always
        written; anything else raises ``FileExistsError`` unless this is set.
        Either way only the report's own files are replaced.
    measured_runs : Mapping[str, TaskRow] | None
        The execution that measured each cache key's result, so a cache hit
        can show the peak memory of the run that produced it.
    include_code : bool
        Whether the static copy of a notebook page shows each cell's code.

    Returns
    -------
    ExportResult

    Raises
    ------
    FileExistsError
        When ``out`` is a user-chosen path holding something no earlier
        export wrote and ``force`` is False.
    """
    report = build_report_data(
        summary=summary,
        workspace_label=workspace_label,
        policy=policy,
        measured_runs=measured_runs,
        include_code=include_code,
    )
    out = Path(out)

    if single_file:
        _prepare_out_file(out, managed_destination=managed_destination, force=force)
        index_path = _render_single_file(report=report, out_path=out)
        embedded = {card.link_relpath for card in report.pages}
        omitted = tuple(
            copy.dest_relpath
            for copy in report.artifact_copies
            if copy.dest_relpath not in embedded
        )
        return ExportResult(index_path=index_path, single_file=True, omitted_links=omitted)

    _prepare_out_dir(out, managed_destination=managed_destination, force=force)
    _write_marker(out, report=report)
    index_path = _render_bundle(report=report, out_dir=out)
    return ExportResult(index_path=index_path, single_file=False)


# ----- Bundle mode --------------------------------------------------------


def _render_bundle(*, report: ReportData, out_dir: Path) -> Path:
    """Write a directory bundle at ``out_dir``."""
    _copy_static(out_dir=out_dir)
    _copy_artifacts(report=report, out_dir=out_dir)
    _write_static_pages(report=report, out_dir=out_dir)

    env = _jinja_env()
    template = env.get_template("index.html.j2")
    html = template.render(
        report=report,
        single_file=False,
        css_href="assets/report.css",
        islands_src="assets/islands.js",
        logo_src="assets/images/ginkgo-leaf.png",
        inline_css=None,
        inline_islands=None,
    )
    index_path = out_dir / "index.html"
    index_path.write_text(html, encoding="utf-8")
    return index_path


def _copy_static(*, out_dir: Path) -> None:
    """Copy CSS, fonts, images, and islands JS into the bundle."""
    dest = out_dir / "assets"
    dest.mkdir(parents=True, exist_ok=True)

    report_css = _STATIC_DIR / "report.css"
    if report_css.is_file():
        shutil.copyfile(report_css, dest / "report.css")

    islands = _STATIC_DIR / "islands.js"
    if islands.is_file():
        shutil.copyfile(islands, dest / "islands.js")

    fonts_src = _STATIC_DIR / "fonts"
    if fonts_src.is_dir():
        fonts_dest = dest / "fonts"
        fonts_dest.mkdir(parents=True, exist_ok=True)
        for font_file in fonts_src.iterdir():
            if font_file.is_file():
                shutil.copyfile(font_file, fonts_dest / font_file.name)

    images_src = _STATIC_DIR / "images"
    if images_src.is_dir():
        images_dest = dest / "images"
        images_dest.mkdir(parents=True, exist_ok=True)
        for image_file in images_src.iterdir():
            if image_file.is_file():
                shutil.copyfile(image_file, images_dest / image_file.name)


def _copy_artifacts(*, report: ReportData, out_dir: Path) -> None:
    """Copy the rendered pages and log files the report links."""
    for copy in report.artifact_copies:
        dest = out_dir / copy.dest_relpath
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(copy.source, dest)


def _write_static_pages(*, report: ReportData, out_dir: Path) -> None:
    """Write the network-free copy of each marimo page beside the page."""
    for card in report.pages:
        if card.static_relpath is not None:
            (out_dir / card.static_relpath).write_text(card.embed_html, encoding="utf-8")


# ----- Single-file mode ---------------------------------------------------


def _render_single_file(*, report: ReportData, out_path: Path) -> Path:
    """Render a single HTML file with its stylesheet, fonts, script and pages inlined."""
    out_path.parent.mkdir(parents=True, exist_ok=True)

    css_text = _load_text(_STATIC_DIR / "report.css")
    islands_text = _load_text(_STATIC_DIR / "islands.js")

    css_text = _inline_font_urls(css_text=css_text)

    logo_path = _STATIC_DIR / "images" / "ginkgo-leaf.png"
    logo_src = _data_uri(logo_path) if logo_path.is_file() else None

    env = _jinja_env()
    template = env.get_template("index.html.j2")
    html = template.render(
        report=report,
        single_file=True,
        css_href=None,
        islands_src=None,
        logo_src=logo_src,
        inline_css=css_text,
        inline_islands=islands_text,
    )
    out_path.write_text(html, encoding="utf-8")
    return out_path


def _inline_font_urls(*, css_text: str) -> str:
    """Replace ``url(fonts/...)`` references with data URIs."""
    fonts_dir = _STATIC_DIR / "fonts"
    if not fonts_dir.is_dir():
        return css_text
    for font_file in fonts_dir.iterdir():
        if not font_file.is_file():
            continue
        mime = _font_mime(font_file)
        uri = _data_uri(font_file, mime=mime)
        needle = f"url(fonts/{font_file.name})"
        css_text = css_text.replace(needle, f"url({uri})")
        needle_quoted = f'url("fonts/{font_file.name}")'
        css_text = css_text.replace(needle_quoted, f"url({uri})")
    return css_text


def _font_mime(path: Path) -> str:
    """Return a MIME type for a font file."""
    suffix = path.suffix.lower()
    if suffix == ".woff2":
        return "font/woff2"
    if suffix == ".woff":
        return "font/woff"
    if suffix == ".ttf":
        return "font/ttf"
    return "application/octet-stream"


def _data_uri(path: Path, *, mime: str | None = None) -> str:
    """Return a base64 ``data:`` URI for a local file."""
    resolved_mime = mime or mimetypes.guess_type(str(path))[0] or "application/octet-stream"
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    return f"data:{resolved_mime};base64,{encoded}"


def _load_text(path: Path) -> str:
    """Read ``path`` or return empty string when missing."""
    if not path.is_file():
        return ""
    return path.read_text(encoding="utf-8")


# ----- Jinja env ----------------------------------------------------------


def _jinja_env() -> jinja2.Environment:
    """Build the Jinja environment used for rendering."""
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(_TEMPLATES_DIR),
        autoescape=jinja2.select_autoescape(["html", "htm", "xml", "html.j2"]),
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
    )
    env.filters["yaml"] = _yaml_filter
    return env


def _yaml_filter(value: Any) -> str:
    """Render a Python value as a YAML block."""
    import yaml

    try:
        return yaml.safe_dump(value, sort_keys=False, default_flow_style=False).rstrip()
    except Exception:
        return str(value)


# ----- Utilities ----------------------------------------------------------


def _is_report_dir(path: Path) -> bool:
    """Report whether ``path`` holds a report written by an earlier export."""
    return (path / _MARKER_NAME).is_file()


def _is_report_file(path: Path) -> bool:
    """Report whether ``path`` is a single-file report an earlier export wrote."""
    with path.open("rb") as handle:
        head = handle.read(4096)
    return _GENERATOR_META.encode("utf-8") in head


def _prepare_out_file(out_path: Path, *, managed_destination: bool, force: bool) -> None:
    """Check that ``out_path`` may be written, refusing to replace others' files.

    Only the file itself is ever written: a single-file report shares its
    directory with whatever else is there, such as the bundle of the same run.
    """
    if out_path.is_dir():
        raise FileExistsError(
            f"{out_path} is a directory; a single-file report needs a file path "
            "such as report.html"
        )
    if out_path.exists() and not (force or managed_destination or _is_report_file(out_path)):
        raise FileExistsError(
            f"{out_path} exists and is not a ginkgo report; "
            "refusing to replace a file ginkgo did not write"
        )


def _prepare_out_dir(out_dir: Path, *, managed_destination: bool, force: bool) -> None:
    """Ready ``out_dir`` for a bundle, never removing files the report does not write.

    A missing or empty directory is used as is. So is a destination ginkgo owns:
    either one it derived itself (``managed_destination``) or one an earlier
    export marked with ``_MARKER_NAME``, so re-rendering a report over itself
    needs no opt-in; the earlier report's own entries are removed, so a page
    or log it carried and this one does not is not left behind. Any other
    non-empty directory belongs to the user and raises ``FileExistsError``
    unless ``force`` is set, when the report is written in beside what is
    there, replacing only files of the same name.
    """
    if out_dir.exists() and any(out_dir.iterdir()):
        if managed_destination or _is_report_dir(out_dir):
            _remove_bundle_entries(out_dir)
        elif not force:
            raise FileExistsError(
                f"{out_dir} is not empty and holds no ginkgo report; "
                "refusing to write over files ginkgo did not write"
            )
    out_dir.mkdir(parents=True, exist_ok=True)


def _write_marker(out_dir: Path, *, report: ReportData) -> None:
    """Stamp ``out_dir`` as a ginkgo-owned report directory."""
    payload = {
        "run_id": report.run_id,
        "generated_at": report.generated_at_label,
        "ginkgo_version": report.ginkgo_version,
    }
    (out_dir / _MARKER_NAME).write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _remove_bundle_entries(path: Path) -> None:
    """Remove what an earlier bundle export wrote into ``path``, and nothing else."""
    for name in _BUNDLE_ENTRIES:
        entry = path / name
        if entry.is_dir() and not entry.is_symlink():
            shutil.rmtree(entry)
        elif entry.exists() or entry.is_symlink():
            entry.unlink()
