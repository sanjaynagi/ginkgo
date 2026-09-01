"""Jinja-based renderer that turns :class:`ReportData` into a bundle.

The default output is a directory bundle containing ``index.html``, the
stylesheet, bundled fonts, a small islands JS module, and copied artifacts
(figures, notebook HTML, log files). ``single_file=True`` produces one
self-contained HTML document with all assets inlined as data URIs.
"""

from __future__ import annotations

import base64
import json
import mimetypes
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import jinja2

from ginkgo.runtime.run_summary import RunSummary

from .model import ReportData, build_report_data
from .sizing import SizingPolicy


_TEMPLATES_DIR = Path(__file__).parent / "templates"
_STATIC_DIR = Path(__file__).parent / "static"

#: Written at the root of every exported report. Its presence marks the
#: directory as ginkgo-owned, so a later export may replace its contents.
_MARKER_NAME = ".ginkgo-report.json"


@dataclass(frozen=True, kw_only=True)
class ExportResult:
    """Summary of a completed export."""

    bundle_root: Path
    index_path: Path
    single_file: bool


def export_report(
    *,
    summary: RunSummary,
    out_dir: Path,
    workspace_label: str | None = None,
    artifacts_root: Path | None = None,
    policy: SizingPolicy | None = None,
    single_file: bool = False,
    managed_destination: bool = False,
    force: bool = False,
) -> ExportResult:
    """Export a bundle or single-file report for one run.

    Parameters
    ----------
    summary : RunSummary
        The run to report on, loaded from the ledger.
    out_dir : Path
        Destination directory. For the default bundle, the report is
        written inside this directory. For ``single_file``, the HTML is
        written at ``out_dir / "index.html"`` with assets inlined.
    workspace_label : str | None
        Label for the workspace header. Inferred when omitted.
    artifacts_root : Path | None
        Override for the artifact store root. Inferred from the run directory
        when omitted.
    policy : SizingPolicy | None
        Per-kind preview caps.
    single_file : bool
        When True, emit one HTML file with CSS, fonts, figures, and logs
        inlined as data URIs.
    managed_destination : bool
        Set when ``out_dir`` is a path ginkgo derived rather than one a user
        chose — the managed ``.ginkgo/reports/<run-id>/`` directory. Such a
        directory is ginkgo's by construction and is replaced without needing
        the ownership marker that later exports leave behind.
    force : bool
        Replace the contents of a non-empty ``out_dir`` that ginkgo did not
        write. An empty, missing, or previously exported directory is always
        replaced; anything else raises ``FileExistsError`` unless this is set.

    Returns
    -------
    ExportResult

    Raises
    ------
    FileExistsError
        When ``out_dir`` is a user-chosen path holding files that no earlier
        export wrote and ``force`` is False.
    """
    report = build_report_data(
        summary=summary,
        workspace_label=workspace_label,
        artifacts_root=artifacts_root,
        policy=policy,
    )
    out_dir = Path(out_dir)
    _prepare_out_dir(out_dir, managed_destination=managed_destination, force=force)
    _write_marker(out_dir, report=report)

    if single_file:
        index_path = _render_single_file(report=report, out_path=out_dir / "index.html")
    else:
        index_path = _render_bundle(report=report, out_dir=out_dir)

    return ExportResult(bundle_root=out_dir, index_path=index_path, single_file=single_file)


# ----- Bundle mode --------------------------------------------------------


def _render_bundle(*, report: ReportData, out_dir: Path) -> Path:
    """Write a directory bundle at ``out_dir``."""
    _copy_static(out_dir=out_dir)
    _copy_artifacts(report=report, out_dir=out_dir)

    env = _jinja_env()
    template = env.get_template("index.html.j2")
    html = template.render(
        report=report,
        css_href="assets/report.css",
        islands_src="assets/islands.js",
        logo_src="assets/images/ginkgo-leaf.png",
        inline_css=None,
        inline_islands=None,
        image_inliner=None,
        log_inliner=None,
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
    """Copy figure / notebook / log artifacts declared by the report."""
    for copy in report.artifact_copies:
        dest = out_dir / copy.dest_relpath
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(copy.source, dest)


# ----- Single-file mode ---------------------------------------------------


def _render_single_file(*, report: ReportData, out_path: Path) -> Path:
    """Render a single HTML file with assets inlined as data URIs."""
    out_path.parent.mkdir(parents=True, exist_ok=True)

    css_text = _load_text(_STATIC_DIR / "report.css")
    islands_text = _load_text(_STATIC_DIR / "islands.js")

    css_text = _inline_font_urls(css_text=css_text)

    copies_by_relpath: dict[str, Path] = {
        copy.dest_relpath: copy.source for copy in report.artifact_copies
    }

    def image_inliner(relpath: str) -> str:
        source = copies_by_relpath.get(relpath)
        if source is None or not source.is_file():
            return relpath
        # The type comes from the bundle path: it always carries the
        # artifact's extension, whether or not the CAS source filename
        # does (older stores wrote extensionless blobs).
        return _data_uri(source, mime=mimetypes.guess_type(relpath)[0])

    def log_inliner(relpath: str) -> str:
        source = copies_by_relpath.get(relpath)
        if source is None or not source.is_file():
            return relpath
        mime = "text/plain"
        return _data_uri(source, mime=mime)

    logo_path = _STATIC_DIR / "images" / "ginkgo-leaf.png"
    logo_src = _data_uri(logo_path) if logo_path.is_file() else None

    env = _jinja_env()
    template = env.get_template("index.html.j2")
    html = template.render(
        report=report,
        css_href=None,
        islands_src=None,
        logo_src=logo_src,
        inline_css=css_text,
        inline_islands=islands_text,
        image_inliner=image_inliner,
        log_inliner=log_inliner,
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
        autoescape=jinja2.select_autoescape(["html", "htm", "xml"]),
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


def _prepare_out_dir(out_dir: Path, *, managed_destination: bool, force: bool) -> None:
    """Make ``out_dir`` an empty directory, refusing to destroy others' files.

    A missing or empty directory is used as is. So is a destination ginkgo owns:
    either one it derived itself (``managed_destination``) or one an earlier
    export marked with ``_MARKER_NAME``, so re-rendering a report over itself
    needs no opt-in. Any other non-empty directory belongs to the user and
    raises ``FileExistsError`` unless ``force`` is set.
    """
    if out_dir.exists() and any(out_dir.iterdir()):
        if not (force or managed_destination or _is_report_dir(out_dir)):
            raise FileExistsError(
                f"{out_dir} is not empty and holds no ginkgo report; "
                "refusing to delete files ginkgo did not write"
            )
        _clean_dir(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)


def _write_marker(out_dir: Path, *, report: ReportData) -> None:
    """Stamp ``out_dir`` as a ginkgo-owned report directory."""
    payload = {
        "run_id": report.run_id,
        "generated_at": report.generated_at_label,
        "ginkgo_version": report.ginkgo_version,
    }
    (out_dir / _MARKER_NAME).write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _clean_dir(path: Path) -> None:
    """Remove all contents of ``path`` without removing ``path`` itself."""
    for entry in path.iterdir():
        if entry.is_dir() and not entry.is_symlink():
            shutil.rmtree(entry)
        else:
            entry.unlink()
