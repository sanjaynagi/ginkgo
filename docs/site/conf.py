"""Sphinx configuration for the end-user documentation site."""

from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

project = "Ginkgo"
author = "Ginkgo contributors"
copyright = "2026, Ginkgo contributors"
release = "0.1.0"

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx_copybutton",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

source_suffix = {
    ".md": "markdown",
    ".rst": "restructuredtext",
}

myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "fieldlist",
]

autosummary_generate = True
autodoc_member_order = "bysource"
autodoc_typehints = "description"
autodoc_class_signature = "separated"
napoleon_google_docstring = False
napoleon_numpy_docstring = True

html_theme = "furo"
html_title = "Ginkgo Docs"
html_static_path = ["_static"]
html_css_files = ["custom.css"]
html_context = {
    "default_mode": "light",
}
html_theme_options = {
    "sidebar_hide_name": False,
    "top_of_page_buttons": [],
}

html_logo = None
html_favicon = None
