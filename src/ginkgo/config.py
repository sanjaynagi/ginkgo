"""Workflow configuration loading.

Loads TOML or YAML config files and returns plain dicts. Schema validation and
multi-file layering are deferred to a later phase.
"""

from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
import tomllib
from typing import TYPE_CHECKING, Any, Iterator, Sequence

import yaml

if TYPE_CHECKING:
    from ginkgo.params import ParamDecl, ParamResolution

PARAMS_CONFIG_KEY = "params"
"""Config table holding declared workflow parameter values."""


@dataclass
class _ConfigSession:
    """Tracks config overrides, loaded values, and declared parameters.

    Spans one CLI-driven workflow import. Parameter declarations accumulate as
    the workflow module executes, so the CLI can validate the supplied flags
    once the flow has been built.

    Parameters
    ----------
    override_paths : list[Path]
        Config files given with ``--config``, layered over the project config.
    loaded_values : list[dict[str, Any]]
        Mappings returned by each ``config()`` call, in load order.
    param_config : dict[str, Any]
        The ``[params]`` table available to declared parameters.
    cli_extras : tuple[str, ...]
        Command-line tokens left unparsed by the main CLI parser.
    require_params : bool
        Whether a required parameter that was not supplied is an error. Read-only
        commands set this false so they can still describe the workflow.
    """

    override_paths: list[Path] = field(default_factory=list)
    loaded_values: list[dict[str, Any]] = field(default_factory=list)
    param_config: dict[str, Any] = field(default_factory=dict)
    cli_extras: tuple[str, ...] = ()
    require_params: bool = True
    declarations: dict[str, ParamDecl] = field(default_factory=dict)
    resolutions: dict[str, ParamResolution] = field(default_factory=dict)
    consumed_extras: set[int] = field(default_factory=set)
    declaration_globals: dict[str, dict[str, Any]] = field(default_factory=dict)

    def merged_loaded_values(self) -> dict[str, Any]:
        """Return all loaded config mappings merged in load order."""
        return _merge_top_level_dicts(self.loaded_values)

    def declare_param(
        self,
        decl: ParamDecl,
        *,
        declaring_globals: dict[str, Any] | None = None,
    ) -> Any:
        """Register a parameter declaration and return its resolved value.

        Re-declaring a parameter identically is a no-op returning the value
        already resolved, so a module imported twice does not fail.

        Parameters
        ----------
        decl : ParamDecl
            The declaration to register.
        declaring_globals : dict[str, Any] | None, optional
            The ``globals()`` mapping of the module making the declaration, used
            to spot task bodies that read the parameter as a global.

        Returns
        -------
        Any
            The resolved value.

        Raises
        ------
        ParamError
            If *decl* conflicts with an earlier declaration of the same name, or
            the parameter cannot be resolved.
        """
        from ginkgo.params import ParamError, extract_flag_values, resolve_param

        existing = self.declarations.get(decl.name)
        if existing is not None:
            if existing != decl:
                raise ParamError(
                    f"Workflow parameter {decl.name!r} is declared twice with different "
                    "settings. Declare each parameter once."
                )
            return self.resolutions[decl.name].value

        cli_values, consumed = extract_flag_values(self.cli_extras, decl)
        resolution = resolve_param(
            decl,
            cli_values=cli_values,
            config_values=self.param_config,
            require=self.require_params,
        )

        self.declarations[decl.name] = decl
        self.resolutions[decl.name] = resolution
        self.consumed_extras |= consumed
        if declaring_globals is not None:
            self.declaration_globals[decl.name] = declaring_globals
        return resolution.value

    def resolved_params(self) -> dict[str, Any]:
        """Return declared parameter values, keyed by name."""
        return {name: item.value for name, item in self.resolutions.items()}

    def param_sources(self) -> dict[str, str]:
        """Return where each declared parameter's value came from."""
        return {name: item.source for name, item in self.resolutions.items()}

    def unconsumed_extras(self) -> list[str]:
        """Return command-line tokens that no declaration claimed."""
        return [
            token
            for index, token in enumerate(self.cli_extras)
            if index not in self.consumed_extras
        ]


_CONFIG_SESSIONS: list[_ConfigSession] = []


def current_session() -> _ConfigSession | None:
    """Return the innermost active config session, or ``None`` outside the CLI."""
    return _CONFIG_SESSIONS[-1] if _CONFIG_SESSIONS else None


def config(path: str | Path) -> dict[str, Any]:
    """Load a TOML or YAML configuration file.

    Parameters
    ----------
    path : str | Path
        Path to the configuration file.

    Returns
    -------
    dict[str, Any]
        The parsed configuration as a nested dict.

    Raises
    ------
    FileNotFoundError
        If the config file does not exist.
    """
    session = current_session()
    if session is not None and session.override_paths:
        # Overrides layer over the file the workflow asked for, so supplying one
        # value with --config does not require restating every other value. The
        # base file stays optional: overrides alone may define the whole config.
        mappings: list[dict[str, Any]] = []
        if Path(path).is_file():
            mappings.append(_load_config_mapping(path))
        mappings.extend(_load_config_mapping(item) for item in session.override_paths)
        data = _merge_top_level_dicts(mappings)
    else:
        data = _load_config_mapping(path)

    if session is not None:
        session.loaded_values.append(deepcopy(data))

    return data


def load_runtime_config(
    *,
    project_root: Path,
    override_paths: Sequence[str | Path] | None = None,
) -> dict[str, Any]:
    """Load the CLI runtime config mapping.

    When explicit override paths are provided, they fully define the runtime
    config. Otherwise the canonical project config file is loaded if present.
    """
    resolved_overrides = [Path(path).resolve() for path in override_paths or ()]
    default_path = _default_runtime_config_path(project_root=project_root)

    mappings: list[dict[str, Any]] = []
    if default_path is not None:
        mappings.append(_load_config_mapping(default_path))
    mappings.extend(_load_config_mapping(path) for path in resolved_overrides)

    if not mappings:
        return {}
    return _merge_top_level_dicts(mappings)


@contextmanager
def config_session(
    *,
    override_paths: Sequence[str | Path] | None = None,
    param_config: dict[str, Any] | None = None,
    cli_extras: Sequence[str] | None = None,
    require_params: bool = True,
) -> Iterator[_ConfigSession]:
    """Temporarily override config loading for CLI-driven workflow imports.

    Parameters
    ----------
    override_paths : Sequence[str | Path] | None, optional
        Config files given with ``--config``, layered over the config file the
        workflow loads.
    param_config : dict[str, Any] | None, optional
        The ``[params]`` table declared parameters resolve against. Pass the
        already-loaded runtime config table so resolution does not depend on
        whether the workflow calls ``config()`` before or after ``param()``.
    cli_extras : Sequence[str] | None, optional
        Command-line tokens left unparsed by the main CLI parser, from which
        declared parameters take their values.
    require_params : bool, optional
        Whether an unsupplied required parameter is an error. Read-only commands
        pass false so they can still import and describe the workflow.
    """
    session = _ConfigSession(
        override_paths=[Path(path).resolve() for path in override_paths or ()],
        param_config=dict(param_config or {}),
        cli_extras=tuple(cli_extras or ()),
        require_params=require_params,
    )
    _CONFIG_SESSIONS.append(session)
    try:
        yield session
    finally:
        popped = _CONFIG_SESSIONS.pop()
        assert popped is session


def _load_config_mapping(path: str | Path) -> dict[str, Any]:
    """Load a single TOML or YAML config file and require a top-level mapping."""
    path = Path(path)
    suffix = path.suffix.lower()

    if suffix == ".toml":
        with path.open("rb") as handle:
            data = tomllib.load(handle)
    elif suffix in {".yaml", ".yml"}:
        with path.open(encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
    else:
        raise ValueError(f"Unsupported config format for {path}. Expected .toml, .yaml, or .yml")

    if not isinstance(data, dict):
        raise TypeError(f"Config file must contain a top-level mapping, got {type(data).__name__}")

    return data


def _merge_top_level_dicts(
    mappings: Sequence[dict[str, Any]] | Iterator[dict[str, Any]],
) -> dict[str, Any]:
    """Shallow-merge top-level config keys with last-write-wins semantics."""
    merged: dict[str, Any] = {}
    for mapping in mappings:
        merged.update(deepcopy(mapping))
    return merged


def _default_runtime_config_path(*, project_root: Path) -> Path | None:
    for candidate_name in ("ginkgo.toml", "ginkgo.yaml", "ginkgo.yml"):
        candidate = (project_root / candidate_name).resolve()
        if candidate.is_file():
            return candidate
    return None
