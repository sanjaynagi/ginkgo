"""Workflow parameter declarations.

A workflow declares the inputs it accepts with :func:`param`, which registers the
declaration and returns the resolved value:

>>> n_replicates = param("n_replicates", type=int, default=12)  # doctest: +SKIP

Values resolve from the CLI first, then the ``[params]`` table of the project
config, then the declared default. A declaration without a default is required.

Declarations are registered on the active config session so that the CLI can
validate them once the workflow has been imported, and so that any consumer able
to import a workflow can enumerate its inputs without running the flow.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Callable, Iterable, Sequence

_NAME_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_]*\Z")

_TRUE_LITERALS = frozenset({"true", "1", "yes", "on"})
_FALSE_LITERALS = frozenset({"false", "0", "no", "off"})

ParamSource = str
"""Where a resolved value came from: ``"cli"``, ``"config"``, or ``"default"``."""


class ParamError(Exception):
    """Raised when a parameter is declared or supplied incorrectly."""


class _Required:
    """Sentinel marking a parameter that has no default and must be supplied."""

    def __repr__(self) -> str:
        return "REQUIRED"


REQUIRED = _Required()


@dataclass(frozen=True, kw_only=True)
class ParamDecl:
    """A declared workflow parameter.

    Parameters
    ----------
    name : str
        Parameter name, also the key read from the config ``[params]`` table.
    value_type : Callable[[str], Any]
        Callable applied to string values to produce the typed value.
    default : Any
        Default value, or ``REQUIRED`` when the parameter must be supplied.
    help_text : str | None
        One-line description shown by ``ginkgo run <workflow> --help``.
    choices : tuple[Any, ...] | None
        Permitted values, or ``None`` to allow any.
    multiple : bool
        When true the flag may repeat and the resolved value is a tuple.
    """

    name: str
    value_type: Callable[[str], Any] = str
    default: Any = REQUIRED
    help_text: str | None = None
    choices: tuple[Any, ...] | None = None
    multiple: bool = False

    @property
    def required(self) -> bool:
        """Whether the parameter has no default and must be supplied."""
        return self.default is REQUIRED

    @property
    def flag(self) -> str:
        """The command-line flag that supplies this parameter."""
        return flag_for(self.name)

    @property
    def type_label(self) -> str:
        """Human-readable type name for help output."""
        return getattr(self.value_type, "__name__", str(self.value_type))

    def to_payload(self) -> dict[str, Any]:
        """Return a JSON-serialisable description of this declaration.

        Lets a consumer enumerate a workflow's inputs — their names, types,
        defaults, and whether they are required — without running the flow.
        """
        return {
            "name": self.name,
            "flag": self.flag,
            "type": self.type_label,
            "required": self.required,
            "default": None if self.required else _jsonable(self.default),
            "help": self.help_text,
            "choices": None if self.choices is None else [_jsonable(c) for c in self.choices],
            "multiple": self.multiple,
        }


@dataclass(frozen=True, kw_only=True)
class ParamResolution:
    """A parameter's resolved value and where it came from."""

    value: Any
    source: ParamSource


@dataclass(frozen=True, kw_only=True)
class ParamContext:
    """The inputs a parameter resolves against, for crossing a process boundary.

    Worker processes re-import the workflow module, which re-runs its ``param()``
    calls. Carrying the resolution *inputs* rather than the resolved values means
    a worker resolves through the same code path as the parent, so declared types
    survive intact — a ``type=Path`` parameter stays a ``Path`` where serialising
    the value as JSON would have flattened it to a string.

    Parameters
    ----------
    config : dict[str, Any]
        The config ``[params]`` table.
    cli_extras : tuple[str, ...]
        Command-line tokens the parent left unparsed.
    """

    config: dict[str, Any]
    cli_extras: tuple[str, ...]

    def to_payload(self) -> dict[str, Any]:
        """Return a JSON-serialisable form for inclusion in a worker payload."""
        return {"config": dict(self.config), "cli_extras": list(self.cli_extras)}

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> ParamContext:
        """Rebuild a context from its ``to_payload`` form."""
        return cls(
            config=dict(payload.get("config") or {}),
            cli_extras=tuple(payload.get("cli_extras") or ()),
        )


def flag_for(name: str) -> str:
    """Return the command-line flag for a parameter name (``n_reps`` → ``--n-reps``)."""
    return f"--{name.replace('_', '-')}"


def param(
    name: str,
    *,
    type: Callable[[str], Any] = str,  # noqa: A002 - matches argparse's keyword
    default: Any = REQUIRED,
    help: str | None = None,  # noqa: A002 - matches argparse's keyword
    choices: Sequence[Any] | None = None,
    multiple: bool = False,
) -> Any:
    """Declare a workflow parameter and return its resolved value.

    Parameters
    ----------
    name : str
        Parameter name. Must start with a letter and contain only letters,
        digits, and underscores. The command-line flag is the dashed form, so
        ``n_replicates`` is supplied as ``--n-replicates``.
    type : Callable[[str], Any], optional
        Callable applied to string values, following ``argparse``'s ``type``
        convention. Defaults to ``str``. ``bool`` is handled specially: the flag
        may be given bare (``--flag``) or with a literal (``--flag false``).
    default : Any, optional
        Value used when the parameter is supplied neither on the command line
        nor in config. Omit to make the parameter required.
    help : str | None, optional
        One-line description shown by ``ginkgo run <workflow> --help``.
    choices : Sequence[Any] | None, optional
        Permitted values. Checked after type conversion.
    multiple : bool, optional
        When true the flag may be repeated and the resolved value is a tuple.

    Returns
    -------
    Any
        The resolved value: from the command line if supplied there, otherwise
        from the config ``[params]`` table, otherwise the default.

    Raises
    ------
    ParamError
        If the name is invalid, the declaration conflicts with an earlier
        declaration of the same name, a required parameter was not supplied, or
        a supplied value fails type conversion or is outside ``choices``.
    """
    decl = ParamDecl(
        name=name,
        value_type=type,
        default=default,
        help_text=help,
        choices=tuple(choices) if choices is not None else None,
        multiple=multiple,
    )
    _validate_declaration(decl)

    from ginkgo.config import current_session

    session = current_session()
    if session is None:
        return _resolve_without_session(decl)
    return session.declare_param(decl)


def resolve_param(
    decl: ParamDecl,
    *,
    cli_values: Sequence[str],
    config_values: dict[str, Any],
    require: bool = True,
) -> ParamResolution:
    """Resolve one parameter from CLI values, config, then its default.

    Parameters
    ----------
    decl : ParamDecl
        The declaration to resolve.
    cli_values : Sequence[str]
        Raw string values collected from the command line for this parameter,
        in the order given. Empty when the flag was not passed.
    config_values : dict[str, Any]
        The config ``[params]`` table.
    require : bool, optional
        When false, a required parameter that was not supplied resolves to
        ``None`` instead of raising. Used by read-only commands that import a
        workflow to describe it.

    Returns
    -------
    ParamResolution
        The resolved value and its source.
    """
    if cli_values:
        return ParamResolution(value=_value_from_cli(decl, cli_values), source="cli")

    if decl.name in config_values:
        raw = config_values[decl.name]
        return ParamResolution(value=_value_from_config(decl, raw), source="config")

    if decl.required:
        if not require:
            return ParamResolution(value=None, source="default")
        raise ParamError(
            f"Workflow parameter {decl.name!r} is required but was not supplied. "
            f"Pass {decl.flag} <value>, or set params.{decl.name} in the project config."
        )

    default = decl.default
    if decl.multiple and not isinstance(default, tuple):
        default = tuple(default) if _is_sequence(default) else (default,)
    return ParamResolution(value=default, source="default")


def extract_flag_values(
    extras: Sequence[str],
    decl: ParamDecl,
) -> tuple[list[str], set[int]]:
    """Collect one parameter's raw values from unparsed command-line tokens.

    Recognises ``--flag value``, ``--flag=value``, and for boolean parameters a
    bare ``--flag``. Returns the raw string values in the order given, plus the
    indices of every token consumed, so the caller can detect tokens that no
    declaration claimed.

    Parameters
    ----------
    extras : Sequence[str]
        Command-line tokens left unparsed by the main CLI parser.
    decl : ParamDecl
        The declaration to collect values for.

    Returns
    -------
    tuple[list[str], set[int]]
        The raw values, and the set of consumed indices into *extras*.
    """
    flag = decl.flag
    inline_prefix = f"{flag}="
    is_bool = decl.value_type is bool

    values: list[str] = []
    consumed: set[int] = set()
    index = 0
    while index < len(extras):
        token = extras[index]

        if token.startswith(inline_prefix):
            values.append(token[len(inline_prefix) :])
            consumed.add(index)
            index += 1
            continue

        if token != flag:
            index += 1
            continue

        consumed.add(index)
        following = extras[index + 1] if index + 1 < len(extras) else None
        takes_following = following is not None and not following.startswith("--")

        if is_bool:
            # A bare boolean flag means true; an explicit literal may follow.
            if takes_following and following.lower() in _TRUE_LITERALS | _FALSE_LITERALS:
                values.append(following)
                consumed.add(index + 1)
                index += 2
            else:
                values.append("true")
                index += 1
            continue

        if not takes_following:
            raise ParamError(f"{flag} expects a value")
        values.append(following)
        consumed.add(index + 1)
        index += 2

    return values, consumed


def format_param_help(decls: Iterable[ParamDecl]) -> list[str]:
    """Render declared parameters as aligned help lines.

    Parameters
    ----------
    decls : Iterable[ParamDecl]
        Declarations to render, in declaration order.

    Returns
    -------
    list[str]
        One line per parameter. Empty when there are no declarations.
    """
    ordered = list(decls)
    if not ordered:
        return []

    signatures = {decl.name: _help_signature(decl) for decl in ordered}
    width = max(len(signature) for signature in signatures.values())
    return [
        f"  {signatures[decl.name]:<{width}}  {_help_suffix(decl)}".rstrip() for decl in ordered
    ]


def _help_signature(decl: ParamDecl) -> str:
    """Return the flag-and-metavar portion of a parameter's help line."""
    if decl.value_type is bool:
        return decl.flag
    return f"{decl.flag} {decl.type_label.upper()}"


def _help_suffix(decl: ParamDecl) -> str:
    """Return the description-and-default portion of a parameter's help line."""
    parts: list[str] = []
    if decl.help_text:
        parts.append(decl.help_text)
    if decl.choices is not None:
        parts.append(f"(choices: {', '.join(str(item) for item in decl.choices)})")
    if decl.required:
        parts.append("(required)")
    else:
        parts.append(f"(default: {decl.default!r})")
    if decl.multiple:
        parts.append("(repeatable)")
    return " ".join(parts)


def _validate_declaration(decl: ParamDecl) -> None:
    """Reject declarations that could not produce a usable flag or config key."""
    if not _NAME_PATTERN.match(decl.name):
        raise ParamError(
            f"Invalid parameter name {decl.name!r}. Names must start with a letter and "
            "contain only letters, digits, and underscores."
        )
    if not callable(decl.value_type):
        raise ParamError(f"Parameter {decl.name!r} has a non-callable type: {decl.value_type!r}")
    if decl.choices is not None and not decl.choices:
        raise ParamError(f"Parameter {decl.name!r} declares an empty choices sequence")


def _resolve_without_session(decl: ParamDecl) -> Any:
    """Resolve a parameter declared outside any CLI session.

    Reached when a workflow module is imported directly rather than through the
    CLI, as in unit tests. Only the declared default is available.
    """
    if decl.required:
        raise ParamError(
            f"Workflow parameter {decl.name!r} is required, but no ginkgo run context is "
            "active to supply it. Give the parameter a default to import this module directly."
        )
    return resolve_param(decl, cli_values=(), config_values={}).value


def _value_from_cli(decl: ParamDecl, raw_values: Sequence[str]) -> Any:
    """Coerce and validate command-line values for one parameter."""
    if decl.multiple:
        return tuple(_check_choice(decl, _coerce(decl, raw)) for raw in raw_values)
    if len(raw_values) > 1:
        raise ParamError(
            f"{decl.flag} was given {len(raw_values)} times but is not declared "
            "multiple=True. Declare multiple=True to accept repeated values."
        )
    return _check_choice(decl, _coerce(decl, raw_values[0]))


def _value_from_config(decl: ParamDecl, raw: Any) -> Any:
    """Coerce and validate a config value for one parameter.

    Config formats carry their own scalar types, so a value is converted only
    when it arrives as a string and a different type was declared.
    """
    if decl.multiple:
        items = raw if _is_sequence(raw) else (raw,)
        return tuple(_check_choice(decl, _coerce_config_scalar(decl, item)) for item in items)
    if _is_sequence(raw):
        raise ParamError(
            f"Config value for params.{decl.name} is a sequence, but the parameter is not "
            "declared multiple=True."
        )
    return _check_choice(decl, _coerce_config_scalar(decl, raw))


def _coerce_config_scalar(decl: ParamDecl, raw: Any) -> Any:
    """Convert a config scalar only when it needs converting."""
    if isinstance(raw, str) and decl.value_type is not str:
        return _coerce(decl, raw)
    return raw


def _coerce(decl: ParamDecl, raw: str) -> Any:
    """Apply a parameter's declared type to a string value."""
    if decl.value_type is bool:
        return _parse_bool(decl, raw)
    try:
        return decl.value_type(raw)
    except (TypeError, ValueError) as exc:
        raise ParamError(
            f"Cannot read {raw!r} as {decl.type_label} for parameter {decl.name!r}: {exc}"
        ) from exc


def _parse_bool(decl: ParamDecl, raw: str) -> bool:
    """Parse a boolean literal, rejecting values ``bool()`` would silently accept."""
    lowered = raw.strip().lower()
    if lowered in _TRUE_LITERALS:
        return True
    if lowered in _FALSE_LITERALS:
        return False
    raise ParamError(
        f"Cannot read {raw!r} as a boolean for parameter {decl.name!r}. "
        "Use one of: true, false, 1, 0, yes, no, on, off."
    )


def _check_choice(decl: ParamDecl, value: Any) -> Any:
    """Reject a value outside a parameter's declared choices."""
    if decl.choices is not None and value not in decl.choices:
        permitted = ", ".join(repr(item) for item in decl.choices)
        raise ParamError(f"Value {value!r} for parameter {decl.name!r} is not one of: {permitted}")
    return value


def _is_sequence(value: Any) -> bool:
    """Whether *value* is a non-string sequence."""
    return isinstance(value, (list, tuple))


def _jsonable(value: Any) -> Any:
    """Render a declared value in a form ``json`` can serialise."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    return str(value)
