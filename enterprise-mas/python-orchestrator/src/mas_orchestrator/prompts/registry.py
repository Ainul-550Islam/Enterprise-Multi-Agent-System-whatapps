"""Versioned prompt registry.

Prompts are versioned for the same reason tools and agents are: a decision
taken last week must be explainable this week, after the wording has changed.

The lookup semantics are deliberately strict:

* a prompt is addressed by ``(name, version)``, never by name alone;
* a version, once registered, is immutable — changing wording creates a new
  version rather than rewriting history;
* asking for a prompt that does not exist raises rather than falling back to
  another version. A silent substitution here would change a model's behaviour
  without changing any record of what it was told.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Final

from mas_orchestrator.config.enums import Environment
from mas_orchestrator.contracts.errors import ConfigurationError, PromptRenderError
from mas_orchestrator.prompts.hashing import fingerprint_template

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

MAX_PROMPT_CHARS: Final[int] = 100_000


@dataclass(frozen=True, slots=True)
class PromptTemplate:
    """An immutable prompt template.

    Attributes:
        name: Stable identifier.
        version: Monotonic version, starting at 1.
        template: The template body.
        variables: Allowlisted variable names. Empty means the prompt takes
            no variables at all.
        description: Optional human-readable purpose.
        fingerprint: Stable hash of the template and its variable schema.
    """

    name: str
    version: int
    template: str
    variables: frozenset[str] = field(default_factory=frozenset)
    description: str = ""
    fingerprint: str = ""

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ConfigurationError("prompt name must not be empty", details={"field": "name"})
        if self.version < 1:
            raise ConfigurationError(
                "prompt version must be at least 1", details={"field": "version"}
            )
        if not self.template.strip():
            raise ConfigurationError(
                "prompt template must not be empty", details={"field": "template"}
            )
        if len(self.template) > MAX_PROMPT_CHARS:
            raise ConfigurationError(
                "prompt template exceeds the maximum size",
                details={"limit": MAX_PROMPT_CHARS, "name": self.name},
            )

        if not self.fingerprint:
            # Computed at construction so a template can never be registered
            # with a hash that does not match its body.
            computed = fingerprint_template(self.template, dict.fromkeys(sorted(self.variables)))
            object.__setattr__(self, "fingerprint", computed)

    @property
    def key(self) -> tuple[str, int]:
        """Registry key for this template."""
        return (self.name, self.version)


class PromptRegistry:
    """Holds immutable prompt versions."""

    def __init__(self, *, environment: Environment = Environment.DEVELOPMENT) -> None:
        self._environment = environment
        self._templates: dict[tuple[str, int], PromptTemplate] = {}
        self._current: dict[str, int] = {}

    def register(self, template: PromptTemplate) -> None:
        """Registers one version.

        Raises:
            ConfigurationError: If that exact version is already registered, or
                if the body was changed under an existing version number.
        """
        key = template.key
        existing = self._templates.get(key)
        if existing is not None:
            if existing.fingerprint != template.fingerprint:
                raise ConfigurationError(
                    "prompt version already registered with different content",
                    details={
                        "name": template.name,
                        "version": template.version,
                        "existing_fingerprint": existing.fingerprint,
                        "new_fingerprint": template.fingerprint,
                    },
                )
            return  # identical re-registration is idempotent

        self._templates[key] = template
        previous = self._current.get(template.name)
        if previous is None or template.version > previous:
            self._current[template.name] = template.version

        _LOGGER.info(
            "prompt registered",
            extra={
                "event": "prompt.registered",
                "name": template.name,
                "version": template.version,
                "fingerprint": template.fingerprint,
            },
        )

    def get(self, name: str, version: int | None = None) -> PromptTemplate:
        """Returns a template.

        Args:
            name: Prompt name.
            version: Specific version. ``None`` resolves the newest.

        Raises:
            PromptRenderError: If no such prompt exists. Deliberately not a
                silent fallback to another version.
        """
        number = version if version is not None else self._current.get(name)
        if number is None:
            raise PromptRenderError(f"prompt '{name}' is not registered", details={"name": name})
        template = self._templates.get((name, number))
        if template is None:
            raise PromptRenderError(
                f"prompt '{name}' has no version {number}",
                details={"name": name, "version": number},
            )
        return template

    def current_version(self, name: str) -> int | None:
        """Newest registered version, or ``None`` if unregistered."""
        return self._current.get(name)

    def versions(self, name: str) -> tuple[int, ...]:
        """All registered versions of a prompt, ascending."""
        return tuple(
            sorted(version for (prompt_name, version) in self._templates if prompt_name == name)
        )

    def names(self) -> tuple[str, ...]:
        """Every registered prompt name, sorted."""
        return tuple(sorted(self._current))

    def __len__(self) -> int:
        """Number of registered prompt versions."""
        return len(self._templates)


def create_prompt(
    name: str,
    template: str,
    *,
    variables: frozenset[str] | set[str] = frozenset(),
    description: str = "",
    version: int = 1,
) -> PromptTemplate:
    """Builds a template."""
    return PromptTemplate(
        name=name,
        version=version,
        template=template,
        variables=frozenset(variables),
        description=description,
    )


def next_version(
    previous: PromptTemplate,
    template: str,
    *,
    variables: frozenset[str] | set[str] | None = None,
    description: str | None = None,
) -> PromptTemplate:
    """Produces the next version of a prompt.

    Editing a prompt in place would invalidate every audit record that cites
    the old wording, so an edit is always a new version.
    """
    return PromptTemplate(
        name=previous.name,
        version=previous.version + 1,
        template=template,
        variables=(previous.variables if variables is None else frozenset(variables)),
        description=previous.description if description is None else description,
    )
