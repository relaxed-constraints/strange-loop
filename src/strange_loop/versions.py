"""Versioned values, keyed outcomes, and their recorded input bindings."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

from ._checkpoints import StoredValue
from .codecs import Codecs, EncodedValue, MemoryValue

type Binding = (
    tuple[Literal["ref"], str]
    | tuple[Literal["members"], tuple[tuple[str, str], ...]]
    | tuple[Literal["encoded"], str, bytes]
    | tuple[Literal["literal"], type, object]
)
type Bindings = tuple[tuple[str, Binding], ...]


@dataclass(frozen=True)
class Version[T]:
    """A value with optional attachments and a recorded history.

    Args:
        value: Output value to record.
        attachments: Named auxiliary values such as an agent conversation.

    Attributes:
        id: Full version ID, assigned when the loop records the value.
        artifact: Stable ID shared by successive versions of the same result.
        actor: Stage, flow, or input label responsible for this version.
        inputs: Recorded input bindings. Use Loop.inputs to resolve them.
        previous: ID of the superseded version, if present.
        adopted_from: ID of the version that supplied a replacement.
        request: Revision instruction associated with the correction, if present.
    """
    _value: T | StoredValue | EncodedValue | MemoryValue
    _attachments: Mapping[str, object]
    id: str | None = None
    artifact: str | None = None
    actor: str | None = None
    inputs: Bindings = ()
    previous: str | None = None
    adopted_from: str | None = None
    request: Revise | None = None

    def __init__(self, value: T, *, attachments: Mapping[str, object] | None = None):
        object.__setattr__(self, "_value", value)
        object.__setattr__(self, "_attachments", dict(attachments or {}))

    @classmethod
    def _recorded(
        cls,
        id,
        artifact,
        value,
        actor,
        inputs,
        previous,
        adopted_from,
        request,
        attachments,
    ):
        ref = cls(value)
        for key, item in (
            ("id", id),
            ("artifact", artifact),
            ("actor", actor),
            ("inputs", inputs),
            ("previous", previous),
            ("adopted_from", adopted_from),
            ("request", request),
            ("_attachments", attachments),
        ):
            object.__setattr__(ref, key, item)
        return ref

    @property
    def value(self) -> T:
        """Read the payload associated with this version."""
        if isinstance(self._value, (EncodedValue, MemoryValue)):
            return self._value.read()
        return (
            self._value.value if isinstance(self._value, StoredValue) else self._value
        )

    @property
    def attachments(self) -> Mapping[str, object]:
        """Auxiliary values indexed by attachment name."""
        return self._attachments


@dataclass(frozen=True)
class Replacement:
    """Instruction to adopt a recorded correction.

    Args:
        target: Version to supersede.
        source: Recorded version that supplies the new value.
    """
    target: Version
    source: Version


@dataclass(frozen=True)
class Revise:
    """Instruction to ask a producer for a correction.

    Args:
        target: Version to revise.
        feedback: Plain feedback text or a version containing the finding.
    """
    target: Version
    feedback: Version | str


@dataclass(frozen=True)
class RevisionRequest:
    """Revision context passed to the producer on a revision attempt.

    Args:
        previous: Value of the target before the requested change.
        feedback: Resolved feedback value.
        target: Recorded target, including its attachments when available.
    """
    previous: object
    feedback: object
    target: Version | None = field(default=None, compare=False)


class Members(dict[str, Version]):
    """A dictionary of recorded versions indexed by member key."""


@dataclass(frozen=True)
class Failure:
    """An unsuccessful flow outcome with optional lineage.

    Args:
        key: Flow invocation key.
        reason: Human-readable explanation.
        kind: Category such as explicit failure or exhausted revision budget.
        target: Version that the failure concerns.
        evidence: Finding or text that explains the failure.
        causes: Related child failures by key.
    """
    key: str
    reason: str
    kind: str = "explicit"
    target: Version | None = None
    evidence: Version | str | None = None
    causes: Mapping[str, Failure] | None = None


@dataclass(frozen=True, kw_only=True)
class StaleVersion(Failure):
    """Failure returned when another correction has already superseded the target.

    Attributes:
        current: Winning version observed when the loop rejected the stale correction.
    """

    current: Version
    kind: str = "stale_version"


class _StaleAdoption(Exception):
    def __init__(self, conflict: StaleVersion):
        self.conflict = conflict
        super().__init__(conflict.reason)


class Collection(dict):
    """A dictionary of keyed items. The loop tracks each item individually."""

    @property
    def succeeded(self) -> Collection:
        """A collection containing the Version entries in their original order."""
        return Collection((k, v) for k, v in self.items() if isinstance(v, Version))

    @property
    def failed(self) -> Collection:
        """A collection containing the Failure entries in their original order."""
        return Collection((k, v) for k, v in self.items() if isinstance(v, Failure))

    def raise_for_failures(self) -> None:
        """Check whether any item has a failure outcome.

        Raises:
            RuntimeError: One or more items failed. The message lists their keys and reasons.
        """
        if failures := self.failed:
            raise RuntimeError(
                ", ".join(f"{k}: {v.reason}" for k, v in failures.items())
            )


MapResult = Collection
"""Alias for Collection used for the keyed outcomes of a map call."""


def binding(value: object, codecs: Codecs | None = None) -> Binding:
    if isinstance(value, Version):
        return ("ref", value.id)
    if isinstance(value, MapResult):
        value.raise_for_failures()
    if isinstance(value, (Collection, Members)):
        return ("members", tuple((key, ref.id) for key, ref in value.items()))
    if codecs is not None:
        name, data = codecs.encode(value)
        return ("encoded", name, data)
    return ("literal", type(value), value)


def resolve(value: object) -> object:
    if isinstance(value, Version):
        return value.value
    if isinstance(value, MapResult):
        value.raise_for_failures()
    if isinstance(value, (Collection, Members)):
        return {key: ref.value for key, ref in value.items()}
    return value


@dataclass(frozen=True)
class TraceEvent:
    key: str
    message: str
    flow: tuple[str, str] | None
    stage: tuple[str, str] | None
