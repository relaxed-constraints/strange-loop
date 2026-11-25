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
        if isinstance(self._value, (EncodedValue, MemoryValue)):
            return self._value.read()
        return (
            self._value.value if isinstance(self._value, StoredValue) else self._value
        )

    @property
    def attachments(self) -> Mapping[str, object]:
        return self._attachments


@dataclass(frozen=True)
class Replacement:
    target: Version
    source: Version


@dataclass(frozen=True)
class Revise:
    target: Version
    feedback: Version | str


@dataclass(frozen=True)
class RevisionRequest:
    previous: object
    feedback: object
    target: Version | None = field(default=None, compare=False)


class Members(dict[str, Version]):
    """Ordered keyed version handles returned by a mapped child flow."""


@dataclass(frozen=True)
class Failure:
    key: str
    reason: str
    kind: str = "explicit"
    target: Version | None = None
    evidence: Version | str | None = None
    causes: Mapping[str, Failure] | None = None


@dataclass(frozen=True, kw_only=True)
class StaleVersion(Failure):
    """The target was superseded. current is the winner observed at rejection."""

    current: Version
    kind: str = "stale_version"


class _StaleAdoption(Exception):
    def __init__(self, conflict: StaleVersion):
        self.conflict = conflict
        super().__init__(conflict.reason)


class Collection(dict):
    """Keyed items, tracked individually at the input or stage-output boundary."""

    @property
    def succeeded(self) -> Collection:
        return Collection((k, v) for k, v in self.items() if isinstance(v, Version))

    @property
    def failed(self) -> Collection:
        return Collection((k, v) for k, v in self.items() if isinstance(v, Failure))

    def raise_for_failures(self) -> None:
        if failures := self.failed:
            raise RuntimeError(
                ", ".join(f"{k}: {v.reason}" for k, v in failures.items())
            )


MapResult = Collection


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
