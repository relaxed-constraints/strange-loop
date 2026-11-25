"""Checkpointable, single-run prototype for per-item replay and version lineage."""

from __future__ import annotations

import asyncio
import json
from collections import Counter, defaultdict
from collections.abc import Awaitable, Callable, Iterator, Mapping
from contextvars import ContextVar
from dataclasses import dataclass, field
from functools import update_wrapper
from inspect import Parameter, isawaitable, signature
from pathlib import Path
from typing import Any, Literal, overload
from urllib.parse import quote
from uuid import uuid4

from ._checkpoints import Checkpoints, StoredValue, outcome_record
from .codecs import Codec, Codecs, CustomCodec, FileCodec, EncodedValue, MemoryValue, CapturedAttachments, capture

type Binding = (
    tuple[Literal["ref"], str]
    | tuple[Literal["members"], tuple[tuple[str, str], ...]]
    | tuple[Literal["encoded"], str, bytes]
    | tuple[Literal["literal"], type, object]
)
type Bindings = tuple[tuple[str, Binding], ...]
type FlowResult = Version | Failure | MapResult | Replacement | Revise
type Flow = Callable[..., Awaitable[FlowResult]]
type MappedFlow = Callable[[Context, Version], Awaitable[FlowResult]]
type StageFunction = Callable[..., object]
type BatchResult = dict[str, Any] | None
type BatchFunction = Callable[[Batch], BatchResult | Awaitable[BatchResult]]
type Accept = Callable[[object], Version]
type Deliver = Callable[[str, object], None]
type Pending = tuple[dict[str, object], RevisionRequest | None, Accept, asyncio.Future[Version]]
type QueueKey = tuple[Stage, bool]

_active: ContextVar[Context] = ContextVar("replay_context")


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
    def _recorded(cls, id, artifact, value, actor, inputs, previous, adopted_from, request, attachments):
        ref = cls(value)
        for key, item in (("id", id), ("artifact", artifact), ("actor", actor), ("inputs", inputs),
                          ("previous", previous), ("adopted_from", adopted_from), ("request", request),
                          ("_attachments", attachments)):
            object.__setattr__(ref, key, item)
        return ref

    @property
    def value(self) -> T:
        if isinstance(self._value, (EncodedValue, MemoryValue)):
            return self._value.read()
        return self._value.value if isinstance(self._value, StoredValue) else self._value

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


@dataclass(frozen=True)
class BatchItem:
    key: str
    inputs: dict[str, Any]
    request: RevisionRequest | None
    _batch: "Batch"

    async def submit(self, value: object) -> None:
        """Accept a result for this item before returning to the stage."""
        self._batch._accept(self.key, value)


class BatchContractError(ValueError):
    """A stage used an invalid batch result or submission."""


class Batch:
    """The items scheduled together for one execution attempt."""

    def __init__(self, pending: Mapping[str, Pending], accept: Deliver) -> None:
        self._items = {
            key: BatchItem(key, inputs, request, self)
            for key, (inputs, request, _, _) in pending.items()
        }
        self._deliver: Deliver | None = accept
        self._accepted: dict[str, object] = {}

    def __iter__(self) -> Iterator[BatchItem]:
        return iter(self._items.values())

    def __len__(self) -> int:
        return len(self._items)

    def __getitem__(self, key: str) -> BatchItem:
        return self._items[key]

    def _accept(self, key: str, value: object) -> None:
        if self._deliver is None:
            raise BatchContractError("This batch attempt has ended.")
        if key in self._accepted:
            if self._accepted[key] != value:
                raise BatchContractError(f"Conflicting submissions for {key!r}.")
            return
        self._deliver(key, value)
        self._accepted[key] = value

    def _finish(self, outputs: object) -> None:
        if outputs is None:
            return
        if not isinstance(outputs, dict):
            raise BatchContractError("Return a keyed dictionary or submit items and return None.")
        if self._accepted:
            raise BatchContractError(
                "This batch stage submitted an item and also returned a dictionary. "
                "Submit the remaining items and return None, or return all results in one dictionary."
            )
        for key in outputs:
            if key not in self._items:
                raise BatchContractError(f"Unexpected submission for {key!r}.")
        for key, value in outputs.items():
            self._accept(key, value)

    def _close(self) -> None:
        self._deliver = None


class Stage:
    def __init__(self, fn: StageFunction, batched: bool = False, revisable: bool = False) -> None:
        self.fn = fn
        self.name = fn.__name__
        self.identity = (fn.__module__, fn.__qualname__)
        self.batched = batched
        params = signature(fn).parameters
        request = params.get("request")
        self.revisable = revisable
        if revisable and not batched and (request is None or request.kind != Parameter.KEYWORD_ONLY):
            raise TypeError("@stage(revisable=True) requires a keyword-only request parameter")

    async def __call__(self, **inputs: object) -> Version:
        if self.revisable and not self.batched and "request" in inputs:
            raise TypeError("Loop supplies the request parameter")
        return await _active.get().call(self, inputs)


@overload
def batch_stage(fn: BatchFunction, *, revisable: bool = False) -> Stage: ...


@overload
def batch_stage(fn: None = None, *, revisable: bool = False) -> Callable[[BatchFunction], Stage]: ...


def batch_stage(
    fn: BatchFunction | None = None, *, revisable: bool = False,
) -> Stage | Callable[[BatchFunction], Stage]:
    """Pass a Batch to a function that returns keyed results or submits items."""
    def decorate(function: BatchFunction) -> Stage:
        return Stage(function, batched=True, revisable=revisable)

    return decorate if fn is None else decorate(fn)


@overload
def stage(fn: StageFunction, *, revisable: bool = False) -> Stage: ...


@overload
def stage(fn: None = None, *, revisable: bool = False) -> Callable[[StageFunction], Stage]: ...


def stage(
    fn: StageFunction | None = None, *, revisable: bool = False,
) -> Stage | Callable[[StageFunction], Stage]:
    """A revisable producer receives a runtime-supplied keyword argument, request."""
    def decorate(function: StageFunction) -> Stage:
        return Stage(function, revisable=revisable)

    return decorate if fn is None else decorate(fn)


@stage
def member(collection: Mapping[str, object], key: str) -> object:
    return collection[key]


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
            raise RuntimeError(", ".join(f"{k}: {v.reason}" for k, v in failures.items()))


MapResult = Collection


class _AttemptsExhausted(Exception):
    """An individual stage call used all its transient-error retries."""


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


class _NestedFlow:
    def __init__(self, body: Flow) -> None:
        update_wrapper(self, body)
        self.body = body

    async def __call__(self, **inputs: Version) -> Version | Failure | MapResult:
        ctx = _active.get()
        ctx.children += 1
        key = f"{ctx.key}/call{ctx.children}-{self.__name__}"
        return await ctx.run._item(self, key, **inputs)


def flow(body: Flow) -> _NestedFlow:
    """Make a flow callable from another flow with its own replay boundary."""
    return _NestedFlow(body)


class Context:
    def __init__(self, run: Loop, key: str) -> None:
        self.run = run
        self.key = key
        self.occurrences: Counter[Stage] = Counter()
        self.maps = 0
        self.children = 0

    def fail(self, reason: str, *, target: Version | None = None,
             evidence: Version | None = None, causes: Mapping[str, Failure] | None = None) -> Failure:
        return Failure(self.key, reason, target=target, evidence=evidence, causes=causes)

    def replace(self, target: Version, *, with_: Version) -> Replacement:
        return Replacement(target, with_)

    def revise(self, target: Version, *, feedback: Version | str) -> Revise:
        return Revise(target, feedback)

    async def map(self, flow: Flow | _NestedFlow,
                  **inputs: Version[Mapping[str, object]] | Mapping[str, Version | Failure]) -> MapResult:
        """Map a versioned collection or keyed outcomes, preserving input order."""
        if len(inputs) != 1:
            raise TypeError("ctx.map expects one keyed input")
        name, source = next(iter(inputs.items()))
        self.maps += 1
        prefix = f"{self.key}/map{self.maps}-{flow.__name__}"

        async def child(key: str) -> Version | Failure:
            path = f"{prefix}/{quote(key, safe='')}"
            if isinstance(source, Version):
                token = _active.set(Context(self.run, path))
                try:
                    child_input = await member(collection=source, key=key)
                finally:
                    _active.reset(token)
            else:
                child_input = source[key]
                if not isinstance(child_input, (Version, Failure)):
                    raise TypeError("A keyed outcome must be a Version or Failure")
            if name == "source" and not isinstance(flow, _NestedFlow):
                return await self.run._item(flow, path, child_input)
            return await self.run._item(flow, path, **{name: child_input})

        items = source.value if isinstance(source, Version) else source
        async with asyncio.TaskGroup() as group:
            tasks = {key: group.create_task(child(key)) for key in items}
        return MapResult((key, task.result()) for key, task in tasks.items())

    async def call(self, node: Stage, inputs: dict[str, object]) -> Version:
        ordinal = self.occurrences[node]
        self.occurrences[node] += 1
        bindings = tuple(sorted(
            (name, binding(v, self.run.store.codecs if self.run.store else None))
            for name, v in inputs.items()
        ))
        slot = (self.key, node.identity, ordinal, bindings)
        run = self.run
        if slot in run._collections:
            items = run._latest(run._collections[slot])
            run.record(self.key, f"reuse {node.name}() -> {len(items)} items", stage=node)
            return items
        if slot in run.slots:
            ref = run.current[run.slots[slot]]
            run.producers[ref.artifact] = (node, inputs, bindings)
            run.record(self.key, f"reuse {node.name}() -> {ref.id}", stage=node)
            return ref

        values = {k: resolve(v) for k, v in inputs.items()}

        def accept(value: object) -> Version:
            if isinstance(value, Collection):
                if node.batched or node.revisable:
                    raise TypeError("Collection outputs currently require a regular non-revisable stage")
                items = run._track_items(value, self.key, node.name, node.name, bindings)
                run._collections[slot] = items
                run.record(self.key, f"finish {node.name}() -> {len(items)} items", stage=node)
                return run._latest(items)
            artifact = run.new_artifact(self.key, node.name)
            ref = run.save(artifact, value, node.name, bindings)
            run.producers[artifact] = (node, inputs, bindings)
            run.slots[slot] = artifact
            if run.store:
                run._new_slots[ref.id] = slot[:3]
            run.record(self.key, f"finish {node.name}() -> {ref.id}", stage=node)
            return ref

        return await run.invoke(node, self.key, values, accept)


@dataclass(frozen=True)
class TraceEvent:
    key: str
    message: str
    flow: tuple[str, str] | None
    stage: tuple[str, str] | None


class Loop:
    """One mapped invocation. Values and literal inputs in this prototype are immutable."""

    def __init__(self, *, attempts: int = 3, store: str | Path | None = None,
                 codecs: tuple[Codec, ...] | list[Codec] = (), allow_pickle: bool = False) -> None:
        if attempts < 1:
            raise ValueError("attempts must be positive")
        self._codecs = Codecs(codecs, allow_pickle)
        self.store = Checkpoints(store, self._codecs) if store is not None else None
        self.run_id: str | None = None
        self._generation = 0
        self._saved_events = self._saved_batches = 0
        self._new_versions: list[Version] = []
        self._new_slots: dict[str, tuple] = {}
        self._collections: dict[tuple, Collection] = {}
        self._roots: dict[str, Version] = {}
        self._finished: dict[str, Version | Failure] = {}
        self._failures: dict[tuple[str, tuple[str, ...]], Failure] = {}
        self._iterations: Counter[str] = Counter()
        self.result: Version | Failure | MapResult | None = None
        self.status = "new"
        self.attempts = attempts
        self.slots: dict[tuple[str, tuple[str, str], int, Bindings], str] = {}
        self.producers: dict[str, tuple[Stage, dict[str, object], Bindings]] = {}
        self.current: dict[str, Version] = {}
        self.versions: dict[str, Version] = {}
        self._flows: dict[str, Flow] = {}
        self._events: list[TraceEvent] = []
        self.batches: list[tuple[str, tuple[str, ...]]] = []
        self._pending: defaultdict[QueueKey, dict[str, Pending]] = defaultdict(dict)
        self._counts: Counter[tuple[str, str]] = Counter()

    def _resolve_binding(self, stored: Binding) -> object:
        if stored[0] == "ref":
            return self.versions[stored[1]]
        if stored[0] == "members":
            return {key: self.versions[ref_id] for key, ref_id in stored[1]}
        if stored[0] == "encoded":
            return self.store.codecs.decode(stored[1], stored[2])
        return stored[2]

    def _check_version(self, version: Version) -> None:
        if self.versions.get(version.id) is not version:
            raise ValueError("version belongs to a different Loop")

    def input(self, version: Version, name: str) -> object:
        """Return the recorded input by name. An unknown name raises KeyError."""
        self._check_version(version)
        return self._resolve_binding(dict(version.inputs)[name])

    def inputs(self, version: Version) -> dict[str, object]:
        """Resolve all named inputs to exact versions, keyed versions, or literals."""
        self._check_version(version)
        return {name: self._resolve_binding(value) for name, value in version.inputs}

    @property
    def events(self) -> list[tuple[str, str]]:
        """Chronological (invocation key, message) pairs."""
        return [(event.key, event.message) for event in self._events]

    def trace(self, subject: str | Flow | Stage | None = None) -> str:
        """Format the entire run, or select a flow, stage, or invocation."""
        if subject is None:
            return "\n".join(
                f"{number:02}  {event.key}  {event.message}"
                for number, event in enumerate(self._events, start=1)
            )

        identity = subject.identity if isinstance(subject, Stage) else (subject.__module__, subject.__qualname__) if callable(subject) else None
        messages = [
            event.message for event in self._events
            if subject == event.key or (identity is not None and identity in (event.flow, event.stage))
        ]
        if not messages:
            raise KeyError(f"No trace recorded for {subject!r}")
        return "\n".join(messages)

    def record(self, key: str, message: str, stage: Stage | None = None) -> None:
        flow = self._flows.get(key)
        self._events.append(TraceEvent(key, message, (flow.__module__, flow.__qualname__) if flow else None,
                                       stage.identity if stage else None))
        self._checkpoint()

    def _track_items(self, items: Collection, path: str, name: str, actor: str,
                     inputs: Bindings) -> Collection:
        tracked = Collection()
        for key, value in items.items():
            if not isinstance(key, str):
                raise TypeError("Collection keys must be strings")
            if isinstance(value, Version) and value.id is not None:
                self._check_version(value)
            elif not isinstance(value, Failure):
                artifact = self.new_artifact(f"{path}/{name}", quote(key, safe=""))
                value = self.save(artifact, value, actor, inputs)
            tracked[key] = value
        return tracked

    def _latest(self, value):
        if isinstance(value, Version):
            return self.current[value.artifact]
        if isinstance(value, Collection):
            return Collection((k, self._latest(v)) for k, v in value.items())
        return value

    def new_artifact(self, key: str, name: str) -> str:
        self._counts[key, name] += 1
        return f"{key}/{name}/{self._counts[key, name]}"

    def save(
        self, artifact: str, value: object, actor: str, inputs: Bindings,
        adopted_from: str | None = None, request: Revise | None = None,
    ) -> Version:
        previous = self.current.get(artifact)
        number = int(previous.id.rsplit("@", 1)[1]) + 1 if previous else 1
        attachments = self.versions[adopted_from].attachments if adopted_from else {}
        if isinstance(value, Version):
            if value.id is not None:
                raise ValueError("A stage cannot return an already recorded Version")
            attachments = value.attachments
            value = value.value
        persistent = self.store is not None
        snapshot = capture(value, self._codecs, persistent=persistent)
        attachments = CapturedAttachments({
            k: capture(v, self._codecs, persistent=persistent) for k, v in attachments.items()
        })
        ref = Version._recorded(f"{artifact}@{number}", artifact, snapshot, actor, inputs,
                       previous.id if previous else None, adopted_from, request, attachments)
        self.versions[ref.id] = self.current[artifact] = ref
        if self.store:
            self._new_versions.append(ref)
        return ref

    async def invoke(
        self, node: Stage, key: str, values: dict[str, object], accept: Accept,
        *, request: RevisionRequest | None = None,
    ) -> Version:
        if not node.batched:
            kwargs = dict(values)
            if node.revisable:
                kwargs["request"] = request
            for attempt in range(1, self.attempts + 1):
                self.record(key, f"start {node.name}()", stage=node)
                try:
                    value = node.fn(**kwargs)
                    if isawaitable(value):
                        value = await value
                except (TimeoutError, ConnectionError) as error:
                    self.record(key, f"{node.name}() attempt {attempt}: {type(error).__name__}", stage=node)
                    if attempt == self.attempts:
                        raise _AttemptsExhausted(f"{node.name}(): {type(error).__name__}") from error
                    continue
                except Exception as error:
                    self.record(key, f"{node.name}() failed: {type(error).__name__}", stage=node)
                    raise
                return accept(value)

        self.record(key, f"queued {node.name}()", stage=node)
        queue = (node, request is not None)
        pending = self._pending[queue]
        if not pending:
            self._group.create_task(self._dispatch(queue))
        future = asyncio.get_running_loop().create_future()
        if key in pending:
            raise ValueError(f"duplicate pending batch key: {key}")
        pending[key] = (values, request, accept, future)
        return await future

    async def _dispatch(self, queue: QueueKey) -> None:
        node, revising = queue
        missing = self._pending.pop(queue)
        kind = "revise" if revising else "draft"
        for attempt in range(1, self.attempts + 1):
            keys = tuple(missing)
            label = f"{node.name}() {kind} attempt {attempt}"
            self.batches.append((node.name, keys))
            self.record("batch", f"{label}: {', '.join(keys)}", stage=node)

            def deliver(key: str, value: object) -> None:
                _, _, accept, future = missing.pop(key)
                try:
                    version = accept(value)
                except _StaleAdoption as error:
                    future.set_exception(error)
                    return
                future.set_result(version)
                self.record("batch", f"accepted {version.id}", stage=node)

            batch = Batch(missing, deliver)
            try:
                outputs = node.fn(batch)
                if isawaitable(outputs):
                    outputs = await outputs
                batch._finish(outputs)
            except (TimeoutError, ConnectionError) as error:
                self.record("batch", f"{label}: {type(error).__name__}", stage=node)
            except Exception as error:
                self.record("batch", f"{label}: {type(error).__name__}", stage=node)
                raise
            finally:
                batch._close()
            if not missing:
                return
            if attempt < self.attempts:
                self.record("batch", f"retry {node.name}(): {', '.join(missing)}", stage=node)
        for _, _, _, future in missing.values():
            future.set_exception(_AttemptsExhausted(f"{node.name}() exhausted {self.attempts} attempts"))

    def _checkpoint(self) -> None:
        if self.store is not None and self.run_id is not None:
            self.store.write(self)

    def runs(self) -> list[str]:
        """List saved run IDs in creation order."""
        return self.store.keys() if self.store else []

    def load(self, run_id: str) -> Loop:
        """Open a saved run for inspection or resumption."""
        if self.store is None:
            raise ValueError("load requires a store")
        loaded = Loop()
        loaded.store = self.store
        loaded._codecs = self.store.codecs
        self.store.read(run_id, loaded)
        return loaded

    def _start(self, flow: Flow, inputs: Mapping[str, object], mapped: bool,
               max_revisions: int, run_id: str | None) -> None:
        if self.status != "new":
            raise ValueError("create a new Loop for each invocation")
        self.run_id = run_id if run_id is not None else uuid4().hex
        self._mapped = mapped
        self._flow_id = (flow.__module__, flow.__qualname__)
        if max_revisions < 0:
            raise ValueError("max_revisions must be nonnegative")
        self.max_revisions = max_revisions
        for key, value in inputs.items():
            if isinstance(value, Collection):
                self._roots[key] = self._track_items(value, key if mapped else flow.__name__,
                                                    "source" if mapped else key, "<input>", ())
                continue
            artifact = self.new_artifact(key, "source") if mapped else self.new_artifact(flow.__name__, key)
            self._roots[key] = self.save(artifact, value, "<input>", ())
        self.status = "incomplete"
        self._checkpoint()

    async def run(self, flow: Flow, *, max_revisions: int = 3, run_id: str | None = None,
                  **inputs: object) -> Version | Failure | MapResult:
        self._start(flow, inputs, False, max_revisions, run_id)
        return await self._execute(flow)

    async def map(self, flow: MappedFlow, *, source: Mapping[str, object], max_revisions: int = 3,
                  run_id: str | None = None) -> MapResult:
        self._start(flow, source, True, max_revisions, run_id)
        return await self._execute(flow)

    async def resume(self, flow: Flow) -> Version | Failure | MapResult:
        if self.status == "new":
            raise ValueError("load a run before resuming")
        if (flow.__module__, flow.__qualname__) != self._flow_id:
            raise ValueError("flow differs from the saved run")
        if self.result is not None:
            return self.result
        self.record("run", "resume")
        return await self._execute(flow)

    async def _execute(self, flow: Flow) -> Any:
        async def item(key: str, ref: Version) -> None:
            self._finished[key] = await self._item(flow, key, ref)
            self._checkpoint()

        async with asyncio.TaskGroup() as self._group:
            if self._mapped:
                for key, ref in self._roots.items():
                    if key not in self._finished:
                        self._group.create_task(item(key, ref))
            else:
                single = await self._item(flow, flow.__name__, **self._roots)
        self.result = MapResult((key, self._finished[key]) for key in self._roots) if self._mapped else single
        self.status = "completed"
        self._checkpoint()
        return self.result

    async def _item(self, flow: Flow, key: str, *args: Version | Failure, **kwargs: Version | Failure) -> Version | Failure | MapResult:
        invocation = (key, tuple(v.id if isinstance(v, Version) else json.dumps(outcome_record(v), sort_keys=True)
                                 for v in (*args, *kwargs.values())))
        if invocation in self._failures:
            return self._failures[invocation]
        try:
            result = await self._evaluate(flow, key, *args, **kwargs)
        except _StaleAdoption as error:
            result = error.conflict
        except _AttemptsExhausted as error:
            result = Failure(key, str(error), kind="attempt_budget_exhausted")
        if isinstance(result, Failure):
            self._failures[invocation] = result
            self.record(key, f"fail ({result.kind}): {result.reason}")
        return result

    def _require_current(self, key: str, target: Version, evidence: Version | str) -> None:
        self._check_version(target)
        current = self.current[target.artifact]
        if current is not target:
            raise _StaleAdoption(StaleVersion(key, f"{target.id} was superseded by {current.id}",
                                              target=target, current=current, evidence=evidence))

    async def _evaluate(self, flow: Flow, key: str, *args: Version | Failure, **kwargs: Version | Failure) -> Version | Failure | MapResult:
        self._flows[key] = flow
        for iteration in range(self._iterations[key], self.max_revisions + 1):
            self.record(key, "replay" if iteration else "start")
            token = _active.set(ctx := Context(self, key))
            try:
                # A replacement advances the input artifact while its original version
                # remains available to earlier calls and lineage inspection.
                body = flow.body if isinstance(flow, _NestedFlow) else flow
                current_args = [self._latest(v) for v in args]
                current_kwargs = {name: self._latest(v) for name, v in kwargs.items()}
                result = await body(ctx, *current_args, **current_kwargs)
            finally:
                _active.reset(token)
            if isinstance(result, Failure):
                return result
            if not isinstance(result, (Replacement, Revise)):
                self._iterations.pop(key, None)
                label = f"{len(result)} mapped items" if isinstance(result, MapResult) else result.id
                self.record(key, f"return {label}")
                return result
            if iteration == self.max_revisions:
                evidence = result.feedback if isinstance(result, Revise) else result.source
                return Failure(key, f"revision budget exhausted ({self.max_revisions})",
                               "revision_budget_exhausted", result.target, evidence)
            target = result.target
            evidence = result.feedback if isinstance(result, Revise) else result.source
            self._require_current(key, target, evidence)
            if isinstance(result, Revise):
                node, inputs, bindings = self.producers[target.artifact]
                feedback = result.feedback
                message = feedback.value if isinstance(feedback, Version) else feedback
                self.record(key,
                    f"request {node.name}() revision of {target.id} with feedback {message!r}", stage=node
                )
                if not node.revisable:
                    raise ValueError(f"{node.name} does not support revision requests")

                def accept(value: object) -> Version:
                    self._require_current(key, target, evidence)
                    self._iterations[key] = iteration + 1
                    revised = self.save(target.artifact, value, node.name, bindings, request=result)
                    self.record(key, f"finish {node.name}() revise -> {revised.id}", stage=node)
                    return revised

                values = {k: resolve(v) for k, v in inputs.items()}
                await self.invoke(node, key, values, accept, request=RevisionRequest(target.value, message, target))
                continue

            self._iterations[key] = iteration + 1
            fixed = result.source
            revised = self.save(target.artifact, fixed.value, flow.__name__, (),
                                adopted_from=fixed.id)
            self.record(key,
                f"{flow.__name__} replaces {target.id} -> {revised.id} from {fixed.id}"
            )
