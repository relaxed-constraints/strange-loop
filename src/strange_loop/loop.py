"""Run, replay, checkpoint, and inspect a workflow execution."""

from __future__ import annotations

import asyncio
import json
from collections import Counter, defaultdict
from collections.abc import Mapping
from inspect import isawaitable
from pathlib import Path
from typing import Any
from urllib.parse import quote
from uuid import uuid4

from ._checkpoints import Checkpoints, outcome_record
from .batch import Accept, Batch, Pending
from .codecs import CapturedAttachments, Codec, Codecs, capture
from .flows import Context, Flow, MappedFlow, Stage, _active, _NestedFlow
from .versions import (
    Binding,
    Bindings,
    Collection,
    Failure,
    MapResult,
    Replacement,
    Revise,
    RevisionRequest,
    StaleVersion,
    TraceEvent,
    Version,
    _StaleAdoption,
    resolve,
)

type QueueKey = tuple[Stage, bool]


class _AttemptsExhausted(Exception):
    """An individual stage call used all its transient-error retries."""


class Loop:
    """One mapped invocation. Values and literal inputs in this prototype are immutable."""

    def __init__(
        self,
        *,
        attempts: int = 3,
        store: str | Path | None = None,
        codecs: tuple[Codec, ...] | list[Codec] = (),
        allow_pickle: bool = False,
    ) -> None:
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

        identity = (
            subject.identity
            if isinstance(subject, Stage)
            else (subject.__module__, subject.__qualname__)
            if callable(subject)
            else None
        )
        messages = [
            event.message
            for event in self._events
            if subject == event.key
            or (identity is not None and identity in (event.flow, event.stage))
        ]
        if not messages:
            raise KeyError(f"No trace recorded for {subject!r}")
        return "\n".join(messages)

    def record(self, key: str, message: str, stage: Stage | None = None) -> None:
        flow = self._flows.get(key)
        self._events.append(
            TraceEvent(
                key,
                message,
                (flow.__module__, flow.__qualname__) if flow else None,
                stage.identity if stage else None,
            )
        )
        self._checkpoint()

    def _track_items(
        self, items: Collection, path: str, name: str, actor: str, inputs: Bindings
    ) -> Collection:
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
        self,
        artifact: str,
        value: object,
        actor: str,
        inputs: Bindings,
        adopted_from: str | None = None,
        request: Revise | None = None,
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
        attachments = CapturedAttachments(
            {
                k: capture(v, self._codecs, persistent=persistent)
                for k, v in attachments.items()
            }
        )
        ref = Version._recorded(
            f"{artifact}@{number}",
            artifact,
            snapshot,
            actor,
            inputs,
            previous.id if previous else None,
            adopted_from,
            request,
            attachments,
        )
        self.versions[ref.id] = self.current[artifact] = ref
        if self.store:
            self._new_versions.append(ref)
        return ref

    async def invoke(
        self,
        node: Stage,
        key: str,
        values: dict[str, object],
        accept: Accept,
        *,
        request: RevisionRequest | None = None,
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
                    self.record(
                        key,
                        f"{node.name}() attempt {attempt}: {type(error).__name__}",
                        stage=node,
                    )
                    if attempt == self.attempts:
                        raise _AttemptsExhausted(
                            f"{node.name}(): {type(error).__name__}"
                        ) from error
                    continue
                except Exception as error:
                    self.record(
                        key, f"{node.name}() failed: {type(error).__name__}", stage=node
                    )
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
                self.record(
                    "batch", f"retry {node.name}(): {', '.join(missing)}", stage=node
                )
        for _, _, _, future in missing.values():
            future.set_exception(
                _AttemptsExhausted(f"{node.name}() exhausted {self.attempts} attempts")
            )

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

    def _start(
        self,
        flow: Flow,
        inputs: Mapping[str, object],
        mapped: bool,
        max_revisions: int,
        run_id: str | None,
    ) -> None:
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
                self._roots[key] = self._track_items(
                    value,
                    key if mapped else flow.__name__,
                    "source" if mapped else key,
                    "<input>",
                    (),
                )
                continue
            artifact = (
                self.new_artifact(key, "source")
                if mapped
                else self.new_artifact(flow.__name__, key)
            )
            self._roots[key] = self.save(artifact, value, "<input>", ())
        self.status = "incomplete"
        self._checkpoint()

    async def run(
        self,
        flow: Flow,
        *,
        max_revisions: int = 3,
        run_id: str | None = None,
        **inputs: object,
    ) -> Version | Failure | MapResult:
        self._start(flow, inputs, False, max_revisions, run_id)
        return await self._execute(flow)

    async def map(
        self,
        flow: MappedFlow,
        *,
        source: Mapping[str, object],
        max_revisions: int = 3,
        run_id: str | None = None,
    ) -> MapResult:
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
        self.result = (
            MapResult((key, self._finished[key]) for key in self._roots)
            if self._mapped
            else single
        )
        self.status = "completed"
        self._checkpoint()
        return self.result

    async def _item(
        self,
        flow: Flow,
        key: str,
        *args: Version | Failure,
        **kwargs: Version | Failure,
    ) -> Version | Failure | MapResult:
        invocation = (
            key,
            tuple(
                v.id
                if isinstance(v, Version)
                else json.dumps(outcome_record(v), sort_keys=True)
                for v in (*args, *kwargs.values())
            ),
        )
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

    def _require_current(
        self, key: str, target: Version, evidence: Version | str
    ) -> None:
        self._check_version(target)
        current = self.current[target.artifact]
        if current is not target:
            raise _StaleAdoption(
                StaleVersion(
                    key,
                    f"{target.id} was superseded by {current.id}",
                    target=target,
                    current=current,
                    evidence=evidence,
                )
            )

    async def _evaluate(
        self,
        flow: Flow,
        key: str,
        *args: Version | Failure,
        **kwargs: Version | Failure,
    ) -> Version | Failure | MapResult:
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
                label = (
                    f"{len(result)} mapped items"
                    if isinstance(result, MapResult)
                    else result.id
                )
                self.record(key, f"return {label}")
                return result
            if iteration == self.max_revisions:
                evidence = (
                    result.feedback if isinstance(result, Revise) else result.source
                )
                return Failure(
                    key,
                    f"revision budget exhausted ({self.max_revisions})",
                    "revision_budget_exhausted",
                    result.target,
                    evidence,
                )
            target = result.target
            evidence = result.feedback if isinstance(result, Revise) else result.source
            self._require_current(key, target, evidence)
            if isinstance(result, Revise):
                await self._revise(key, result, iteration)
                continue

            self._iterations[key] = iteration + 1
            fixed = result.source
            revised = self.save(
                target.artifact, fixed.value, flow.__name__, (), adopted_from=fixed.id
            )
            self.record(
                key,
                f"{flow.__name__} replaces {target.id} -> {revised.id} from {fixed.id}",
            )

    async def _revise(self, key: str, result: Revise, iteration: int) -> None:
        target = result.target
        node, inputs, bindings = self.producers[target.artifact]
        feedback = result.feedback
        message = feedback.value if isinstance(feedback, Version) else feedback
        self.record(
            key,
            f"request {node.name}() revision of {target.id} with feedback {message!r}",
            stage=node,
        )
        if not node.revisable:
            raise ValueError(f"{node.name} does not support revision requests")

        def accept(value: object) -> Version:
            self._require_current(key, target, feedback)
            self._iterations[key] = iteration + 1
            revised = self.save(
                target.artifact, value, node.name, bindings, request=result
            )
            self.record(key, f"finish {node.name}() revise -> {revised.id}", stage=node)
            return revised

        values = {k: resolve(v) for k, v in inputs.items()}
        await self.invoke(
            node,
            key,
            values,
            accept,
            request=RevisionRequest(target.value, message, target),
        )
