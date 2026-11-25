"""Stage and flow declarations and the context of one flow invocation."""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Awaitable, Callable, Mapping
from contextvars import ContextVar
from functools import update_wrapper
from inspect import Parameter, signature
from typing import TYPE_CHECKING, overload
from urllib.parse import quote

from .batch import BatchFunction
from .versions import (
    Collection,
    Failure,
    MapResult,
    Replacement,
    Revise,
    Version,
    binding,
    resolve,
)

if TYPE_CHECKING:
    from .loop import Loop


type FlowResult = Version | Failure | MapResult | Replacement | Revise
type Flow = Callable[..., Awaitable[FlowResult]]
type MappedFlow = Callable[[Context, Version], Awaitable[FlowResult]]
type StageFunction = Callable[..., object]


_active: ContextVar[Context] = ContextVar("replay_context")


class Stage:
    """Callable stage created by the stage and batch_stage decorators."""

    def __init__(
        self, fn: StageFunction, batched: bool = False, revisable: bool = False
    ) -> None:
        self.fn = fn
        self.name = fn.__name__
        self.identity = (fn.__module__, fn.__qualname__)
        self.batched = batched
        params = signature(fn).parameters
        request = params.get("request")
        self.revisable = revisable
        if (
            revisable
            and not batched
            and (request is None or request.kind != Parameter.KEYWORD_ONLY)
        ):
            raise TypeError(
                "@stage(revisable=True) requires a keyword-only request parameter"
            )

    async def __call__(self, **inputs: object) -> Version:
        """Invoke the stage in the active flow.

        Args:
            **inputs: Named stage inputs, including versions or literal values.

        Returns:
            The recorded output. A regular non-revisable stage can also return a Collection.

        Raises:
            LookupError: The stage was called outside a flow.
            TypeError: The caller supplied the runtime-managed revision request.
        """
        if self.revisable and not self.batched and "request" in inputs:
            raise TypeError("Loop supplies the request parameter")
        return await _active.get().call(self, inputs)


@overload
def batch_stage(fn: BatchFunction, *, revisable: bool = False) -> Stage: ...


@overload
def batch_stage(
    fn: None = None, *, revisable: bool = False
) -> Callable[[BatchFunction], Stage]: ...


def batch_stage(
    fn: BatchFunction | None = None,
    *,
    revisable: bool = False,
) -> Stage | Callable[[BatchFunction], Stage]:
    """Group pending stage calls into a batch attempt.

    Args:
        fn: Function that receives a Batch. Return a dictionary keyed by item,
            or submit individual results and return None.
        revisable: Allow revision attempts. Each item carries its own request.

    Returns:
        A callable stage, or a decorator when used with options.
    """

    def decorate(function: BatchFunction) -> Stage:
        return Stage(function, batched=True, revisable=revisable)

    return decorate if fn is None else decorate(fn)


@overload
def stage(fn: StageFunction, *, revisable: bool = False) -> Stage: ...


@overload
def stage(
    fn: None = None, *, revisable: bool = False
) -> Callable[[StageFunction], Stage]: ...


def stage(
    fn: StageFunction | None = None,
    *,
    revisable: bool = False,
) -> Stage | Callable[[StageFunction], Stage]:
    """Declare a synchronous or asynchronous stage.

    Args:
        fn: Function that receives the resolved input values.
        revisable: Let the producer revise its output. Its function must accept
            a keyword-only `request` parameter.

    Returns:
        A callable stage, or a decorator when used with options.

    Raises:
        TypeError: A revisable function lacks the required keyword-only parameter.
    """

    def decorate(function: StageFunction) -> Stage:
        return Stage(function, revisable=revisable)

    return decorate if fn is None else decorate(fn)


@stage
def member(collection: Mapping[str, object], key: str) -> object:
    return collection[key]


# Give the built-in projection a stable identity across internal module moves.
member.identity = ("strange_loop", "member")


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
    """Declare an async flow with its own replay boundary.

    Args:
        body: Async function that accepts a Context followed by flow inputs.

    Returns:
        A flow suitable for Loop.run, mapping, or a nested call.
    """
    return _NestedFlow(body)


class Context:
    """Operations available inside one flow invocation. The loop supplies this object."""

    def __init__(self, run: Loop, key: str) -> None:
        self.run = run
        self.key = key
        self.occurrences: Counter[Stage] = Counter()
        self.maps = 0
        self.children = 0

    def fail(
        self,
        reason: str,
        *,
        target: Version | None = None,
        evidence: Version | None = None,
        causes: Mapping[str, Failure] | None = None,
    ) -> Failure:
        """Construct a failure outcome for this invocation.

        Args:
            reason: Human-readable reason for the failure.
            target: Version that failed a check.
            evidence: Version that detected the defect.
            causes: Keyed child failures that contributed to this failure.

        Returns:
            A failure for the flow to return.
        """
        return Failure(
            self.key, reason, target=target, evidence=evidence, causes=causes
        )

    def replace(self, target: Version, *, with_: Version) -> Replacement:
        """Adopt a supplied correction when the flow returns this instruction.

        Args:
            target: Version to supersede.
            with_: Recorded version that supplies the corrected value.

        Returns:
            A replacement instruction for the loop.
        """
        return Replacement(target, with_)

    def revise(self, target: Version, *, feedback: Version | str) -> Revise:
        """Ask a revisable stage to produce a new version.

        Args:
            target: Version produced by the revisable stage.
            feedback: Feedback text or a recorded finding for the producer.

        Returns:
            A revision instruction for the flow to return.
        """
        return Revise(target, feedback)

    async def map(
        self,
        flow: Flow | _NestedFlow,
        **inputs: Version[Mapping[str, object]] | Mapping[str, Version | Failure],
    ) -> MapResult:
        """Run a child flow for each item in one keyed input.

        Args:
            flow: Child flow to invoke.
            **inputs: One parameter mapping keys to versions or failures, or a version
                containing a keyed collection. The keyword names the child parameter.

        Returns:
            A collection of child outcomes in input order.

        Raises:
            TypeError: The caller supplied multiple inputs, no input, or a keyed
                outcome other than a version or failure.
        """
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
        """Resolve a stage call within this invocation.

        Args:
            node: Stage to call.
            inputs: Named arguments supplied by the caller.

        Returns:
            A reused or newly recorded output, or the items of a Collection output.

        Raises:
            TypeError: A revisable or batched stage returned a Collection.
        """
        ordinal = self.occurrences[node]
        self.occurrences[node] += 1
        bindings = tuple(
            sorted(
                (name, binding(v, self.run.store.codecs if self.run.store else None))
                for name, v in inputs.items()
            )
        )
        slot = (self.key, node.identity, ordinal, bindings)
        run = self.run
        if slot in run._collections:
            items = run._latest(run._collections[slot])
            run.record(
                self.key, f"reuse {node.name}() -> {len(items)} items", stage=node
            )
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
                    raise TypeError(
                        "Collection outputs currently require a regular non-revisable stage"
                    )
                items = run._track_items(
                    value, self.key, node.name, node.name, bindings
                )
                run._collections[slot] = items
                run.record(
                    self.key, f"finish {node.name}() -> {len(items)} items", stage=node
                )
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
