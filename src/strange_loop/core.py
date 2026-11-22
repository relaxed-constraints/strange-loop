"""Serial, revision-aware workflows over JSON artifacts."""

import inspect
import json
import sqlite3
import uuid
from collections.abc import Callable
from contextvars import ContextVar
from copy import copy
from dataclasses import dataclass
from typing import Any

_active = ContextVar("invocation")


@dataclass(frozen=True)
class Ref:
    """Identify one immutable revision in a store.

    Attributes:
        id: SQLite revision identifier within the originating store.
    """
    id: int


@dataclass(frozen=True)
class Repair:
    """Request another attempt from a producer.

    Args:
        target: Revision of a declared repairable input.
        feedback: Evidence or instructions for the next attempt.
    """
    target: Ref
    feedback: str


@dataclass(frozen=True)
class Pass:
    """Accept the exact input revisions inspected by a grader."""


class Keyed(dict):
    """Group independent JSON inputs by stable string ID for mapped calls.

    Construct this mapping with the public name `keyed`. Each key becomes
    a separate logical item and retains its own output revision."""


keyed = Keyed


class Store:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute("""CREATE TABLE IF NOT EXISTS revisions
            (id INTEGER PRIMARY KEY, slot TEXT, value TEXT, bindings TEXT)""")

    def save(self, slot, value, bindings):
        with self.db:
            row = self.db.execute(
                "INSERT INTO revisions(slot,value,bindings) VALUES(?,?,?)",
                (slot, json.dumps(value, allow_nan=False), json.dumps(bindings)),
            )
        return Ref(row.lastrowid)

    def record(self, ref):
        row = self.db.execute(
            "SELECT value,bindings FROM revisions WHERE id=?", (ref.id,)
        ).fetchone()
        if row is None:
            raise KeyError(ref)
        return json.loads(row[0]), json.loads(row[1])

    def read(self, ref) -> Any:
        """Read the value of a revision.

        Args:
            ref (Ref): Reference from this store.

        Returns:
            The decoded JSON value.
        """
        return self.record(ref)[0]

    def inputs(self, ref) -> dict[str, Ref]:
        """Find the direct inputs to a revision.

        Args:
            ref (Ref): Reference from this store.

        Returns:
            A mapping from argument names to input references.
        """
        return {k: Ref(v) for k, v in self.record(ref)[1].items()}

    def consumers(self, ref) -> list[Ref]:
        """Find revisions that directly consumed an input.

        Args:
            ref (Ref): Reference from this store.

        Returns:
            A list of consumer references, including recorded grader verdicts.
        """
        return [
            Ref(i)
            for (i,) in self.db.execute(
                "SELECT DISTINCT revisions.id FROM revisions, json_each(bindings) WHERE json_each.value=?",
                (ref.id,),
            )
        ]


async def resolve(value):
    return await value if inspect.isawaitable(value) else value


class Node:
    def __init__(self, fn, kind, repairable=()):
        self.fn, self.kind, self.name = fn, kind, fn.__name__
        self.repairable = repairable

    async def __call__(self, **kwargs):
        return await self._invoke(kwargs, False)

    async def map(self, **kwargs) -> Keyed:
        """Call this node once per key inside a flow.

        Args:
            **kwargs (Any): Named input references. At least one collection must be
                keyed. All keyed inputs must have identical keys.

        Returns:
            A keyed collection of results.
        """
        return await self._invoke(kwargs, True)

    async def _invoke(self, kwargs, mapped):
        ctx = _active.get()
        if not mapped:
            rows = {"": kwargs}
        else:
            groups = [v for v in kwargs.values() if isinstance(v, Keyed)]
            if not groups or any(g.keys() != groups[0].keys() for g in groups):
                raise ValueError("map requires aligned keyed inputs")
            rows = {
                k: {n: v[k] if isinstance(v, Keyed) else v for n, v in kwargs.items()}
                for k in groups[0]
            }
        path = ctx.child(self.name)
        results = await ctx.runner.execute(self, path, rows)
        return Keyed(results) if mapped else results.get("")


def stage(fn) -> Node:
    """Declare a Python producer.

    Args:
        fn (Callable): Synchronous or async function that accepts decoded inputs and
            returns a JSON value.

    Returns:
        A callable node that supports direct and mapped calls inside a flow.
    """
    return Node(fn, "stage")


def flow(fn) -> Node:
    """Declare a workflow that composes stages and child flows.

    Args:
        fn (Callable): Async function that receives a context and input references.

    Returns:
        A workflow node suitable for a runner or an enclosing flow.
    """
    return Node(fn, "flow")


def grader(*, repairable) -> Callable:
    """Declare a check on specific input revisions.

    Args:
        repairable (tuple[str, ...]): Names of inputs the grader may target for repair.

    Returns:
        A decorator for a function accepting a context and decoded values.
            The function must return a Pass or Repair verdict.
    """
    return lambda fn: Node(fn, "grader", repairable)


@dataclass(frozen=True)
class Batch:
    """Group independent agent calls into one backend turn.

    Args:
        max_items: Positive upper limit on items per turn.
    """
    max_items: int

    def __post_init__(self):
        if self.max_items < 1:
            raise ValueError("max_items must be positive")


class AgentStage(Node):
    """Generate JSON outputs with the configured agent backend.

    Args:
        name (str): Stable stage name within its enclosing flow.
        instructions (str): Prompt shared by the items in one attempt. CodexBackend renders
            named inputs such as {subject} using their current values.
        repair_instructions (str | None): Replacement prompt for a direct repair.
            When omitted, the stage uses its original instructions.
        output (type): Exact built-in type expected for each JSON output.
        batching (Batch | None): Optional limit on the number of items per backend turn.
    """
    def __init__(self, *, name, instructions, output, batching=None, repair_instructions=None):
        self.repair_instructions = repair_instructions
        self.name, self.instructions, self.output = name, instructions, output
        if output not in (str, int, float, bool, dict, list):
            raise ValueError("output must be a JSON type")
        self.kind, self.batch_size = "agent", batching.max_items if batching else 1


class Context:
    def __init__(self, runner, path, args):
        self.runner, self.path, self.args = runner, path, args
        self.counts = {}

    def child(self, name):
        count = self.counts.get(name, 0)
        self.counts[name] = count + 1
        return (*self.path, name, count)

    def ref(self, name) -> Ref:
        """Identify an input revision of the current invocation.

        Args:
            name (str): Input argument name.

        Returns:
            The original input reference.
        """
        return self.args[name]


class Runner:
    """Execute serial workflows and store their revisions.

    Args:
        backend (object): Adapter that generates and submits agent outputs.
        store (str | Path): SQLite file path or the default in-memory database.
        max_repairs (int): Maximum number of replay rounds after the first round.
        attempts (int): Maximum backend attempts per batch, including the first.
    """
    def __init__(self, *, backend, store=":memory:", max_repairs=12, attempts=3):
        if attempts < 1 or max_repairs < 0:
            raise ValueError("invalid retry or repair budget")
        self.store = Store(store)
        self.backend, self.attempts, self.max_repairs = backend, attempts, max_repairs
        self.pending = {}
        self.cache = {}
        self.running = False

    async def run(self, workflow, **kwargs) -> Any:
        """Start a new workflow invocation.

        Args:
            workflow (Node): Root workflow declared with the flow decorator.
            **kwargs (Any): Named JSON inputs or keyed collections of inputs.

        Returns:
            The value returned by the workflow after its checks pass.

        Raises:
            RuntimeError: The runner is busy or the repair budget is exhausted.
        """
        if self.running:
            raise RuntimeError("Runner already executing")
        self.running = True
        try:
            return await self._run(workflow, kwargs)
        finally:
            self.running = False

    async def _run(self, workflow, kwargs):
        self.pending.clear()
        self.cache.clear()
        path = (uuid.uuid4().hex, workflow.name)

        def bind(name, value):
            return self.store.save(json.dumps((*path, name)), value, {})

        args = {
            n: Keyed({k: bind((n, k), x) for k, x in v.items()})
            if isinstance(v, Keyed)
            else bind(n, v)
            for n, v in kwargs.items()
        }
        for _ in range(self.max_repairs + 1):
            self.repairs = []
            out = await self.call_flow(workflow, path, args)
            if not self.repairs:
                return out
            for target, feedback in self.repairs:
                (slot,) = self.store.db.execute(
                    "SELECT slot FROM revisions WHERE id=?", (target.id,)
                ).fetchone()
                if slot in self.pending:
                    feedback = self.pending[slot][1] + "\n" + feedback
                self.pending[slot] = (target, feedback)
        raise RuntimeError("repair budget exhausted")

    async def call_flow(self, node, path, args):
        ctx = Context(self, path, args)
        token = _active.set(ctx)
        try:
            return await node.fn(ctx, **args)
        finally:
            _active.reset(token)

    async def execute(self, node, path, rows):
        results, jobs = {}, {}
        for key, args in rows.items():
            slot = json.dumps((*path, key))
            if node.kind == "flow":
                results[key] = await self.call_flow(node, (*path, key), args)
                continue
            bindings = {n: r.id for n, r in args.items()}
            prior = self.cache.get(slot)
            repair = self.pending.pop(slot, None)
            if prior and not repair and self.store.record(prior)[1] == bindings:
                results[key] = prior
                continue
            if node.kind == "agent":
                jobs[key] = (slot, bindings, self.packet(args, repair))
                continue
            ctx = Context(self, path, args)
            values = {n: self.store.read(r) for n, r in args.items()}
            value = await resolve(
                node.fn(ctx, **values) if node.kind == "grader" else node.fn(**values)
            )
            if node.kind == "grader":
                if isinstance(value, Repair):
                    if value.target not in [args[n] for n in node.repairable]:
                        raise ValueError("undeclared repair route")
                    self.store.save(
                        slot,
                        {"repair": value.target.id, "feedback": value.feedback},
                        bindings,
                    )
                    self.repairs.append((value.target, value.feedback))
                    continue
                if not isinstance(value, Pass):
                    raise ValueError("expected Pass or Repair")
                value = "pass"
            results[key] = self.cache[slot] = self.store.save(slot, value, bindings)
        if jobs:
            # A changed upstream input can cause ordinary work and direct repairs
            # to become ready together. Each backend turn uses one prompt.
            groups = {}
            for key, job in jobs.items():
                entry, _ = job[2]
                prompt = node.instructions
                if "previous" in entry and node.repair_instructions is not None:
                    prompt = node.repair_instructions
                groups.setdefault(prompt, {})[key] = job

            for prompt, group in groups.items():
                attempt_node = node
                if prompt != node.instructions:
                    attempt_node = copy(node)
                    attempt_node.instructions = prompt
                await self.generate(attempt_node, group, results)
        return results

    def packet(self, args, repair):
        roots = list(args.values())
        packet = {"inputs": {k: v.id for k, v in args.items()}}
        if repair:
            old, feedback = repair
            roots.append(old)
            packet.update(previous=old.id, feedback=feedback)
        records = {}
        while roots:
            ref = roots.pop()
            if ref.id not in records:
                value, inputs = self.store.record(ref)
                records[ref.id] = {"value": value, "inputs": inputs}
                roots.extend(Ref(i) for i in inputs.values())
        return packet, records

    async def generate(self, node, jobs, results):
        keys = list(jobs)
        for offset in range(0, len(keys), node.batch_size):
            missing = dict.fromkeys(keys[offset : offset + node.batch_size])
            for attempt in range(self.attempts):
                entries, lineage = {}, {}
                for key in missing:
                    entries[key], records = jobs[key][2]
                    lineage.update(records)

                def submit(key, value):
                    if key not in entries or type(value) is not node.output:
                        raise ValueError("unexpected key or output type")
                    if key not in missing:
                        if self.store.read(results[key]) == value:
                            return
                        raise ValueError("conflicting duplicate submission")
                    slot, bindings, _ = jobs[key]
                    results[key] = self.cache[slot] = self.store.save(
                        slot, value, bindings
                    )
                    del missing[key]

                try:
                    await self.backend.generate(node, entries, lineage, submit)
                except (ConnectionError, TimeoutError):
                    if missing and attempt + 1 == self.attempts:
                        raise
                if not missing:
                    break
            if missing:
                raise RuntimeError(f"missing submissions: {list(missing)}")
