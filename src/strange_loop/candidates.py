"""Item-level repair and replacement within a flow."""

from copy import deepcopy
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .core import Ref


@dataclass(frozen=True)
class Fail:
    """Close the current candidate.

    Args:
        reason: Explanation from the grader.
    """

    reason: str


@dataclass(frozen=True)
class Failure:
    """Describe the check that closed a candidate.

    Attributes:
        reason: Either explicit_failure or repair_limit.
        grader: Name of the grader that issued the verdict.
        feedback: Explanation from the grader.
        verdict: Stored verdict with links to the inputs the grader inspected.
    """

    reason: str
    grader: str
    feedback: str
    verdict: "Ref"


@dataclass
class Candidate:
    """Record one attempt to fill a slot.

    Attributes:
        number: Candidate number, starting at one.
        repairs: Number of repairs executed by producer name.
        failure: Failure details for a closed candidate.
        status: Current state (pending, accepted, or failed).
    """

    number: int
    repairs: dict[str, int] = field(default_factory=dict)
    failure: Failure | None = None
    status: str = "pending"


@dataclass
class CandidateReport:
    """Collect accepted output references, terminal failures, and per-slot history."""

    outputs: dict[str, "Ref"] = field(default_factory=dict)
    failures: dict[str, Failure] = field(default_factory=dict)
    history: dict[str, list[Candidate]] = field(default_factory=dict)


class Candidates:
    """Run a bounded sequence of candidates for each input key."""

    def __init__(self, ctx, items, max_candidates, on_failure):
        from .core import Keyed

        if not isinstance(items, Keyed):
            raise TypeError("candidates requires keyed inputs")
        if max_candidates < 1 or on_failure not in ("replace", "stop"):
            raise ValueError("invalid candidate limit or failure policy")
        self.ctx, self.items = ctx, items
        self.limit, self.policy = max_candidates, on_failure
        self.result = CandidateReport(history={key: [Candidate(1)] for key in items})
        self.active = dict.fromkeys(items)
        self.targets = {}
        self.requests = set()
        self.started = False
        self.finished = False

    async def __aenter__(self):
        from .core import Context, _active

        path = self.ctx.child("candidates")
        runner = self.ctx.runner
        self.body = Context(runner, path, self.ctx.args)
        self.counts = runner.candidate_counts
        self.saved = runner.candidate_history.setdefault(path, {})
        for key, ref in self.items.items():
            previous = self.saved.get((key, ref))
            if previous is not None:
                self.result.history[key] = deepcopy(previous)
                # Replay the current candidate. Earlier candidates are closed.
                current = self.current(key)
                current.status = "pending"
                current.failure = None
        self.body.scope = self
        self.token = _active.set(self.body)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        from .core import _active

        _active.reset(self.token)
        if exc_type is None and not self.finished:
            raise RuntimeError("candidate loop ended before all slots finished")

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.started:
            pending = {}
            for key in self.active:
                candidate = self.current(key)
                if candidate.failure:
                    if self.policy == "replace" and candidate.number < self.limit:
                        self.result.history[key].append(Candidate(candidate.number + 1))
                        pending[key] = None
                    else:
                        self.result.failures[key] = candidate.failure
                elif key in self.requests:
                    pending[key] = None
                elif key not in self.result.outputs:
                    raise RuntimeError(f"candidate {key!r} needs acceptance or a verdict")
            self.active = pending
        self.started = True
        if not self.active:
            for key, history in self.result.history.items():
                self.saved[key, self.items[key]] = history
            self.finished = True
            raise StopAsyncIteration
        self.requests.clear()
        self.body.counts.clear()
        return self.items.select(self.active)

    def current(self, key):
        return self.result.history[key][-1]

    def reject(self, key, grader, reason, feedback, verdict):
        candidate = self.current(key)
        self.result.outputs.pop(key, None)
        candidate.status = "failed"
        candidate.failure = Failure(reason, grader, feedback, verdict)

    def request(self, key, grader, repair, verdict):
        target = self.targets.get(repair.target)
        if target is None or target[:2] != (key, self.current(key).number):
            raise ValueError("candidate repair must target a producer of the same candidate")
        _, _, slot, limit = target
        if self.counts.get(slot, 0) >= limit:
            self.reject(key, grader, "repair_limit", repair.feedback, verdict)
            return
        self.result.outputs.pop(key, None)
        self.requests.add(key)
        pending = self.ctx.runner.pending
        feedback = repair.feedback
        if slot in pending:
            feedback = pending[slot][1] + "\n" + feedback
        pending[slot] = (repair.target, feedback)

    def accept(self, outputs):
        """Accept output references for keys that passed their final check."""
        from .core import Ref

        for key, ref in outputs.items():
            if key not in self.active or not isinstance(ref, Ref):
                raise ValueError("accept requires active keys and output references")
            if self.current(key).failure or key in self.requests:
                raise ValueError(f"candidate {key!r} has an unresolved verdict")
            self.result.outputs[key] = ref
            self.current(key).status = "accepted"

    def report(self) -> CandidateReport:
        """Return the report after all slots finish."""
        if not self.finished:
            raise RuntimeError("candidate loop has not finished")
        return self.result
