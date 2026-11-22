# Repair or replace individual items

Use `ctx.candidates` when each input needs a verified output and some inputs can fail independently.
A **slot** is one input key. A **candidate** is one attempt to fill that slot.
A repair revises a stage output within the same candidate. A replacement starts the loop body again with the original input for that slot.

## Define the work

Suppose each slot needs a short label. A label must fit within 12 characters.
A reserved label disqualifies the candidate immediately.

```python
from strange_loop import AgentStage, Batch, Fail, Pass, Repair, flow, grader, keyed

write_label = AgentStage(
    name="write_label",
    instructions="Write a short label for each brief.",
    output=str,
    batching=Batch(8),
    max_repairs=2,
)

@grader(repairable=("label",))
def check_label(ctx, *, label):
    if label.casefold() == "admin":
        return Fail("This label is reserved.")
    if len(label) > 12:
        return Repair(ctx.ref("label"), "Use at most 12 characters.")
    return Pass()
```

## Set the recovery policy

```python
@flow
async def labels(ctx, *, briefs):
    async with ctx.candidates(briefs, max_candidates=2, on_failure="replace") as slots:
        async for pending in slots:
            drafts = await write_label.map(brief=pending)
            checks = await check_label.map(label=drafts)
            slots.accept(checks.passed("label"))
    return slots.report()
```

Each iteration follows these steps.

1. The loop supplies the keys that still need work. The first iteration includes every key.
2. The grader checks each new draft. A repair request sends the previous draft and feedback to the producer.
3. The runner reuses earlier outputs whose inputs still match. A new revision causes dependent checks to run again.
4. `checks.passed("label")` selects the label references for the keys that passed. Pass this collection to the next stage, or call `accept` at the last stage.
5. The loop removes accepted keys from subsequent iterations. It continues until each slot has an output or a terminal failure.

When a later stage needs two keyed inputs, select the same keys from both collections.
For example, `briefs.select(valid_labels)` selects the original briefs for the keys in `valid_labels`.

## Choose the limits

- `max_repairs=2` allows the initial output and two repairs for this producer in each candidate. Multiple graders that target the same output share this budget.
- If the last revision needs another repair, the runner records a failure with reason `repair_limit`. A grader can also return `Fail(message)` at any check.
- `on_failure="replace"` starts a fresh candidate for the failed key. Put each stage that must produce fresh work inside the loop.
- `max_candidates=2` includes the original candidate. The replacement gets its own repair budget. If both candidates fail, the slot ends with a failure.
- Use the default `on_failure="stop"` when a failed slot must end immediately. Other slots continue.

A candidate can repair a producer inside its own slot. Keep shared inputs outside this recovery policy.
If a producer omits its repair limit, the candidate uses the runner's `max_repairs` value for that producer.
Backend interruptions use the separate `attempts` limit. An unhandled backend exception still ends the run.

## Inspect the result

```python
report = await runner.run(labels, briefs=keyed(home="Home page", help="Help desk"))

for key, ref in report.outputs.items():
    print(key, runner.store.read(ref))

for key, failure in report.failures.items():
    print(key, failure.grader, failure.reason, failure.feedback)

for candidate in report.history["home"]:
    print(candidate.number, candidate.status, candidate.repairs)
    if candidate.failure:
        print(runner.store.inputs(candidate.failure.verdict))
```

The report includes failed candidates even when a replacement succeeds. Only terminal failures appear in `report.failures`.
The report lives in memory. The store retains each verdict and the revisions that the grader checked.

## Reuse work during a workflow replay

An outer grader can request a repair after the candidate loop ends.
The runner then enters the same loop again and checks the input revisions for each call.
It resumes each slot at its latest candidate and retains its repair counts.

For example, a producer can use a style guide to write labels.
A grader can use a separate length limit to check them.

- If an unrelated input changes, the runner reuses the label and its verdict.
- If only the length limit changes, the runner checks the existing label again.
- If the style guide changes, the producer writes a new label. The grader checks that revision.

Pass each dependency as an argument to the stage or grader that uses it.
For external data, pass a snapshot identifier if changes must trigger another check.
Reuse applies within one call to `runner.run`. A new run starts with an empty cache.
