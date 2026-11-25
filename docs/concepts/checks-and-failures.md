# Checks and failures

## Failures

A **Failure** is an outcome that explains what went wrong. Like a **Version**, it can be returned by a flow and inspected by the caller. Suppose a flow reviews a draft reply to a customer. It can return the draft if it passes review, or a failure if it needs a correction.

A Failure contains the following information:

- **Reason** (`reason`) is a plain text string explaining why the review failed.
- **Target** (`target`) is an optional reference to the version that failed review, such as the draft reply.
- **Evidence** (`evidence`) can point to another version, such as the spelling suggestion that identified the defect.

Below is a demonstration of how Failures are used in a flow.


## Check a draft

A check is an ordinary stage. This example simulates a spell checker for one known typo. It returns a suggestion when it finds “reciept” and `None` otherwise. Run the examples in order in a notebook.

```python
from strange_loop import Context, Failure, Loop, Version, flow, stage


@stage
def check_spelling(text: str) -> str | None:
    # simulate a spell checker
    if "reciept" in text:
        return "Change 'reciept' to 'receipt'."
    return None
```

Finding a typo means the check did its job. The flow decides whether that finding makes the draft unacceptable.

## Return the outcome

Use `ctx.fail()` to construct a failure from a reason, the draft version, and the recorded finding. Returning that failure makes it the outcome of this review.

```python
@flow
async def review_reply(ctx: Context, draft: Version[str]) -> Version[str] | Failure:
    finding = await check_spelling(text=draft)
    if finding.value is None:
        return draft

    return ctx.fail("The reply needs a spelling correction.", target=draft, evidence=finding)
```

## Inspect the outcome

With the correct spelling, the flow returns the draft version.

```python
accepted = await Loop().run(review_reply, draft="I’ve attached your receipt.")
print(accepted.value)
```

```text
I’ve attached your receipt.
```

With the typo, the caller receives a failure. Its evidence links the suggestion to the exact draft the checker read.

```python
loop = Loop()
outcome = await loop.run(review_reply, draft="I’ve attached your reciept.")
if isinstance(outcome, Failure):
    print(f"Reason for failure: {outcome.reason}")
    print(f"The draft that failed: {outcome.target.value}")
    print(f"Evidence from the check: {outcome.evidence.value}")
    print(
        "The `text` input to the check_spelling() is the same as the Failure target:",
        loop.input(outcome.evidence, "text") is outcome.target,
    )
```

```text
Reason for failure: The reply needs a spelling correction.
The draft that failed: I’ve attached your reciept.
Evidence from the check: Change 'reciept' to 'receipt'.
The `text` input to the check_spelling() is the same as the Failure target: True
```

This flow returns a failure for an expected quality problem. An unexpected exception in the checker, such as a `TypeError`, propagates to the caller.
