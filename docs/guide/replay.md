# Replay and reuse

After a correction the issuing flow re-enters from the top. Calls with matching identities and input versions resolve to their current outputs. Changed dependencies require fresh work.

Run this example in a Python notebook.

```python
from strange_loop.replay import Context, Loop, RevisionRequest, Revise, Version, stage

@stage(revisable=True)
def positive(number: int, *, request: RevisionRequest | None) -> int:
    return number if request is None else abs(request.previous)

@stage
def valid(number: int) -> bool:
    return number > 0

async def ensure_positive(ctx: Context, number: Version) -> Version | Revise:
    candidate = await positive(number=number)
    verdict = await valid(number=candidate)
    if not verdict.value:
        return ctx.revise(candidate, feedback="Use the absolute value.")
    return candidate

loop = Loop()
await loop.run(ensure_positive, number=-3)
print(loop.trace(ensure_positive))
```

Look for `replay`, then `reuse positive()`, `finish valid()`, and `return`. The checker saw a new version. The prototype identifies repeated stage calls by occurrence within an invocation, so keep call order stable where you expect reuse.
