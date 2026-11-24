# Revising and replacing

Use `ctx.revise` when the producer should try again. Use replacement when another stage already has the corrected value. Both operations target an exact version.

Run this example in a Python notebook.

```python
from strange_loop.replay import Context, Loop, RevisionRequest, Revise, Replacement, Version, stage

@stage(revisable=True)
def write(text: str, *, request: RevisionRequest | None) -> str:
    if request is None:
        return text
    return request.previous.replace("teh", "the")

@stage
def correct(text: str) -> str:
    return text.replace("teh", "the")

async def proofread(ctx: Context, text: Version, external: Version) -> Version | Revise | Replacement:
    draft = await write(text=text)
    if draft.value == "teh":
        if external.value:
            fixed = await correct(text=draft)
            return ctx.replace(draft, with_=fixed)
        return ctx.revise(draft, feedback="Transpose the last two letters.")
    return draft

for external in (False, True):
    result = await Loop().run(proofread, text="teh", external=external)
    print(result.value, result.adopted_from)
```

Return the instruction to end the current pass. The producer receives its original inputs plus the previous value and feedback. Passing a verdict handle as feedback preserves its origin. Replacement records the supplying version in adopted_from. Both versions keep a previous link.
