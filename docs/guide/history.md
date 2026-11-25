# Execution traces and version history

Use the trace to see which calls ran. Follow version links to see who produced a value, who adopted it, and what inputs they saw.

Run this example in a Python notebook.

```python
from strange_loop import Context, Loop, Replacement, Version, stage


@stage
def draft(text: str) -> str:
    return text


@stage
def trim(text: str) -> str:
    return text.strip()


async def clean(ctx: Context, text: Version) -> Version | Replacement:
    result = await draft(text=text)
    if result.value != result.value.strip():
        fixed = await trim(text=result)
        return ctx.replace(result, with_=fixed)
    return result


loop = Loop()
final = await loop.run(clean, text=" cat ")
old = loop.versions[final.previous]
proposal = loop.versions[final.adopted_from]
print(old.actor, proposal.actor, final.actor)  # draft trim clean
print(loop.input(proposal, "text") is old)  # True
```

A producer revision instead stores its request on final.request, including the feedback version if supplied. Use `print(loop.trace())` for a numbered chronological log across all items. Use `loop.trace(key)` for the events from one invocation in the same order. Both the trace and version history live in memory.
