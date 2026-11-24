# Checks and outcomes

A checker can return a score, label, or application-defined verdict. The flow assigns meaning to that value.

Run this example in a Python notebook.

```python
from strange_loop.replay import Context, Loop, Version, stage

@stage
def score(text: str) -> int:
    return len(set(text.split()))

@stage
def label(value: str) -> str:
    return value

async def assess(ctx: Context, text: Version) -> Version:
    diversity = await score(text=text)
    if diversity.value < 2:
        return await label(value="repetitive")
    return await label(value="varied")

print((await Loop().run(assess, text="hello hello")).value)  # repetitive
```

Reading a score in an if statement controls which stage runs. To record the score as an explicit dependency of a later stage, pass its version as a named input to that stage.
