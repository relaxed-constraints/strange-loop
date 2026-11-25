# Artifacts, versions, and dependencies

An artifact identifies a logical output. A version identifies one value of that output. Passing the version to a stage gives the stage its value and records its identity as an input.

Run this example in a Python notebook.

```python
from strange_loop.replay import Context, Loop, Version, stage

@stage
def normalize(text: str) -> str:
    return text.strip()

@stage
def length(text: str) -> int:
    return len(text)

async def measure(ctx: Context, text: Version) -> Version:
    cleaned = await normalize(text=text)
    return await length(text=cleaned)

loop = Loop()
size = await loop.run(measure, text="  cat ")
parents = loop.inputs(size)
print(parents["text"].value)  # cat
```

Passing `cleaned.value` instead would record the literal string as the input to length. Pass the handle when the origin matters. Treat version IDs as opaque.

A recorded version captures its value and attachments when the runtime accepts it.
You can edit a nested dictionary received by a stage, or reuse a buffer after
`await item.submit(buffer)`. Each consumer receives its own working value.
In memory, the loop deep-copies values or uses a registered codec. With SQLite,
it encodes the accepted value once and decodes it on access. Reading a
pickle-encoded value requires `allow_pickle=True` for a trusted run.
