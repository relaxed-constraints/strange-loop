# Build a flow

A stage produces a value. Strange Loop stores that value as a revision. A flow connects stages by passing references to their revisions.

## Define a Python stage

Use `@stage` for work that Python can do directly. The function receives ordinary Python values. Return a value that Python can encode as JSON.

```python
from strange_loop import CodexBackend, Runner, flow, stage

@stage
def clean(text):
    return text.strip()

@stage
def word_count(text):
    return len(text.split())
```

## Connect the stages

Use `@flow` on an async function. Give the function a context parameter first. Use keyword arguments for the inputs to each stage.

```python
@flow
async def count_words(ctx, *, text):
    cleaned = await clean(text=text)
    count = await word_count(text=cleaned)
    return count
```

Here, `cleaned` refers to the revision from `clean`. The runner records that revision as an input to `word_count`. Keep the order of calls stable when a flow runs again after a repair.

## Run the flow

Pass the flow and its inputs to `Runner.run`. Read the result from the runner's store.

```python
import asyncio

async def main():
    runner = Runner(backend=CodexBackend())
    count = await runner.run(count_words, text="  two words  ")
    print(runner.store.read(count))  # 2

asyncio.run(main())
```

This flow uses only Python stages. The backend starts a Codex agent when a flow calls an agent stage.

## Call a child flow

Call a child flow the same way you call a stage. The child can return a reference from any stage it calls.

```python
@flow
async def count_title(ctx, *, title):
    return await count_words(text=title)
```

The caller receives the reference to the count. The store retains the link from that revision to the cleaned text.
