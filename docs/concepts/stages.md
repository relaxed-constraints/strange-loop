# Stages

A **stage** is a Python function whose calls Strange Loop tracks. When a stage produces a value, its caller receives a [version](versions.md) that links the result to its inputs.

## Inside the stage

Add `@stage` to a function that does one piece of work. Here, `clean_name` removes spaces from the beginning and end of a name.

```python
from strange_loop import stage


@stage
def clean_name(text: str) -> str:
    return text.strip()
```

Given `"  Alice  "`, the function receives a string with spaces and returns the string `"Alice"`. The type hint `-> str` describes the value returned by the function body.

## What the caller receives

Run the stage definition above in a notebook. Then call the stage from a flow. The flow passes its recorded input to `clean_name`. Strange Loop gives the stage function the underlying string.

```python
from strange_loop import Context, Loop, Version, flow


@flow
async def prepare_name(ctx: Context, text: Version[str]) -> Version[str]:
    output = await clean_name(text=text)
    return output


result = await Loop().run(prepare_name, text="  Alice  ")
print(type(result).__name__, repr(result.value), result.actor)
```

```text
Version 'Alice' clean_name
```

The function body returned `"Alice"`. Its caller received a `Version[str]` and returned that version from the flow. You can read the cleaned string through `.value` and see who produced it through `.actor`.

Continue to [Lineage and traces](lineage-and-traces.md) to follow a stage result back to its inputs.
