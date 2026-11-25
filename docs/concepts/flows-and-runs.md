# Flows and runs

A **flow** is a Python function that determines what your application does and in what order. You can call functions, branch with `if`, and choose which result to return. A **run** is one execution of a flow.

## Run a greeting flow

This flow chooses a greeting based on the name it receives. It prints the greeting and returns the original name. Strange Loop records the input as a [version](versions.md). Here, `Version[str]` holds a string, and `name.value` reads that string.

Run this example in a Python notebook.

```python
from strange_loop import Context, Loop, Version, flow


@flow
async def greet(ctx: Context, name: Version[str]) -> Version[str]:
    if name.value:
        print(f"Hello, {name.value}!")
    else:
        print("Hello, stranger!")
    return name


loop = Loop()
result = await loop.run(greet, name="Alice")
```

```text
Hello, Alice!
```

The `@flow` decorator marks `greet` as a flow. To start a run, pass the flow and its input to `loop.run()`. The loop supplies `ctx`, the context for this execution. This flow only needs the name, so its body can leave `ctx` unused.

## See what the loop recorded

A **trace** lists the events that the loop recorded during a run. Print it in the same notebook.

```python
print(loop.trace())
```

```text
01  greet  start
02  greet  return greet/name/1@1
```

The first event marks the start of the flow. The second gives the ID of the version it returned. You can read `greet/name/1@1` in four parts.

| Part | Meaning in this run |
| --- | --- |
| `greet` | The flow invocation that received the input. |
| `name` | The input parameter where the caller supplied `"Alice"`. |
| `/1` | The first **artifact** recorded under `greet/name`. An artifact is one tracked item that can have several versions. |
| `@1` | The first version of that artifact. A second version of the same artifact would end in `@2`. |

Here, `return name` returns the original input version, so `result.id` is `"greet/name/1@1"` and `result.value` is `"Alice"`. The greeting appears on screen because the flow called `print()`. Treat the full ID as opaque in application code. Its parts help you read the trace.

Use the ID from the trace to look up that version in this run.

```python
name = loop.versions["greet/name/1@1"]
print(name.value)
```

```text
Alice
```

A flow becomes more useful when it coordinates several pieces of work. In [Stages](stages.md), the flow calls a function that cleans a name, and Strange Loop records the result of that call.
