# Lineage and traces

**Lineage** answers “Which inputs produced this result?” A **trace** answers “What happened during this run, and in what order?” The same greeting can answer both questions.

Run these examples in order in a Python notebook.

```python
from strange_loop import Context, Loop, Version, flow, stage


@stage
def clean_name(text: str) -> str:
    return text.strip()


@stage
def greet(name: str) -> str:
    return f"Hello, {name}!"


@flow
async def make_greeting(ctx: Context, text: Version[str]) -> Version[str]:
    name = await clean_name(text=text)
    return await greet(name=name)


loop = Loop()
answer = await loop.run(make_greeting, text="  Alice  ")
```

## Follow a result back to its inputs

Start with the final greeting. `loop.inputs(answer)` returns a dictionary keyed by the argument names of the call that produced it. Here, `greet` has one argument, `name`. Its recorded input is the version produced by `clean_name`.

```python
parents = loop.inputs(answer)
print(list(parents))
parent = parents["name"]
print(parent.actor, repr(parent.value))
```

```text
['name']
clean_name 'Alice'
```

Take one more step back. The `text` input to `clean_name` holds the original string, including the spaces. The caller supplied it, so it has no earlier inputs.

```python
root = loop.inputs(parent)["text"]
print(root.actor, repr(root.value))
print(loop.inputs(root))
```

```text
<input> '  Alice  '
{}
```

## Read the full trace

`loop.trace()` shows events in chronological order. The middle column names the flow invocation. The last column tells you what happened.

```python
print(loop.trace())
```

```text
01  make_greeting  start
02  make_greeting  start clean_name()
03  make_greeting  finish clean_name() -> make_greeting/clean_name/1@1
04  make_greeting  start greet()
05  make_greeting  finish greet() -> make_greeting/greet/1@1
06  make_greeting  return make_greeting/greet/1@1
```

## Focus on one stage

Pass the decorated stage itself to see its events in the same order. The finish event identifies the exact version the stage produced.

```python
print(loop.trace(clean_name))
```

```text
start clean_name()
finish clean_name() -> make_greeting/clean_name/1@1
```

## Inspect a version from the trace

The trace supports filters for a stage, a flow, or an invocation key. To inspect a specific version, look up its full ID from the trace in `loop.versions`. You can then read its value or follow its inputs.

```python
recorded = loop.versions["make_greeting/clean_name/1@1"]
print(recorded is parent)
print(repr(loop.inputs(recorded)["text"].value))
```

```text
True
'  Alice  '
```

Both routes reach the same version. You found it first by following the greeting’s input and then by looking up the ID in the stage’s finish event.
