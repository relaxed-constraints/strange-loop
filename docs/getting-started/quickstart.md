# Quickstart

Create a greeting, then trace the name in the greeting back to the exact text you supplied.

## Write the flow

A **stage** receives ordinary Python values. When you call it from a **flow**, use `await` to get a **version** of its result. Pass that version to the next stage so Strange Loop can record the connection.

In this example, `clean_name` removes extra spaces and `greet` formats the greeting. The `Loop` runs the flow and supplies its first argument, a `Context`. This flow only needs the name from the caller.

Save this script as `greeting.py`.

```python
import asyncio

from strange_loop import Context, Loop, Version, flow, stage


@stage
def clean_name(text: str) -> str:
    return text.strip()


@stage
def greet(name: str) -> str:
    return f"Hello, {name}!"


@flow
async def make_greeting(ctx: Context, text: Version) -> Version:
    name = await clean_name(text=text)
    return await greet(name=name)


async def main() -> None:
    loop = Loop()
    answer = await loop.run(make_greeting, text="  Alice  ")
    print(answer.value)

    parent = loop.input(answer, "name")
    root = loop.input(parent, "text")
    print(parent.actor, repr(parent.value))
    print(root.actor, repr(root.value))


if __name__ == "__main__":
    asyncio.run(main())
```

## Run the script

```sh
python greeting.py
```

You should see this output.

```text
Hello, Alice!
clean_name 'Alice'
<input> '  Alice  '
```

The first line is the final value. The next two lines show its **lineage**, the chain of inputs that produced it. `loop.input(answer, "name")` follows the `name` argument of `greet` back to the version produced by `clean_name`. Following its `text` argument reaches the original input, including the spaces. The label `<input>` identifies a value supplied by the caller.

## Read the execution trace

The **trace** lists the events in a run in chronological order. To see how the greeting came together, add this line at the end of `main()` and run the script again.

```python
    print(loop.trace())
```

Six more lines appear after the original output.

```text
01  make_greeting  start
02  make_greeting  start clean_name()
03  make_greeting  finish clean_name() -> make_greeting/clean_name/1@1
04  make_greeting  start greet()
05  make_greeting  finish greet() -> make_greeting/greet/1@1
06  make_greeting  return make_greeting/greet/1@1
```

- Line 01 marks the start of the flow. The `make_greeting` column tells you which flow invocation each event belongs to.
- Lines 02–05 show that `clean_name()` finished before `greet()` started. Each `finish` line gives the ID of the version that stage produced.
- Line 06 repeats the version ID from line 05. That tells you the flow returned the greeting produced by `greet()`.

Use `loop.trace()` to see what ran and in what order. Use `loop.input()`, as you did above, to find the exact input a stage used.
