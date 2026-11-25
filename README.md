# Strange Loop

Strange Loop is a Python library for workflows where an intermediate result may need a second attempt. You write the flow in ordinary Python. Strange Loop records each stage’s output and the inputs that produced it.

Suppose a language model writes ten queries and one fails a check. You can send that query back with feedback, check the new version, and reuse the nine that passed. Later, you can trace the corrected query back to its first draft and the feedback that prompted the change. For longer workflows, save the run in SQLite and resume it after an interruption.

## Installation

Strange Loop requires Python 3.12 or later. Install `relcon-strange-loop` from PyPI and import it as `strange_loop`.

```sh
python -m pip install relcon-strange-loop
```

[Installation guide](https://relaxed-constraints.github.io/strange-loop/getting-started/installation/)

## Quickstart

This flow cleans a name and uses it in a greeting. Each stage receives a string. The flow receives a version of each result, so you can follow the greeting back to the exact input, including its spaces.

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
async def make_greeting(ctx: Context, text: Version[str]) -> Version[str]:
    name = await clean_name(text=text)
    return await greet(name=name)


async def main() -> None:
    loop = Loop()
    greeting = await loop.run(make_greeting, text="  Alice  ")
    parent = loop.input(greeting, "name")
    root = loop.input(parent, "text")

    print(greeting.value)                  # Hello, Alice!
    print(parent.actor, repr(parent.value))  # clean_name 'Alice'
    print(root.actor, repr(root.value))      # <input> '  Alice  '


if __name__ == "__main__":
    asyncio.run(main())
```

The [full quickstart](https://relaxed-constraints.github.io/strange-loop/getting-started/quickstart/) shows how to read this flow’s execution trace.

## Documentation

- [Overview](https://relaxed-constraints.github.io/strange-loop/) introduces stages, versions, flows, and runs.
- [Revisions and replacements](https://relaxed-constraints.github.io/strange-loop/concepts/revisions-and-replacements/) shows two ways to correct a result and how each appears in its history.
- [API reference](https://relaxed-constraints.github.io/strange-loop/reference/) groups classes and methods by task.

## License

Strange Loop is licensed under the MIT License.
