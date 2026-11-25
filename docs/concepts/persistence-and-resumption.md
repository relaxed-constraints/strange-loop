# Persistence and resumption

## In memory or in SQLite

`Loop()` keeps the run in memory. You can inspect its result, versions, and trace while you have the loop object. To reopen a run after your Python session ends, pass a SQLite file path to `Loop(store=...)`. The loop saves inputs, stage outputs, and trace events incrementally, including work completed before an interruption.

Give the run a `run_id` so you can find it again. Use `run()` to start it, `load(run_id)` to open its saved state, and `resume(flow)` to get its result or finish pending work. Supply the same flow when you resume. The saved run already contains its inputs.

## Reopen a completed run

Suppose a flow translates a customer reply and formats it as an email. Each stage prints a line when its body executes. Run the examples in order in a notebook. For this example, a temporary directory provides a fresh SQLite file. For a run you want to keep, choose a permanent file path.

```python
from pathlib import Path
from tempfile import TemporaryDirectory

from strange_loop import Context, Loop, Version, flow, stage

directory = TemporaryDirectory()
database = Path(directory.name) / "replies.sqlite"


@stage
def translate(message: str) -> str:
    print("Calling the translator")
    # Stand-in for a translation service.
    return {"Your order has shipped.": "Su pedido está en camino."}[message]


@stage
def format_email(text: str) -> str:
    print("Formatting the email")
    return f"Hola,\n{text}"


@flow
async def prepare_email(ctx: Context, message: Version[str]) -> Version[str]:
    translation = await translate(message=message)
    return await format_email(text=translation)


loop = Loop(store=database)
email = await loop.run(prepare_email, message="Your order has shipped.", run_id="reply-01")
print(email.value)
```

```text
Calling the translator
Formatting the email
Hola,
Su pedido está en camino.
```

Open the same file with a fresh loop and load the run. Its result is already available. Calling `resume()` on a completed run returns the saved result immediately.

```python
saved = Loop(store=database).load("reply-01")
print("Loaded status:", saved.status)
trace_before = saved.trace()
email = await saved.resume(prepare_email)
print(email.value)
print("Trace unchanged:", saved.trace() == trace_before)
```

```text
Loaded status: completed
Hola,
Su pedido está en camino.
Trace unchanged: True
```

Only the email and inspection results appear. Neither stage prints an execution message, and the trace contains exactly the same events as before.

## Resume after an interruption

Now suppose the email formatter is unavailable after the translation finishes. This second run uses the same translator. A flag stands in for the formatter’s availability so you can reproduce the interruption and recovery locally.

```python
class FormatterUnavailable(RuntimeError):
    pass


formatter_online = False


@stage
def format_remotely(text: str) -> str:
    print("Calling the email formatter")
    if not formatter_online:
        raise FormatterUnavailable("Email formatter is offline")
    return f"Hola,\n{text}"


@flow
async def prepare_remote_email(ctx: Context, message: Version[str]) -> Version[str]:
    translation = await translate(message=message)
    return await format_remotely(text=translation)


try:
    await Loop(store=database).run(
        prepare_remote_email, message="Your order has shipped.", run_id="reply-02"
    )
except* FormatterUnavailable:
    print("Run interrupted after translation")
```

```text
Calling the translator
Calling the email formatter
Run interrupted after translation
```

The formatter raises `FormatterUnavailable`, and the loop’s task group wraps it in an `ExceptionGroup`. A regular `except FormatterUnavailable:` would miss it because Python checks the type of the outer exception. The `except* FormatterUnavailable:` clause looks inside the group and handles the matching error, so the notebook can continue. Any unrelated errors in the group still propagate.

Restore the formatter and reopen the interrupted run. Print the translator’s trace to see which translation the loop used on resumption.

```python
formatter_online = True
saved = Loop(store=database).load("reply-02")
print("Before resumption:", saved.status)
email = await saved.resume(prepare_remote_email)
print("After resumption:", saved.status)
print(email.value)
print(saved.trace(translate))
```

```text
Before resumption: incomplete
Calling the email formatter
After resumption: completed
Hola,
Su pedido está en camino.
start translate()
finish translate() -> prepare_remote_email/translate/1@1
reuse translate() -> prepare_remote_email/translate/1@1
```

The flow starts again and reaches `translate()`. The `reuse` line identifies the translation saved on the first pass. The formatter still needs to produce a result, so its body runs again. If an interrupted stage has already called an external service, resumption can repeat that call when the stage has no saved output.
