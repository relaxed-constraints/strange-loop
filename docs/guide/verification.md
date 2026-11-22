# Batch and check items

Use an `AgentStage` when an agent must produce the output. Give the stage a name, instructions, and an output type. Use one of these types: `str`, `int`, `float`, `bool`, `dict`, or `list`.

```python
from strange_loop import AgentStage, Batch, Pass, Repair, flow, grader, keyed

translate = AgentStage(
    name="translate",
    instructions="Translate the text into French. Preserve each {{name}} placeholder.",
    output=str,
    batching=Batch(max_items=4),
)
```

## Check one output

A grader checks a specific output revision. Declare which inputs the grader can send for repair. Give the producer enough feedback to repair the output.

```python
@grader(repairable=("translation",))
def check(ctx, *, text, translation):
    if "{name}" in text and "{name}" not in translation:
        return Repair(ctx.ref("translation"), "Restore the {name} placeholder.")
    return Pass()
```

The grader receives both strings as values. The call to `ctx.ref("translation")` identifies the exact revision that needs repair. This grader checks one formatting rule. It does not assess translation quality.

## Map the stages over items

Use `keyed` to give each input a stable ID. Use `.map()` to call a stage once for each ID.

```python
@flow
async def translate_messages(ctx, *, texts):
    translations = await translate.map(text=texts)
    await check.map(text=texts, translation=translations)
    return translations

# Inside an async function with a configured runner:
texts = keyed({"welcome": "Welcome, {name}!", "exit": "Goodbye!"})
results = await runner.run(translate_messages, texts=texts)
```

All keyed inputs to one mapped call must have the same IDs. The runner passes a single shared input to every item.

The batch size controls how many items an agent receives in one turn. Omit `batching` to give the agent one item per turn. To produce a single output, use `await translate(text=text)` inside a flow.

## Follow a repair

1. The grader finds a missing placeholder in the welcome message.
2. The runner sends the agent the previous welcome translation, the current inputs, and the feedback.
3. The grader checks the new welcome translation. The exit message keeps its passing verdict.

If a grader targets a shared input, the runner revisits each stage that used the revised input. A child flow can request a repair from an agent stage in its parent flow.

Outside a candidate loop, the runner allows up to `max_repairs` repair rounds per run. A backend interruption uses a separate limit, `attempts`. The runner retries missing items after a partial submission.

## Use named inputs in a prompt

Use `{language}` to insert the value of a stage argument named `language`. Use ordinary Python strings for these templates. Write `{{name}}` to include the literal text `{name}`.

Each input in the prompt must have the same value for every entry in that turn. Keep varying inputs in `work.json`. The adapter inserts strings directly and encodes other values as JSON. It expands each placeholder once.

The adapter selects the prompt separately for each attempt. An input that the repair prompt omits appears in the repair entry under `inputs`. The agent writes each answer to the path in `output_file`. For example, the key `welcome` uses `welcome.json`.
