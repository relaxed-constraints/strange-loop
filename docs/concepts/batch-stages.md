# Batch stages

Three customer replies need Spanish translations. Each translation needs the same instructions about language, tone, and output format. Sending all three messages in one LLM request uses one copy of those instructions instead of three, saving input tokens on the shared prompt. With `@batch_stage`, each flow calls the stage with one reply and receives its own result.

A [collection](collections-and-mapping.md) holds the items you want to process. A `Batch` holds the calls that are ready for one stage attempt. Run the examples in order in a notebook.

## Share one translation prompt

The batch stage sends one instruction and a JSON object containing the keyed English messages. The model should return a JSON object with the same keys and Spanish text. Here a local stand-in prints the prompt and returns fixed translations, so you can inspect the request without an API key.

```python
import json

from strange_loop import Batch, Context, Loop, Version, batch_stage, flow

drafts = {
    "r01": "Your order has shipped.",
    "r02": "Your receipt is attached.",
    "r03": "Your refund is complete.",
}


instructions = (
    "Translate each customer message into polite Spanish. "
    "Return a JSON object with the same keys and the translated messages."
)

# Mock translations for demonstration purposes
spanish = {
    "Your order has shipped.": "Su pedido está en camino.",
    "Your receipt is attached.": "Adjuntamos su recibo.",
    "Your refund is complete.": "Su reembolso se ha completado.",
}

# Mock LLM function for demonstration purposes
def mock_llm(instructions: str, messages_json: str) -> str:
    print(instructions)
    print(messages_json)
    messages = json.loads(messages_json)
    return json.dumps({key: spanish[text] for key, text in messages.items()})


@batch_stage
def translate(batch: Batch) -> dict[str, str]:
    messages = {item.key: item.inputs["text"] for item in batch}
    response = mock_llm(instructions, json.dumps(messages))
    return json.loads(response)
```

The caller supplies `text=source`. Inside the batch, `item.inputs["text"]` holds the underlying string. The stage makes one model call and returns a dictionary keyed by `item.key`. The loop uses those keys to deliver a separate `Version` to each caller.

Here the whole run consists of one flow per reply, so we can use `loop.map()` directly. Like `ctx.map()` inside a flow, it preserves the keys of the mapped inputs.

```python
@flow
async def translate_reply(ctx: Context, source: Version[str]) -> Version[str]:
    return await translate(text=source)


loop = Loop()
translations = await loop.map(translate_reply, source=drafts)
for key, translation in translations.items():
    print(key, translation.id, translation.value)
```

```text
Translate each customer message into polite Spanish. Return a JSON object with the same keys and the translated messages.
{"r01": "Your order has shipped.", "r02": "Your receipt is attached.", "r03": "Your refund is complete."}
r01 r01/translate/1@1 Su pedido está en camino.
r02 r02/translate/1@1 Adjuntamos su recibo.
r03 r03/translate/1@1 Su reembolso se ha completado.
```

The printed prompt contains the instruction once and all three English messages. The next three lines show each caller’s version ID and Spanish translation. The loop groups pending calls to the same batch stage automatically. A stage can receive a single item if only one call is ready.

Use the stage trace to see how the loop grouped the calls.

```python
print(loop.trace(translate))
```

```text
queued translate()
queued translate()
queued translate()
translate() draft attempt 1: r01, r02, r03
finish translate() -> r01/translate/1@1
accepted r01/translate/1@1
finish translate() -> r02/translate/1@1
accepted r02/translate/1@1
finish translate() -> r03/translate/1@1
accepted r03/translate/1@1
```

Each `queued translate()` line represents one reply waiting for translation. The single `draft attempt 1` line lists all three keys, so the loop ran the stage body once with all three replies. Each `finish` and `accepted` pair records the output version for one reply.

The trace records stage activity. Here the body of `translate()` calls `mock_llm()` once per attempt, and the mock prints its prompt on every call. The single prompt above confirms that it received one request.

## Submit partial results

Suppose you ask the model to stream one JSON record per translation. As soon as your client decodes a complete record, the stage can submit that translation. Here a mock client yields two decoded records and simulates a timeout before the third arrives.

```python
from collections.abc import AsyncIterator


async def stream_translations(texts: dict[str, str]) -> AsyncIterator[tuple[str, str]]:
    print("Streaming request:", ", ".join(texts))
    for key, text in texts.items():
        if key == "r02" and len(texts) > 1:
            continue  # Simulate a delayed response for r02.
        yield key, spanish[text]
    if "r02" in texts and len(texts) > 1:
        raise TimeoutError("Timed out waiting for r02")


@batch_stage
async def translate_stream(batch: Batch) -> None:
    texts = {item.key: item.inputs["text"] for item in batch}
    async for key, translation in stream_translations(texts):
        await batch[key].submit(translation)


@flow
async def translate_streamed_reply(ctx: Context, source: Version[str]) -> Version[str]:
    return await translate_stream(text=source)


loop = Loop(attempts=2)
translations = await loop.map(translate_streamed_reply, source=drafts)
print("Final r02 translation:", translations["r02"].value)
print(loop.trace("batch"))
```

```text
Streaming request: r01, r02, r03
Streaming request: r02
Final r02 translation: Adjuntamos su recibo.
translate_stream() draft attempt 1: r01, r02, r03
accepted r01/translate_stream/1@1
accepted r03/translate_stream/1@1
translate_stream() draft attempt 1: TimeoutError
retry translate_stream(): r02
translate_stream() draft attempt 2: r02
accepted r02/translate_stream/1@1
```

The first two `accepted` events record the results for `r01` and `r03`. When the timeout occurs, only `r02` still needs a result. The loop sends that key in the second request. `attempts=2` allows the initial attempt and one retry.

Choose one way to deliver results per attempt. Return a keyed dictionary when the results arrive together. Use `await item.submit(value)` and return `None` when results arrive individually. Submitting an item and returning a dictionary in the same attempt raises `BatchContractError`.

## When does a reply need a revision?

The timeout calls for a **retry** because the caller still needs a translation for `r02`. If a reviewer reads a completed translation and asks for a less formal tone, the flow can pass that feedback to the translator and request a **revision**.

A revisable batch stage uses `@batch_stage(revisable=True)`. Each item carries its own revision request in `item.request`, with `None` on the initial call. The loop groups revision requests separately from initial calls. See [Revisions and replacements](revisions-and-replacements.md) for the writer example.
