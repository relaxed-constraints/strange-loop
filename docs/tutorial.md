# Tutorial · Translate and verify three queries

Translate three requests in one batch. One translation passes immediately, one needs investigation, and one goes back to its producer for a syntax fix. The translator and investigator use deterministic fixtures, so you can follow every execution locally.

## 1. Start three independent flows

From the repository root, run the complete example:

```sh
uv run python -m examples.replay
```

Open `examples/replay.py` alongside the output. Its main function creates a Loop and calls `await loop.map(translate_query, source=SOURCES)` (the example names the instance `run`). Each item has its own invocation.

| Key | Source request |
| --- | --- |
| q01 | list orders |
| q02 | count order lines |
| q03 | list customers |

## 2. Trace the first batch

Each invocation reaches the same line:

```python
query = await translate(source=source)
```

The translator is an async `@batch_stage(revisable=True)`. It receives all three rows, submits SQL for q01 and q03, then raises a simulated timeout before delivering q02. Read the first entries in the batch trace:

```text
translate() draft attempt 1: q01, q02, q03
accepted q01/translate/1@1
accepted q03/translate/1@1
translate() draft attempt 1: TimeoutError
retry translate(): q02
translate() draft attempt 2: q02
accepted q02/translate/1@1
```

Each accepted output has its own version. The second attempt fills the missing row. At this point q02 has received its first translation.

## 3. Follow the checker’s decision

Here is the flow from the example. The checker receives the decoded SQL, while the runtime records which SQL version it checked.

```python
from strange_loop import Context, Revise, Replacement, Version


async def translate_query(
    ctx: Context, source: Version
) -> Version | Revise | Replacement:
    query = await translate(source=source)
    verdict = await check(source=source, query=query)
    if verdict.value == "pass":
        return query
    if verdict.value == SYNTAX_FEEDBACK:
        return ctx.revise(query, feedback=verdict)
    fixed = await investigate(source=source, query=query, verdict=verdict)
    return ctx.replace(query, with_=fixed)
```

q01 passes and returns its first version. The other items take different paths through the same function.

## 4. Follow q02 through replacement

The SQL counts orders. The checker expected a count of order lines. The investigator supplies SQL with `UNNEST` and the flow adopts that result. On replay, `translate` returns the adopted version. The checker runs again because its input changed. Its passing verdict returns the corrected SQL.

The returned version’s `previous` link points to the original translation. Its `adopted_from` link points to the investigator’s output. Follow that output’s inputs to see the SQL and checker feedback that informed the correction.

## 5. Follow q03 through producer revision

The first SQL contains `FORM` where it needs `FROM`. The flow sends the checker’s verdict to the translator. On this call, `item.inputs["source"]` still holds the original request. The translator reads the old SQL from `item.request.previous` and the correction instruction from `item.request.feedback`.

```text
translate() revise attempt 1: q03
accepted q03/translate/1@2
```

q03 replays and the checker passes. Its second version records the translator as its author and links to the feedback through its revision request.

## 6. Compare the returned results

| Query | Final translation | Execution history |
| --- | --- | --- |
| q01 | Version 1 | One accepted draft and one check |
| q02 | Version 2 | Missing draft retried, investigator’s correction adopted, new version checked |
| q03 | Version 2 | Producer revised the accepted draft using feedback, new version checked |

The batch trace explains why the translator ran. The version links explain where each returned value came from. For a separate example where one change invalidates a whole workload measurement, see [Mapping and joining](guide/mapping.md).
