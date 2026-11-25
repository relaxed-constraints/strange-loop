# Revisions and replacements

A customer asks for a receipt. Your LLM or AI agent drafts a reply, but the reply has a typo. It says “reciept,” and a spelling check catches the typo. Who should correct the draft?

- **Revision.** If the writer has the feedback it needs, use `ctx.revise()` to ask the stage that produced the draft to try again.
- **Replacement.** If a proofreader has already supplied a corrected reply, use `ctx.replace()` to adopt that reply as the next version of the draft.

Both operations keep the original draft. Run the examples in order in a notebook to compare their histories.

## Give the writer a chance to revise

In [Checks and failures](checks-and-failures.md), the caller supplied the draft. Here, a stage named `write_reply` writes it. Imagine that this stage calls a language model. On its first call, the model needs the customer’s request. On a revision, it also needs its previous reply and the spelling feedback.

This small stand-in for a model returns predictable answers so you can run the example locally.

```python
from strange_loop import Context, Loop, Replacement, Revise, RevisionRequest, Version, flow, stage


def llm_call(prompt: str) -> str:
    if prompt.startswith("Fix the reply"):
        return "I’ve attached your receipt."
    return "I’ve attached your reciept."
```

Mark the writer with `@stage(revisable=True)`. The loop supplies the keyword-only `request` argument. On the first call it is `None`. On the next attempt it is a `RevisionRequest` containing the previous value and feedback.

```python
@stage(revisable=True)
def write_reply(customer_request: str, *, request: RevisionRequest | None) -> str:
    if request is None:
        return llm_call(f"Write a reply to the customer.\n{customer_request}")
    return llm_call(
        f"Fix the reply using the feedback.\nCustomer request: {customer_request}\n"
        f"Previous reply: {request.previous}\nFeedback: {request.feedback}"
    )


@stage
def check_spelling(text: str) -> str | None:
    if "reciept" in text:
        return "Change 'reciept' to 'receipt'."
    return None


@flow
async def ask_writer(ctx: Context, message: Version[str]) -> Version[str] | Revise:
    draft = await write_reply(customer_request=message)
    finding = await check_spelling(text=draft)
    if finding.value is not None:
        return ctx.revise(draft, feedback=finding)
    return draft
```

The flow returns the revision instruction. Passing `finding` gives the writer the suggestion text and preserves a link to the check result. The loop calls the writer again, records the new draft, and replays the flow. The spelling check now passes.

```python
message = "Could you send me a copy of my receipt?"
writer_loop = Loop()
revised = await writer_loop.run(ask_writer, message=message)

print(revised.value)
print(writer_loop.trace())
```

```text
I’ve attached your receipt.
01  ask_writer  start
02  ask_writer  start write_reply()
03  ask_writer  finish write_reply() -> ask_writer/write_reply/1@1
04  ask_writer  start check_spelling()
05  ask_writer  finish check_spelling() -> ask_writer/check_spelling/1@1
06  ask_writer  request write_reply() revision of ask_writer/write_reply/1@1 with feedback "Change 'reciept' to 'receipt'."
07  ask_writer  start write_reply()
08  ask_writer  finish write_reply() revise -> ask_writer/write_reply/1@2
09  ask_writer  replay
10  ask_writer  reuse write_reply() -> ask_writer/write_reply/1@2
11  ask_writer  start check_spelling()
12  ask_writer  finish check_spelling() -> ask_writer/check_spelling/2@1
13  ask_writer  return ask_writer/write_reply/1@2
```

## Adopt a proofreader’s correction

A proofreader can fix this particular typo directly. Its stage produces a version of its own. Pass that version to `ctx.replace()` to make the corrected text the next version of the draft.

```python
@stage
def proofread(text: str) -> str:
    return text.replace("reciept", "receipt")


@flow
async def use_proofreader(ctx: Context, message: Version[str]) -> Version[str] | Replacement:
    draft = await write_reply(customer_request=message)
    finding = await check_spelling(text=draft)
    if finding.value is not None:
        fixed = await proofread(text=draft)
        return ctx.replace(draft, with_=fixed)
    return draft
```

On replay, the flow gets the corrected draft and checks it again. The writer only needed one model call in this run.

```python
proofreader_loop = Loop()
replaced = await proofreader_loop.run(use_proofreader, message=message)

print(replaced.value)
print(proofreader_loop.trace())
```

```text
I’ve attached your receipt.
01  use_proofreader  start
02  use_proofreader  start write_reply()
03  use_proofreader  finish write_reply() -> use_proofreader/write_reply/1@1
04  use_proofreader  start check_spelling()
05  use_proofreader  finish check_spelling() -> use_proofreader/check_spelling/1@1
06  use_proofreader  start proofread()
07  use_proofreader  finish proofread() -> use_proofreader/proofread/1@1
08  use_proofreader  use_proofreader replaces use_proofreader/write_reply/1@1 -> use_proofreader/write_reply/1@2 from use_proofreader/proofread/1@1
09  use_proofreader  replay
10  use_proofreader  reuse write_reply() -> use_proofreader/write_reply/1@2
11  use_proofreader  start check_spelling()
12  use_proofreader  finish check_spelling() -> use_proofreader/check_spelling/2@1
13  use_proofreader  return use_proofreader/write_reply/1@2
```

## Compare the two histories

In both traces, step **03** records the draft with the typo (`write_reply/1@1`), and steps **04–05** check its spelling. Steps **06–08** show who corrects it.

| Step | Writer revision (`ask_writer`) | Proofreader replacement (`use_proofreader`) |
| --- | --- | --- |
| **06** | The flow asks the writer to revise the draft and passes the spelling feedback. | The flow calls `proofread()` with the draft. |
| **07** | `write_reply()` starts its second attempt. | `proofread()` returns its own corrected version, `proofread/1@1`. |
| **08** | The writer produces `write_reply/1@2`. Its `.actor` names the writer, and `.request.feedback` points to the check result from step **05**. | The flow adopts the proofreader’s correction as `write_reply/1@2`. Its `.adopted_from` points to the proofreader’s output from step **07**. |

In both runs, `.previous` on the version from step **08** points to the draft from step **03**.

### Why the spelling check runs twice

The first pass ends when the flow returns `ctx.revise(...)` or `ctx.replace(...)`. Both instructions give the draft a new version at step **08**. Because the draft changed, the loop starts the flow function again at **09** so it can evaluate the corrected draft. The runtime supplies this second pass automatically.

- **10 — Reuse the corrected draft.** Python reaches `draft = await write_reply(...)` again. The loop returns the version recorded at **08** without executing the writer’s function body. That makes **two writer calls total** for revision (the original at **02** and the revision at **07**) and **one writer call total** for replacement (the original at **02**).
- **11–12 — Check the new version.** The finding from **05** belongs to the original draft, `write_reply/1@1`. This time `check_spelling(text=draft)` receives `write_reply/1@2`, so the loop runs the spelling check again and records a fresh finding.
- **13 — Return the reply.** The second check finds no typo. In both flows, `finding.value` is now `None`, so Python skips the correction branch and reaches `return draft`. Both examples print “I’ve attached your receipt.”

On replay, ordinary Python in the flow runs again. That includes the `if` statement, so a new finding can change which branch the flow takes. A `print()` just before that `if` would run on both passes. A `print()` inside `write_reply()` would run only when the loop executes the stage body. Step **10** reuses the recorded reply, so it would produce no printout from the writer.
