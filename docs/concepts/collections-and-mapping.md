# Collections and mapping

An editor has three customer replies to check and one document to assemble. Each reply needs its own spelling result. The document needs all three replies. Run the examples in order in a notebook.

## Give each reply a key

A `Collection` groups items by key. You supply plain values. At the flow boundary, Strange Loop records a separate `Version` for each item. A stage can also return a `Collection` when it generates the items itself.

Below, we run the `drafts` collection through a flow that simply returns it, so we can inspect the recorded versions.

```python
from strange_loop import Collection, Context, Failure, Loop, Replacement, Version, flow, stage

drafts = Collection({
    "r01": "Your order has shipped.",
    "r02": "Your reciept is attached.",
    "r03": "Your refund is complete.",
})


@flow
async def inspect_drafts(ctx: Context, drafts: Collection) -> Collection:
    return drafts


recorded = await Loop().run(inspect_drafts, drafts=drafts)
reply = recorded["r02"]
print("Key:", "r02")
print("Version:", reply.id)
print("Text:", reply.value)
```

```text
Key: r02
Version: inspect_drafts/drafts/r02/1@1
Text: Your reciept is attached.
```

The key `r02` identifies the reply across edits. The version ID identifies the exact text at this point in the run.

## Check one reply at a time

First define what to do with **one** reply. The check looks for one known typo. The flow returns the draft if it passes, or a failure that links the draft to the spelling finding.

```python
@stage
def check_spelling(text: str) -> str | None:
    if "reciept" in text:
        return "Change 'reciept' to 'receipt'."
    return None


@flow
async def check_reply(ctx: Context, draft: Version[str]) -> Version[str] | Failure:
    finding = await check_spelling(text=draft)
    if finding.value is not None:
        return ctx.fail(finding.value, target=draft, evidence=finding)
    return draft
```

Now map that flow over the collection. In `ctx.map(check_reply, draft=drafts)`, the keyword `draft` names the child flow’s parameter. Each child receives one version. The result is a `Collection` with the same keys in the same order.

```python
@flow
async def check_all(ctx: Context, drafts: Collection) -> Collection:
    return await ctx.map(check_reply, draft=drafts)


checked = await Loop().run(check_all, drafts=drafts)
print("Passed:", list(checked.succeeded))
print("Failed:", list(checked.failed))
print("Reason for r02:", checked.failed["r02"].reason)
```

```text
Passed: ['r01', 'r03']
Failed: ['r02']
Reason for r02: Change 'reciept' to 'receipt'.
```

## Correct just the failed replies

Both `.succeeded` and `.failed` are keyed collections. Pass `checked.failed` to a second map, and only the failed keys enter the correction flow. Here that flow fixes the typo recorded by the spelling check and checks the corrected text.

```python
@stage
def proofread(text: str) -> str:
    return text.replace("reciept", "receipt")


@flow
async def correct_reply(ctx: Context, failure: Failure) -> Version[str] | Failure:
    fixed = await proofread(text=failure.target)
    return await check_reply(draft=fixed)


@flow
async def prepare_replies(ctx: Context, drafts: Collection) -> Collection:
    checked = await ctx.map(check_reply, draft=drafts)
    corrections = await ctx.map(correct_reply, failure=checked.failed)
    checked.update(corrections)
    return checked


prepared = await Loop().run(prepare_replies, drafts=drafts)
print("Corrected r02:", prepared["r02"].value)
print("Passed:", list(prepared.succeeded))
```

```text
Corrected r02: Your receipt is attached.
Passed: ['r01', 'r02', 'r03']
```

Here `checked.update(corrections)` assigns the proofreader’s result to the existing key `r02`. The entries for `r01` and `r03` keep their original versions. Updating the collection continues the current pass through the flow.

## Assemble the replies into one document

Pass the successful collection to a single stage to combine the replies. Inside `assemble_replies()`, the argument is a dictionary of strings. Strange Loop records the key and input version for each string.

Suppose the editor reads the document and asks for one more change to `r02`. Here the editor wants the receipt line to say “email.” This time the flow calls `ctx.replace()` to give the reply a new version and trigger replay.

```python
@stage
def assemble_replies(replies: dict[str, str]) -> str:
    return "\n".join(replies.values())


@stage
def clarify_delivery(text: str) -> str:
    return text.replace("is attached.", "is attached to this email.")


@flow
async def edit_document(ctx: Context, drafts: Collection) -> Version[str] | Replacement:
    prepared = await prepare_replies(drafts=drafts)
    document = await assemble_replies(replies=prepared.succeeded)
    if "Your receipt is attached." in document.value:
        reply = prepared["r02"]
        fixed = await clarify_delivery(text=reply)
        return ctx.replace(reply, with_=fixed)
    return document


loop = Loop()
document = await loop.run(edit_document, drafts=drafts)
print(document.value)
```

```text
Your order has shipped.
Your receipt is attached to this email.
Your refund is complete.
```

Follow the document’s `replies` input to see the three versions that went into the final text.

```python
for key, reply in loop.input(document, "replies").items():
    print(key, reply.id)
```

```text
r01 edit_document/drafts/r01/1@1
r02 edit_document/call1-prepare_replies/map2-correct_reply/r02/proofread/1@2
r03 edit_document/drafts/r03/1@1
```

The traces for the spelling check and assembly show what changed on replay.

```python
print(loop.trace(check_spelling))
print(loop.trace(assemble_replies))
```

```text
start check_spelling()
finish check_spelling() -> edit_document/call1-prepare_replies/map1-check_reply/r01/check_spelling/1@1
start check_spelling()
finish check_spelling() -> edit_document/call1-prepare_replies/map1-check_reply/r02/check_spelling/1@1
start check_spelling()
finish check_spelling() -> edit_document/call1-prepare_replies/map1-check_reply/r03/check_spelling/1@1
start check_spelling()
finish check_spelling() -> edit_document/call1-prepare_replies/map2-correct_reply/r02/call1-check_reply/check_spelling/1@1
reuse check_spelling() -> edit_document/call1-prepare_replies/map1-check_reply/r01/check_spelling/1@1
reuse check_spelling() -> edit_document/call1-prepare_replies/map1-check_reply/r03/check_spelling/1@1
start check_spelling()
finish check_spelling() -> edit_document/call1-prepare_replies/map2-correct_reply/r02/call1-check_reply/check_spelling/2@1
start assemble_replies()
finish assemble_replies() -> edit_document/assemble_replies/1@1
start assemble_replies()
finish assemble_replies() -> edit_document/assemble_replies/2@1
```

Read the two `reuse check_spelling()` lines first. They identify `r01` and `r03`, whose texts stayed the same. The next spelling check runs on the editor’s new version of `r02`. The assembly trace shows two calls because each document depends on all three reply versions. Changing even one input gives the editor a new document and preserves the first document’s history.
