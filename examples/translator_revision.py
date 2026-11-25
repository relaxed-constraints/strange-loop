"""Run draft and repair in separate processes against the same SQLite file."""
import argparse
import asyncio
import os
from pathlib import Path

from codex_box import Trace, UserMessage
from examples.conversation_forks import DRAFT, REQUESTS, count, turn
from strange_loop.replay import Loop, RevisionRequest, Version, stage


@stage(revisable=True)
async def translate(prompt: str, *, request: RevisionRequest | None) -> Version:
    if request is None:
        answer = await turn(prompt)
    else:
        parent = Trace.from_json(request.target.attachments["conversation"])
        answer = await turn(request.feedback, parent)
    return Version(answer["sql"], attachments={"conversation": answer["trace"]})


@stage
def explain(sql: str) -> str | None:
    assert isinstance(sql, str)
    return None if count(sql) == 2 else REQUESTS["distinct"]


async def workflow(ctx, prompt):
    sql = await translate(prompt=prompt)
    if os.environ.get("TRANSLATOR_DEMO_STOP") == "1":
        # Intentional process interruption after the stage has committed and closed its box.
        os._exit(75)
    feedback = await explain(sql=sql)
    return ctx.revise(sql, feedback=feedback) if feedback.value else sql


async def main(mode: str, path: Path):
    store = Loop(store=path)
    if mode == "draft":
        os.environ["TRANSLATOR_DEMO_STOP"] = "1"
        await store.run(workflow, prompt=DRAFT, run_id="translation", max_revisions=1)
    if mode == "repair":
        await store.load("translation").resume(workflow)
    saved = store.load("translation")
    revised = saved.result
    draft = revised.request.target
    parent = Trace.from_json(draft.attachments["conversation"])
    child = Trace.from_json(revised.attachments["conversation"])
    assert child.forked_from.trace_id == parent.trace_id
    assert child.forked_from.at_segment_uuid == parent.segments[-1].uuid
    assert child.segments[:len(parent.segments)] == parent.segments
    assert [s.text for s in child.segments[len(parent.segments):] if isinstance(s, UserMessage)] == [
        revised.request.feedback.value]
    print(saved.trace())
    for ref in (draft, revised):
        print(ref.id, ref.value, "=>", count(ref.value))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["draft", "repair", "inspect"])
    parser.add_argument("database", type=Path)
    args = parser.parse_args()
    asyncio.run(main(args.mode, args.database))
