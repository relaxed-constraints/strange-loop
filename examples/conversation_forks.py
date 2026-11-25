"""Run draft, then repairs, against the same database in separate processes.

python -m examples.conversation_forks draft /tmp/conversations.sqlite
python -m examples.conversation_forks repairs /tmp/conversations.sqlite
Requires codex-box, Docker and a local Codex login. Each phase makes live calls.
"""
import argparse
import asyncio
import sqlite3
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

from codex_box import AssistantMessage, ChatGPTSubscription, CodexBox, Trace, TurnCompleted, UserMessage
from strange_loop.replay import Loop, stage

DRAFT = ("Translate into SQLite SQL: count rows in the table order_events. "
         "Its only column is order_id INTEGER NOT NULL. Return only the SELECT statement, "
         "without Markdown or commentary. Do not use tools.")
DEFECT = ("The draft has a defect: duplicate events for one order inflate the count. "
          "Please repair the previous SQL so it counts unique orders. ")
REQUESTS = {
    "distinct": DEFECT + "Use COUNT(DISTINCT ...). Return only SQL, without Markdown.",
    "grouped": DEFECT + "Use GROUP BY in a subquery and count its rows. Return only SQL, without Markdown.",
}


async def turn(prompt: str, parent: Trace | None = None) -> dict:
    # Each invocation has a new container and an empty workspace.
    with TemporaryDirectory() as directory:
        async with await CodexBox.launch(workspace=Path(directory), credentials=ChatGPTSubscription()) as box:
            agent = await box.launch_agent(resume_from=parent, sandbox="readOnly")
            await agent.send(prompt)
            async with asyncio.timeout(180):
                async for event in agent.events():
                    if isinstance(event, TurnCompleted):
                        if event.status != "completed":
                            raise RuntimeError(event.error or event.status)
                        break
                else:
                    raise RuntimeError("Session ended before turn completion")
            trace = agent.trace()
            suffix = trace.segments[len(parent.segments) if parent else 0:]
            sql = next(s.text for s in reversed(suffix) if isinstance(s, AssistantMessage))
            return {"sql": sql.strip(), "trace": trace.to_json()}


@stage
async def translate(prompt: str) -> dict:
    return await turn(prompt)


@stage
async def repair(draft: dict, feedback: str) -> dict:
    return await turn(feedback, Trace.from_json(draft["output"]["trace"]))


async def draft_flow(ctx, prompt):
    return await translate(prompt=prompt)


async def repair_flow(ctx, draft, feedback):
    return await repair(draft=draft, feedback=feedback)


def count(sql: str) -> int:
    with closing(sqlite3.connect(":memory:")) as db:
        db.executescript("CREATE TABLE order_events(order_id INTEGER NOT NULL);"
                         "INSERT INTO order_events VALUES (1),(1),(2); PRAGMA query_only=ON;")
        return db.execute(sql).fetchone()[0]


async def main(mode: str, path: Path) -> None:
    if mode == "draft":
        result = await Loop(store=path).run(draft_flow, prompt=DRAFT, run_id="draft")
        assert count(result.value["sql"]) == 3
        print(result.id, result.value["sql"])
        return

    store = Loop(store=path)
    draft = store.load("draft").result
    source = {"run_id": "draft", "version_id": draft.id, "output": draft.value}
    parent = Trace.from_json(draft.value["trace"])
    children = set()
    for name, feedback in REQUESTS.items():
        if mode == "repairs" and name not in store.runs():
            await Loop(store=path).run(repair_flow, draft=source, feedback=feedback, run_id=name)
        saved = store.load(name)
        assert saved.input(saved.result, "draft").value == source
        output = saved.result.value
        child = Trace.from_json(output["trace"])
        assert child.forked_from.trace_id == parent.trace_id
        assert child.forked_from.at_segment_uuid == parent.segments[-1].uuid
        prefix = len(parent.segments)
        assert child.segments[:prefix] == parent.segments
        assert [s.text for s in child.segments[prefix:] if isinstance(s, UserMessage)] == [feedback]
        assert child.trace_id != parent.trace_id and child.trace_id not in children
        children.add(child.trace_id)
        assert count(output["sql"]) == 2
        print(f"{name}: {child.trace_id} from {parent.trace_id} | {output['sql']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["draft", "repairs", "inspect"])
    parser.add_argument("database", type=Path)
    args = parser.parse_args()
    asyncio.run(main(args.mode, args.database))
