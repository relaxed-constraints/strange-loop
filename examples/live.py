"""Run a three-item batch, with a deliberate repair of one item."""

import asyncio
import os

from strange_loop import (
    AgentStage,
    Batch,
    CodexBackend,
    Pass,
    Repair,
    Runner,
    flow,
    grader,
    keyed,
)

produce = AgentStage(
    name="produce",
    output=str,
    batching=Batch(3),
    instructions="Copy each text input in {language}.",
    repair_instructions="Revise the previous answer according to the grader feedback.",
)


@grader(repairable=("answer",))
def verify(ctx, *, text, answer):
    if answer == (text + "!" if text == "b" else text):
        return Pass()
    return Repair(ctx.ref("answer"), "Append ! to b")


@flow
async def check(ctx, *, text, answer):
    await verify(text=text, answer=answer)
    return answer


@flow
async def demo(ctx, *, texts, language):
    answers = await produce.map(text=texts, language=language)
    return await check.map(text=texts, answer=answers)


async def main():
    runner = Runner(backend=CodexBackend(model=os.getenv("CODEX_MODEL")))
    refs = await runner.run(
        demo, texts=keyed({k: k for k in "abc"}), language="English"
    )
    print({k: runner.store.read(ref) for k, ref in refs.items()})


if __name__ == "__main__":
    asyncio.run(main())
