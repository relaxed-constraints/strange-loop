"""Run twice with python -m examples.persistence {interrupt,resume} STORE.sqlite."""

import argparse
import asyncio
import os
from pathlib import Path

from strange_loop import Batch, Context, Loop, Version, batch_stage

interrupt = False


@batch_stage
async def uppercase(batch: Batch) -> None:
    for item in batch:
        if interrupt and item.key == "q02":
            continue
        await item.submit(item.inputs["text"].upper())
    if interrupt:
        os._exit(75)  # Simulate process loss without cleanup.


async def shout(ctx: Context, source: Version) -> Version:
    return await uppercase(text=source)


async def main(mode: str, path: Path) -> None:
    global interrupt
    loop = Loop(store=path)
    if mode == "interrupt":
        interrupt = True
        await loop.map(
            shout,
            source={"q01": "one", "q02": "two", "q03": "three"},
            run_id="three-queries",
        )
    else:
        run = loop.load("three-queries")
        results = await run.resume(shout)
        print(run.trace())
        print({key: ref.value for key, ref in results.items()})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["interrupt", "resume"])
    parser.add_argument("store", type=Path)
    args = parser.parse_args()
    asyncio.run(main(args.mode, args.store))
