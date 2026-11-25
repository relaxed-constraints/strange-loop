"""Run with python -m examples.batch_repair."""
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

from strange_loop.replay import Batch, Collection, Context, Failure, Loop, Version, batch_stage, flow, stage


@stage
def explain(query: dict) -> str | None:
    if "name" in query and not isinstance(query["name"], str):
        return "name must be a string"
    return None


@flow
async def check_query(ctx: Context, query: Version) -> Version | Failure:
    verdict = await explain(query=query)
    if verdict.value is None:
        return query
    return ctx.fail(verdict.value, target=query, evidence=verdict)


@batch_stage
def repair_queries(batch: Batch) -> dict[str, dict]:
    outputs = {}
    for item in batch:
        query = item.inputs["query"]
        if item.inputs["feedback"] == "name must be a string" and query["name"] >= 0:
            query = {**query, "name": str(query["name"])}
        outputs[item.key] = query
    return outputs


@flow
async def repair_and_check(ctx: Context, failure: Failure) -> Version | Failure:
    fixed = await repair_queries(query=failure.target, feedback=failure.evidence)
    return await check_query(query=fixed)


@stage
def generate_queries(names: list) -> Collection:
    return Collection({f"q{i:02}": {"name": name} for i, name in enumerate(names, 1)})


@flow
async def prepare_workload(ctx: Context, queries: Collection | None = None,
                           names: Version | None = None) -> Collection:
    if queries is None:
        queries = await generate_queries(names=names)
    checked = await ctx.map(check_query, query=queries)
    repairs = await ctx.map(repair_and_check, failure=checked.failed)
    checked.update(repairs)
    return checked


async def main() -> None:
    names = ["Alice", 42, -1, "Bob"]
    supplied = Collection({f"q{i:02}": {"name": name} for i, name in enumerate(names, 1)})
    with TemporaryDirectory() as directory:
        path = Path(directory) / "runs.sqlite"
        for label, inputs in [("supplied", {"queries": supplied}), ("generated", {"names": names})]:
            await Loop(store=path).run(prepare_workload, run_id=label, **inputs)
            saved = Loop(store=path).load(label)
            repaired = saved.result["q02"]
            source = saved.input(repaired, "query")
            print(f"{label}: {repaired.value} <- {source.actor} {source.id} {source.value}")
            if source.actor == "generate_queries":
                print("  generator input:", saved.input(source, "names").value)


if __name__ == "__main__":
    asyncio.run(main())
