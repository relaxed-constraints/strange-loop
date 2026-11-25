"""Run with python -m examples.workload. All timings are simulated."""
import argparse
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

from strange_loop.replay import Loop, batch_stage, flow, stage

SCAN = "SELECT RAW COUNT(*) FROM orders WHERE LOWER(status) = 'open'"
INDEXED = "SELECT RAW COUNT(*) FROM orders WHERE status = 'open'"


@stage
def schema(dataset):
    # The fixture stores status in lowercase and has an index on that field.
    return ("orders", "status", "lowercase", "indexed")


@stage
def generate_queries(schema):
    table = schema[0]
    return {
        "q01": f"count all {table}",
        "q02": f"count open {table}",
        "q03": f"list {table}",
    }


@batch_stage
def translate(batch):
    sql = {
        "count all orders": "SELECT RAW COUNT(*) FROM orders",
        "count open orders": SCAN,
        "list orders": "SELECT RAW o FROM orders AS o",
    }
    return {item.key: sql[item.inputs["source"]] for item in batch}


@flow
async def translate_query(ctx, source):
    return await translate(source=source)


@stage
def time_workload(queries):
    costs = {SCAN: 90, INDEXED: 10, "SELECT RAW COUNT(*) FROM orders": 15,
             "SELECT RAW o FROM orders AS o": 25}
    return sum(costs[sql] for sql in queries.values())


@stage
def grade(elapsed, threshold):
    return elapsed <= threshold


@stage
def diagnose(query, schema, verdict):
    assert query == SCAN and schema[2:] == ("lowercase", "indexed") and not verdict
    return "status is stored lowercase; compare it directly to use the status index."


@stage
def optimize(query, diagnosis):
    assert query == SCAN and diagnosis
    return INDEXED


@flow
async def benchmark(ctx, dataset):
    layout = await schema(dataset=dataset)
    queries = await generate_queries(schema=layout)
    translations = await ctx.map(translate_query, source=queries)
    elapsed = await time_workload(queries=translations)
    passed = await grade(elapsed=elapsed, threshold=75)
    if passed.value:
        return elapsed

    slow = translations["q02"]  # Known bottleneck in this deterministic fixture.
    reason = await diagnose(query=slow, schema=layout, verdict=passed)
    fixed = await optimize(query=slow, diagnosis=reason)
    return ctx.replace(slow, with_=fixed)


def show_lineage(saved: Loop) -> None:
    final = saved.result
    revised = saved.input(final, "queries")["q02"]
    original = saved.versions[revised.previous]
    fix = saved.versions[revised.adopted_from]
    diagnosis = saved.input(fix, "diagnosis")
    verdict = saved.input(diagnosis, "verdict")
    first = saved.input(verdict, "elapsed")

    for timing in (first, final):
        print(f"{timing.id} = {timing.value} ms")
        for key, query in saved.input(timing, "queries").items():
            print(f"  {key}: {query.id}")
    print(f"Failed grade: {first.value} ms exceeds {saved.input(verdict, 'threshold')} ms")
    print(f"{diagnosis.actor}(): {diagnosis.value}")
    print(f"Previous SQL: {original.value}")
    print(f"{revised.actor} adopted {fix.actor}() output {fix.id}: {revised.value}")


async def demo(path: Path, inspect_only: bool) -> None:
    if not inspect_only:
        await Loop(store=path).run(benchmark, dataset="orders fixture", run_id="workload")
    show_lineage(Loop(store=path).load("workload"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path, nargs="?")
    parser.add_argument("--inspect", action="store_true", help="read a previously saved workload")
    args = parser.parse_args()
    if args.database:
        asyncio.run(demo(args.database, args.inspect))
    elif args.inspect:
        parser.error("--inspect requires a database path")
    else:
        with TemporaryDirectory() as directory:
            asyncio.run(demo(Path(directory) / "runs.sqlite", False))
