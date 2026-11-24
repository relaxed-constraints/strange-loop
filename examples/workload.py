"""Run with python -m examples.workload. All timings are simulated."""
import asyncio

from strange_loop.replay import Loop, batch_stage, stage

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
def optimize(query, schema, verdict):
    assert query == SCAN and schema[2:] == ("lowercase", "indexed") and not verdict
    return INDEXED


async def benchmark(ctx, dataset):
    layout = await schema(dataset=dataset)
    queries = await generate_queries(schema=layout)
    translations = await ctx.map(translate_query, source=queries)
    elapsed = await time_workload(queries=translations)
    passed = await grade(elapsed=elapsed, threshold=75)
    if passed.value:
        return elapsed

    slow = translations["q02"]  # Known bottleneck in this deterministic fixture.
    fixed = await optimize(query=slow, schema=layout, verdict=passed)
    return ctx.replace(slow, with_=fixed)


async def main():
    run = Loop()
    await run.map(benchmark, source={"workload": "orders fixture"})
    print(run.trace())
    for ref in run.versions.values():
        if ref.actor == "time_workload":
            print(f"{ref.value} ms", dict(ref.inputs)["queries"][1])
        elif ref.actor == "grade":
            print("threshold passed:", ref.value)


if __name__ == "__main__":
    asyncio.run(main())
