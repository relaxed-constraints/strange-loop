"""Run with python -m examples.nested_queries."""

import asyncio

from strange_loop import Context, Loop, MapResult, Replacement, Version, flow, stage


@stage
def generate_queries(schema: dict[str, str]) -> dict[str, dict]:
    return {f"q{i:02}": {"document": {field: 1}} for i, field in enumerate(schema, 1)}


@stage
def explain(query: dict) -> str | None:
    doc = query["document"]
    if "name" in doc and not isinstance(doc["name"], str):
        return "name must be a string"
    return None


@stage
def fix_query(query: dict, feedback: str) -> dict:
    assert feedback == "name must be a string"
    return {
        **query,
        "document": {**query["document"], "name": str(query["document"]["name"])},
    }


@flow
async def check_query(ctx: Context, query: Version) -> Version | Replacement:
    verdict = await explain(query=query)
    if verdict.value is None:
        return query
    fixed = await fix_query(query=query, feedback=verdict)
    return ctx.replace(query, with_=fixed)


@flow
async def benchmark(ctx: Context, schema: Version) -> MapResult:
    queries = await generate_queries(schema=schema)
    return await ctx.map(check_query, query=queries)


async def main() -> None:
    loop = Loop()
    await loop.run(benchmark, schema={"_id": "int", "name": "str", "age": "int"})
    print(loop.trace())


if __name__ == "__main__":
    asyncio.run(main())
