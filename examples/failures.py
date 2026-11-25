"""Run with python -m examples.failures."""

import asyncio

from strange_loop import Context, Failure, Loop, Revise, RevisionRequest, Version, stage


@stage(revisable=True)
def translate(query: str, *, request: RevisionRequest | None) -> str:
    # This deterministic translator cannot improve the draft when asked to revise.
    return {
        "list orders": "SELECT * FROM orders",
        "count orders": "SELECT count(*)",
    }.get(query, "")


@stage
def check(sql: str) -> str:
    if not sql:
        return "Unsupported query"
    return "Missing FROM clause" if "FROM" not in sql else "OK"


async def translate_query(ctx: Context, source: Version) -> Version | Failure | Revise:
    sql = await translate(query=source)
    verdict = await check(sql=sql)
    match verdict.value:
        case "OK":
            return sql
        case "Unsupported query":
            return ctx.fail(verdict.value, target=sql, evidence=verdict)
        case _:
            return ctx.revise(sql, feedback=verdict)


async def main() -> None:
    loop = Loop()
    results = await loop.map(
        translate_query,
        max_revisions=2,
        source={
            "q01": "list orders",
            "q02": "count orders",
            "q03": "forecast orders",
        },
    )
    for key, version in results.succeeded.items():
        print(key, version.value)
    for key, failure in results.failed.items():
        print(key, failure.kind, failure.reason, failure.target.id)
    print(loop.trace())


if __name__ == "__main__":
    asyncio.run(main())
