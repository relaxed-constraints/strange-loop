"""Run with python -m examples.replay from the checkout."""
import asyncio

from strange_loop.replay import Loop, batch_stage, stage

SOURCES = {"q01": "list orders", "q02": "count order lines", "q03": "list customers"}
COUNT_ORDERS = "SELECT RAW COUNT(*) FROM orders"
COUNT_LINES = "SELECT RAW COUNT(*) FROM orders AS o UNNEST o.lines AS line"


BAD_CUSTOMERS = "SELECT RAW c FORM customers AS c"
CUSTOMERS = "SELECT RAW c FROM customers AS c"
SYNTAX_FEEDBACK = "Replace FORM with FROM."


@batch_stage(revisable=True)
async def translate(batch):
    drafts = {
        "list orders": "SELECT RAW o FROM orders AS o",
        "count order lines": COUNT_ORDERS,
        "list customers": BAD_CUSTOMERS,
    }
    interrupted = {item.key for item in batch} == {"q01", "q02", "q03"}
    for item in batch:
        if interrupted and item.key == "q02":
            continue
        if item.request is not None:
            request = item.request
            assert (item.inputs["source"], request.previous, request.feedback) == (
                "list customers", BAD_CUSTOMERS, SYNTAX_FEEDBACK
            )
            sql = request.previous.replace(" FORM ", " FROM ")
        else:
            sql = drafts[item.inputs["source"]]
        await item.submit(sql)
    if interrupted:
        raise TimeoutError("simulated interruption")


@stage
def check(source, query):
    if source == "count order lines" and query == COUNT_ORDERS:
        return "counted orders instead of lines"
    if source == "list customers" and query != CUSTOMERS:
        return SYNTAX_FEEDBACK
    return "pass"


@stage
def investigate(source, query, verdict):
    assert (source, query, verdict) == (
        "count order lines", COUNT_ORDERS, "counted orders instead of lines"
    )
    return COUNT_LINES


async def translate_query(ctx, source):
    query = await translate(source=source)
    verdict = await check(source=source, query=query)
    if verdict.value == "pass":
        return query
    if verdict.value == SYNTAX_FEEDBACK:
        return ctx.revise(query, feedback=verdict)
    fixed = await investigate(source=source, query=query, verdict=verdict)
    return ctx.replace(query, with_=fixed)


async def main():
    run = Loop()
    results = await run.map(translate_query, source=SOURCES)
    print(run.trace("batch"))
    for key in ("q02", "q03"):
        print(f"\n{key}")
        print(run.trace(key))
        revised = results[key]
        original = run.versions[revised.previous]
        print(f"{original.id} ({original.actor}): {original.value}")
        print(f"{revised.id} ({revised.actor}): {revised.value}")
        if revised.adopted_from:
            print("Adopted from:", revised.adopted_from)
        else:
            feedback = revised.request.feedback
            print(f"Feedback from {feedback.id}: {feedback.value}")


if __name__ == "__main__":
    asyncio.run(main())
