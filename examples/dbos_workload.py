# /// script
# requires-python = ">=3.12"
# dependencies = ["dbos==3.7.0"]
# ///
"""Run with uv run examples/dbos_workload.py {benchmark,batch} DIRECTORY.

For the interruption experiment use a fresh directory and add --interrupt to
the first batch invocation. Run it again without the flag to recover.
"""
import argparse
import json
import os
import sqlite3
from contextlib import closing
from pathlib import Path

from dbos import DBOS, SetWorkflowID

SCAN = "SELECT count(*) FROM orders WHERE lower(status) = 'open'"
INDEXED = "SELECT count(*) FROM orders WHERE status = 'open'"


@DBOS.step()
def generate(table: str) -> dict[str, str]:
    return {"q01": f"SELECT count(*) FROM {table}", "q02": SCAN.replace("FROM ", ""),
            "q03": f"SELECT * FROM {table}"}


@DBOS.step()
def explain(sql: str) -> str | None:
    return None if " FROM " in sql else "Missing FROM"


@DBOS.step()
def fix(sql: str, feedback: str) -> str:
    assert feedback == "Missing FROM"
    return sql.replace(" orders", " FROM orders", 1)


@DBOS.workflow()
def check_query(key: str, sql: str, budget: int) -> dict:
    versions = [dict(id=f"{key}@1", sql=sql, actor="generate")]
    for attempt in range(budget + 1):
        feedback = explain(sql)
        if feedback is None or attempt == budget:
            return dict(versions=versions, error=feedback)
        sql = fix(sql, feedback)
        versions.append(dict(id=f"{key}@{attempt + 2}", sql=sql, actor="fix",
                             previous=versions[-1]["id"], feedback=feedback))


@DBOS.step()
def time_workload(current: dict) -> dict:
    costs = {SCAN: 90, INDEXED: 10, "SELECT * FROM orders": 25, "SELECT count(*) FROM orders": 15}
    return dict(ms=sum(costs[v["sql"]] for v in current.values()),
                inputs={key: v["id"] for key, v in current.items()})


@DBOS.step()
def investigate(sql: str, elapsed: int) -> str:
    assert sql == SCAN and elapsed > 75
    return INDEXED


@DBOS.workflow()
def benchmark(budget: int) -> dict:
    handles = {k: DBOS.enqueue_workflow("checks", check_query, k, sql, budget)
               for k, sql in generate("orders").items()}
    checked = {k: h.get_result() for k, h in handles.items()}
    timings = []
    if all(row["error"] is None for row in checked.values()):
        current = {k: row["versions"][-1] for k, row in checked.items()}
        timings.append(time_workload(current))
        if timings[0]["ms"] > 75:
            # q02 is the known bottleneck in this fixture.
            slow = current["q02"]
            sql = investigate(slow["sql"], timings[0]["ms"])
            history = checked["q02"]["versions"]
            current["q02"] = dict(id=f"q02@{len(history)+1}", sql=sql, actor="investigate",
                                  previous=slow["id"], timing=0)
            history.append(current["q02"])
            timings.append(time_workload(current))
    return dict(queries=checked, timings=timings)


@DBOS.step()
def streaming_batch(ledger: str, keys: list[str]) -> dict:
    # The whole provider call is one DBOS step. This application-owned ledger
    # closes the gap between receiving an item and completing the step.
    with closing(sqlite3.connect(ledger)) as db:
        db.execute("CREATE TABLE IF NOT EXISTS submissions (key TEXT PRIMARY KEY, value TEXT)")
        accepted = dict(db.execute("SELECT key,value FROM submissions"))
        pending = [key for key in keys if key not in accepted]
        if pending:
            print("Provider request:", pending, flush=True)
            for key in sorted(pending, key=lambda k: k == "q02"):
                with db:
                    db.execute("INSERT INTO submissions VALUES (?,?)", (key, key.upper()))
                if key == "q03" and os.environ.get("DBOS_DEMO_INTERRUPT") == "1":
                    os._exit(75)
        return dict(db.execute("SELECT key,value FROM submissions ORDER BY key"))


@DBOS.workflow()
def batch(ledger: str) -> dict:
    return streaming_batch(ledger, ["q01", "q02", "q03"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("flow", choices=["benchmark", "batch"])
    parser.add_argument("directory", type=Path, help="one isolated experiment per directory")
    parser.add_argument("--interrupt", action="store_true")
    parser.add_argument("--budget", type=int, default=2)
    args = parser.parse_args()
    directory = args.directory.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    os.environ["DBOS_DEMO_INTERRUPT"] = "1" if args.interrupt else "0"
    DBOS(config={"name": "query-experiment", "application_version": "0.1.0",
                 "system_database_url": f"sqlite:///{directory / 'dbos.sqlite'}",
                 "run_admin_server": False})
    DBOS.launch()
    try:
        DBOS.register_queue("checks", worker_concurrency=3)
        with SetWorkflowID(f"benchmark-{args.budget}" if args.flow == "benchmark" else "batch"):
            handle = (DBOS.start_workflow(benchmark, args.budget) if args.flow == "benchmark"
                      else DBOS.start_workflow(batch, str(directory / "submissions.sqlite")))
        print(json.dumps(handle.get_result(), indent=2))
    finally:
        DBOS.destroy()


if __name__ == "__main__":
    main()
