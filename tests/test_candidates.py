import unittest
from collections import Counter

from strange_loop import (
    AgentStage,
    Batch,
    Fail,
    Pass,
    Repair,
    Runner,
    flow,
    grader,
    keyed,
    stage,
)


class Backend:
    def __init__(self):
        self.generated = Counter()
        self.batches = []

    async def generate(self, node, entries, lineage, submit):
        self.batches.append((node.name, list(entries)))
        for key, entry in entries.items():
            if node.name == "generate":
                self.generated[key] += 1
                submit(key, f"{key}/{self.generated[key]}")
            else:
                query = lineage[entry["inputs"]["query"]]["value"]
                submit(key, query + ("/revised" if "previous" in entry else ""))


class CandidateTests(unittest.IsolatedAsyncioTestCase):
    async def test_replacement_gets_fresh_budget_and_peer_runs_once(self):
        backend = Backend()
        runner = Runner(backend=backend, max_repairs=0)
        generate = AgentStage(name="generate", instructions="", output=str, batching=Batch(8))
        translate = AgentStage(
            name="translate", instructions="", output=str, batching=Batch(8), max_repairs=3
        )
        evaluated = Counter()

        @grader(repairable=("translation",))
        def syntax(ctx, translation):
            if translation == "q04/1":
                return Repair(ctx.ref("translation"), "syntax error")
            return Pass()

        @grader(repairable=("translation",))
        def evaluate(ctx, query, translation):
            evaluated[query] += 1
            if query == "q04/1" or translation == "q04/2":
                return Repair(ctx.ref("translation"), "result mismatch")
            return Pass()

        @flow
        async def workload(ctx, cases):
            async with ctx.candidates(cases, max_candidates=2, on_failure="replace") as loop:
                async for pending in loop:
                    queries = await generate.map(case=pending)
                    translations = await translate.map(query=queries)
                    checks = await syntax.map(translation=translations)
                    valid = checks.passed("translation")
                    final = await evaluate.map(query=queries.select(valid), translation=valid)
                    loop.accept(final.passed("translation"))
            return loop.report()

        report = await runner.run(workload, cases=keyed(q04="complex", q05="simple"))
        self.assertEqual(backend.generated, {"q04": 2, "q05": 1})
        self.assertEqual(evaluated, {"q04/1": 3, "q04/2": 2, "q05/1": 1})
        self.assertEqual(backend.batches.count(("translate", ["q04", "q05"])), 1)
        first, second = report.history["q04"]
        self.assertEqual(first.repairs, {"translate": 3})
        self.assertEqual(first.failure.reason, "repair_limit")
        self.assertEqual(first.failure.grader, "evaluate")
        inputs = runner.store.inputs(first.failure.verdict)
        self.assertEqual(runner.store.read(inputs["query"]), "q04/1")
        self.assertEqual(second.repairs, {"translate": 1})
        self.assertEqual(report.failures, {})
        self.assertEqual(runner.store.read(report.outputs["q04"]), "q04/2/revised")
        self.assertEqual(runner.store.read(report.outputs["q05"]), "q05/1")

    async def test_explicit_failure_and_terminal_report(self):
        runner = Runner(backend=Backend())

        @grader(repairable=())
        def check(ctx, case):
            return Fail("unsupported") if case == "bad" else Pass()

        @flow
        async def workload(ctx, cases):
            async with ctx.candidates(cases, max_candidates=2, on_failure="replace") as loop:
                async for pending in loop:
                    checked = await check.map(case=pending)
                    loop.accept(checked.passed("case"))
            return loop.report()

        report = await runner.run(workload, cases=keyed(a="bad", b="ok"))
        self.assertEqual(list(report.outputs), ["b"])
        self.assertEqual(len(report.history["a"]), 2)
        self.assertEqual(report.failures["a"], report.history["a"][1].failure)
        self.assertEqual(report.failures["a"].feedback, "unsupported")
        self.assertEqual(report.failures["a"].reason, "explicit_failure")

    async def exercise_outer_replay(self, repaired):
        backend = Backend()
        runner = Runner(backend=backend)
        calls = Counter()
        reports = []

        @stage
        def setting(name):
            calls[name] += 1
            return calls[name]

        generate = AgentStage(name="generate", instructions="", output=str)
        translate = AgentStage(name="translate", instructions="", output=str)

        @grader(repairable=())
        def evaluate(ctx, translation, policy):
            calls["evaluate"] += 1
            return Pass()

        @grader(repairable=("value",))
        def outer_check(ctx, value):
            if value == 1:
                return Repair(ctx.ref("value"), "revise")
            return Pass()

        @flow
        async def workload(ctx, cases, schema_name, policy_name, other_name):
            schema = await setting(name=schema_name)
            policy = await setting(name=policy_name)
            other = await setting(name=other_name)
            async with ctx.candidates(cases) as loop:
                async for pending in loop:
                    query = await generate.map(case=pending, schema=schema)
                    translation = await translate.map(query=query)
                    final = await evaluate.map(translation=translation, policy=policy)
                    loop.accept(final.passed("translation"))
            reports.append(loop.report())
            await outer_check(value={"schema": schema, "policy": policy, "other": other}[repaired])

        await runner.run(workload, cases=keyed(q05="simple"), schema_name="schema",
                         policy_name="policy", other_name="other")
        self.assertEqual(len(reports), 2)
        self.assertEqual(calls[repaired], 2)
        self.assertEqual(reports[0].outputs["q05"] == reports[1].outputs["q05"],
                         repaired != "schema")
        return Counter(name for name, keys in backend.batches), calls["evaluate"]

    async def test_unrelated_repair_skips_accepted_q05(self):
        self.assertEqual(await self.exercise_outer_replay("other"),
                         ({"generate": 1, "translate": 1}, 1))

    async def test_policy_change_rechecks_q05(self):
        self.assertEqual(await self.exercise_outer_replay("policy"),
                         ({"generate": 1, "translate": 1}, 2))

    async def test_schema_change_regenerates_q05(self):
        self.assertEqual(await self.exercise_outer_replay("schema"),
                         ({"generate": 2, "translate": 2}, 2))

    async def test_missing_submission_retries_only_missing_and_continues_next_batch(self):
        calls = []
        class Partial:
            async def generate(self, node, entries, lineage, submit):
                calls.append(entries)
                for key in entries:
                    if key != "q0287":
                        submit(key, key)
                return {"q0287": "Expected q0287.json; file missing."}

        producer = AgentStage(name="translate", instructions="", output=str, batching=Batch(2))
        @flow
        async def workload(ctx, cases):
            async with ctx.candidates(cases, max_candidates=1, on_failure="stop") as loop:
                async for pending in loop:
                    loop.accept(await producer.map(case=pending))
            return loop.report()

        runner = Runner(backend=Partial())
        report = await runner.run(workload, cases=keyed(q0287=1, q0288=2, q0289=3))
        self.assertEqual([list(entries) for entries in calls], [
            ["q0287", "q0288"], ["q0287"], ["q0287"], ["q0289"]
        ])
        self.assertIn("q0287.json", calls[1]["q0287"]["submission_feedback"])
        self.assertEqual(set(report.outputs), {"q0288", "q0289"})
        self.assertEqual(report.failures["q0287"].reason, "missing_submission")
        self.assertEqual(report.history["q0287"][0].repairs, {})
