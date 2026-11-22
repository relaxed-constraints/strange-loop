import tempfile
import unittest
from collections import Counter
from pathlib import Path

from strange_loop import (
    AgentStage,
    Batch,
    Pass,
    Repair,
    Runner,
    flow,
    grader,
    keyed,
    stage,
)


class Backend:
    def __init__(self, scenario):
        self.scenario = scenario
        self.batches = []
        self.repairs = []
        self.prompts = []

    async def generate(self, node, entries, lineage, submit):
        if node.name == "schema":
            for key, entry in entries.items():
                submit(key, 2 if "previous" in entry else 1)
            return
        self.prompts.append(node.instructions)
        self.batches.append(list(entries))
        for key, entry in entries.items():
            if self.scenario == "interrupt" and len(self.batches) == 1 and key == "c":
                raise ConnectionError("turn interrupted")
            design = entry["inputs"]["design"]
            if "previous" in entry:
                old = lineage[entry["previous"]]["inputs"]["design"]
                self.repairs.append((lineage[old]["value"], lineage[design]["value"]))
            submit(key, f"{key}-{lineage[design]['value']}-{'previous' in entry}")


class Contract(unittest.IsolatedAsyncioTestCase):
    async def exercise(self, scenario, repair_instructions=None):
        backend = Backend(scenario)
        grades, independent = Counter(), []
        schema = AgentStage(name="schema", instructions="design", output=int)
        query = AgentStage(
            name="query", instructions="translate", output=str, batching=Batch(3),
            repair_instructions=repair_instructions,
        )

        @stage
        def baseline(case):
            independent.append(case)
            return case

        @grader(repairable=("query", "design"))
        def verify(ctx, *, query, design, case):
            grades[case] += 1
            if design == 1:
                if (
                    scenario in ("single", "both")
                    and case == "a"
                    and query.endswith("False")
                ):
                    return Repair(ctx.ref("query"), "fix a")
                if scenario in ("shared", "both") and case == "b":
                    return Repair(ctx.ref("design"), "missing field")
            return Pass()

        @flow
        async def child(ctx, *, query, design, case):
            await verify(query=query, design=design, case=case)
            return query

        @flow
        async def outer(ctx, *, cases):
            design = await schema()
            expected = await baseline.map(case=cases)
            queries = await query.map(case=cases, design=design)
            return await child.map(query=queries, design=design, case=expected)

        with tempfile.TemporaryDirectory() as tmp:
            runner = Runner(backend=backend, store=Path(tmp) / "run.sqlite")
            out = await runner.run(outer, cases=keyed({k: k for k in "abc"}))
            self.assertEqual(set(out), set("abc"))
            self.assertEqual(independent, list("abc"))
            for ref in out.values():
                self.assertTrue(runner.store.consumers(ref))
            runner.store.db.close()
        return backend, grades

    async def test_item_repair(self):
        backend, grades = await self.exercise("single")
        self.assertEqual(backend.batches, [list("abc"), ["a"]])
        self.assertEqual(backend.prompts, ["translate", "translate"])
        self.assertEqual(grades, {"a": 2, "b": 1, "c": 1})

    async def test_shared_repair(self):
        backend, grades = await self.exercise("shared")
        self.assertEqual(backend.batches, [list("abc")] * 2)
        self.assertEqual(grades, dict.fromkeys("abc", 2))

    async def test_interrupted_batch(self):
        backend, grades = await self.exercise("interrupt")
        self.assertEqual(backend.batches, [list("abc"), ["c"]])
        self.assertEqual(grades, dict.fromkeys("abc", 1))

    async def test_old_and_current_inputs(self):
        backend, _ = await self.exercise("both")
        self.assertEqual(backend.repairs, [(1, 2)])

    async def test_direct_repair_and_changed_inputs_use_distinct_prompts(self):
        backend, _ = await self.exercise("both", repair_instructions="revise")
        self.assertEqual(backend.batches, [list("abc"), ["a"], ["b", "c"]])
        self.assertEqual(backend.prompts, ["translate", "revise", "translate"])
