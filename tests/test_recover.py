import tempfile
import unittest
from pathlib import Path

from examples.batch_repair import prepare_workload
from strange_loop.replay import Collection, Loop


class RecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_repairs_only_failed_keys_and_rejoins_in_order(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.sqlite"
            loop = Loop(store=path)
            await loop.run(prepare_workload, queries={"q01": {"age": 1}, "q02": {"name": 42},
                                                      "q03": {"name": -1}, "q04": {"age": 4}},
                           run_id="repair")
            result = Loop(store=path).load("repair").result
            self.assertEqual(list(result), ["q01", "q02", "q03", "q04"])
            self.assertEqual([v.value for v in result.succeeded.values()], [{"age": 1}, {"name": "42"}, {"age": 4}])
            self.assertEqual(result.failed["q03"].target.value, {"name": -1})
            self.assertEqual(len(loop.batches), 1)
            self.assertEqual([k.split("/")[-1] for k in loop.batches[0][1]], ["q02", "q03"])

    async def test_empty_failure_map_schedules_no_repairs(self):
        loop = Loop()
        result = await loop.run(prepare_workload, queries={"ok": {"age": 4}})
        self.assertEqual(result["ok"].value, {"age": 4})
        self.assertFalse(loop.batches)

    async def test_supplied_and_generated_member_lineage(self):
        names = ["Alice", 42, -1]
        supplied = Collection({f"q{i:02}": {"name": v} for i, v in enumerate(names, 1)})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runs.sqlite"
            for actor, inputs in [("<input>", {"queries": supplied}), ("generate_queries", {"names": names})]:
                await Loop(store=path).run(prepare_workload, run_id=actor, **inputs)
                saved = Loop(store=path).load(actor)
                self.assertNotIn("member()", saved.trace())
                fixed = saved.result["q02"]
                source = saved.input(fixed, "query")
                verdict = saved.input(fixed, "feedback")
                self.assertIs(saved.input(verdict, "query"), source)
                self.assertEqual((source.actor, source.value, fixed.value),
                                 (actor, {"name": 42}, {"name": "42"}))
                if actor == "generate_queries":
                    self.assertEqual(saved.input(source, "names").value, names)
