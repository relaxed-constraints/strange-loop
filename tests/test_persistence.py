import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from examples.workload import benchmark
from strange_loop.replay import Loop


class PersistenceTests(unittest.IsolatedAsyncioTestCase):
    def test_partial_batch_survives_process_loss(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runs.sqlite"
            cmd = [sys.executable, "-m", "examples.persistence"]
            self.assertEqual(subprocess.run([*cmd, "interrupt", path]).returncode, 75)
            store = Loop(store=path)
            self.assertEqual(store.runs(), ["three-queries"])
            partial = store.load("three-queries")
            self.assertEqual(partial.status, "incomplete")
            self.assertEqual([v.value for v in partial.versions.values() if v.actor == "uppercase"],
                             ["ONE", "THREE"])
            subprocess.run([*cmd, "resume", path], check=True, capture_output=True)
            finished = store.load("three-queries")
            self.assertEqual(finished.batches, [("uppercase", ("q01", "q02", "q03")),
                                                ("uppercase", ("q02",))])
            self.assertEqual([v.value for v in finished.result.values()], ["ONE", "TWO", "THREE"])
            self.assertEqual(finished.input(finished.result["q02"], "text").value, "two")

    async def test_completed_workload_round_trip(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runs.sqlite"
            loop = Loop(store=path)
            await loop.run(benchmark, dataset="orders fixture", run_id="example")
            saved = loop.load("example")
            self.assertEqual(saved.trace(benchmark), loop.trace(benchmark))
            answer = await saved.resume(benchmark)
            self.assertEqual(answer.value, 50)
            sql = saved.input(answer, "queries")["q02"]
            self.assertIsNotNone(sql.adopted_from)
            self.assertIsNotNone(sql.previous)
            self.assertEqual(saved.trace(), loop.trace())
            with self.assertRaisesRegex(ValueError, "already exists"):
                await Loop(store=path).run(benchmark, dataset="other", run_id="example")
