import tempfile
import unittest
from pathlib import Path

from examples.failures import translate_query
from strange_loop import Loop, batch_stage, stage


class FailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_three_queries_persist_and_resume(self):
        with tempfile.TemporaryDirectory() as tmp:
            loop = Loop(store=Path(tmp) / "runs.sqlite")
            await loop.map(
                translate_query,
                max_revisions=2,
                run_id="demo",
                source={
                    "q01": "list orders",
                    "q02": "count orders",
                    "q03": "forecast orders",
                },
            )
            loaded = loop.load("demo")
            results = await loaded.resume(translate_query)
            self.assertEqual(list(results.succeeded), ["q01"])
            self.assertEqual(list(results.failed), ["q02", "q03"])
            exhausted, unsupported = results.failed.values()
            self.assertEqual(exhausted.kind, "revision_budget_exhausted")
            self.assertEqual(exhausted.target.id, "q02/translate/1@3")
            self.assertEqual(exhausted.evidence.value, "Missing FROM clause")
            self.assertEqual(unsupported.kind, "explicit")
            self.assertEqual(unsupported.evidence.value, "Unsupported query")
            self.assertEqual(loaded.trace(), loop.trace())

    async def test_partial_batch_exhaustion_preserves_submission(self):
        @batch_stage
        async def partial(batch):
            for item in batch:
                if item.key == "ok":
                    await item.submit(1)
            raise TimeoutError()

        async def flow(ctx, source):
            return await partial(source=source)

        loop = Loop(attempts=2)
        result = await loop.map(flow, source={"ok": 1, "slow": 2})
        self.assertEqual(loop.batches[-1][1], ("slow",))
        self.assertEqual(result["ok"].value, 1)
        self.assertEqual(result["slow"].kind, "attempt_budget_exhausted")

    async def test_parent_failure_keeps_child_cause(self):
        async def workload(ctx, source):
            children = await ctx.map(translate_query, source=source)
            if children.failed:
                return ctx.fail("Incomplete workload", causes=children.failed)
            return source

        with tempfile.TemporaryDirectory() as tmp:
            loop = Loop(store=Path(tmp) / "runs.sqlite")
            await loop.run(
                workload, source={"q03": "forecast orders"}, run_id="workload"
            )
            failure = loop.load("workload").result
            self.assertEqual(failure.causes["q03"].target.value, "")

    async def test_single_stage_retries_transient_errors(self):
        calls = []

        @stage
        def read(source):
            calls.append(source)
            raise ConnectionError()

        async def flow(ctx, source):
            return await read(source=source)

        failure = await Loop(attempts=2).run(flow, source=42)
        self.assertEqual(calls, [42, 42])
        self.assertEqual(failure.kind, "attempt_budget_exhausted")
