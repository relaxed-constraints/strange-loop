import asyncio
import tempfile
import unittest
from pathlib import Path

from strange_loop import Collection, Loop, StaleVersion, batch_stage, stage


class StaleRevisionTests(unittest.IsolatedAsyncioTestCase):
    async def test_competing_revisions(self):
        for batched in (False, True):
            with self.subTest(batched=batched):
                await self._check_competing_revisions(batched)

    async def _check_competing_revisions(self, batched):
        with tempfile.TemporaryDirectory() as tmp:
            slow_started = asyncio.Event()
            winner_adopted = asyncio.Event()

            @stage(revisable=True)
            async def single(source, *, request):
                if request is None:
                    return source
                if request.feedback == "slow":
                    slow_started.set()
                    await winner_adopted.wait()
                else:
                    await slow_started.wait()
                return request.feedback

            @batch_stage(revisable=True)
            async def grouped(batch):
                # Both requests are in flight before either submission.
                for item in sorted(
                    batch, key=lambda i: i.request and i.request.feedback
                ):
                    await item.submit(
                        item.request.feedback if item.request else item.inputs["source"]
                    )

            async def branch(ctx, query):
                if query.previous:
                    winner_adopted.set()
                    return query
                return ctx.revise(query, feedback=ctx.key.split("/")[-1])

            async def root(ctx, source):
                draft = await (grouped if batched else single)(source=source)
                return await ctx.map(branch, query=Collection(fast=draft, slow=draft))

            path = Path(tmp) / "runs.sqlite"
            loop = Loop(store=path)
            await asyncio.wait_for(
                loop.run(root, source="draft", run_id="race"), timeout=5
            )
            for run in (loop, Loop(store=path).load("race")):
                winner = run.result["fast"]
                conflict = run.result.failed["slow"]
                self.assertIsInstance(conflict, StaleVersion)
                self.assertIs(conflict.current, winner)
                self.assertIs(conflict.target, winner.request.target)
                self.assertEqual(conflict.evidence, "slow")
                self.assertEqual(winner.value, "fast")
                history = [
                    v for v in run.versions.values() if v.artifact == winner.artifact
                ]
                self.assertEqual([v.value for v in history], ["draft", "fast"])
                self.assertEqual(winner.previous, conflict.target.id)
