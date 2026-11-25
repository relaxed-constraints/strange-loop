import tempfile
import unittest
from pathlib import Path

from strange_loop.replay import Loop, Version, stage


class AttachmentTests(unittest.IsolatedAsyncioTestCase):
    async def test_revise_reopened_target(self):
        @stage(revisable=True)
        def produce(source, *, request):
            if request is None:
                return Version(source, attachments={"turns": ["draft"]})
            turns = request.target.attachments["turns"]
            return Version("fixed", attachments={"turns": [*turns, request.feedback]})

        @stage
        def explain(sql):
            self.assertIsInstance(sql, str)
            return None if sql == "fixed" else "fix it"

        pause = True

        async def flow(ctx, source):
            draft = await produce(source=source)
            if pause:
                raise RuntimeError("interrupt fixture")
            verdict = await explain(sql=draft)
            return ctx.revise(draft, feedback=verdict) if verdict.value else draft

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runs.sqlite"
            with self.assertRaises(ExceptionGroup):
                await Loop(store=path).run(flow, source="draft", run_id="run")
            pause = False
            await Loop(store=path).load("run").resume(flow)
            result = Loop(store=path).load("run").result
            self.assertEqual(result.request.target.attachments["turns"], ["draft"])
            self.assertEqual(result.attachments["turns"], ["draft", "fix it"])
