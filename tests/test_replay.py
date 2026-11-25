import unittest

from examples.replay import (
    BAD_CUSTOMERS,
    COUNT_LINES,
    CUSTOMERS,
    SOURCES,
    SYNTAX_FEEDBACK,
    translate_query,
)
from strange_loop import (
    Batch,
    BatchContractError,
    Loop,
    RevisionRequest,
    batch_stage,
    stage,
)


class ReplayTests(unittest.IsolatedAsyncioTestCase):
    async def test_batch_replacement_and_replay_lineage(self):
        run = Loop()
        results = await run.map(translate_query, source=SOURCES)
        self.assertEqual(
            run.batches,
            [
                ("translate", tuple(SOURCES)),
                ("translate", ("q02",)),
                ("translate", ("q03",)),
            ],
        )
        full = run.trace().splitlines()
        self.assertEqual(
            full,
            [
                f"{i:02}  {key}  {message}"
                for i, (key, message) in enumerate(run.events, 1)
            ],
        )
        self.assertEqual(
            run.trace("q02").splitlines(),
            [message for key, message in run.events if key == "q02"],
        )
        self.assertLess(
            run.events.index(("batch", "retry translate(): q02")),
            run.events.index(("batch", "translate() draft attempt 2: q02")),
        )
        self.assertNotIn("replay", run.trace("q01"))
        self.assertIsNone(results["q01"].previous)

        winner = results["q02"]
        original = run.versions[winner.previous]
        proposal = run.versions[winner.adopted_from]
        self.assertEqual((winner.value, winner.actor), (COUNT_LINES, "translate_query"))
        self.assertEqual(proposal.actor, "investigate")
        self.assertEqual(dict(proposal.inputs)["query"], ("ref", original.id))
        self.assertEqual(original.actor, "translate")
        checks = [ref for ref in run.versions.values() if ref.actor == "check"]
        self.assertEqual(len(checks), 5)
        checked = {dict(ref.inputs)["query"][1]: ref.value for ref in checks}
        self.assertEqual(checked[original.id], "counted orders instead of lines")
        self.assertEqual(checked[winner.id], "pass")
        self.assertIn(f"reuse translate() -> {winner.id}", run.trace("q02"))

        revised = results["q03"]
        prior = run.versions[revised.previous]
        self.assertEqual(prior.value, BAD_CUSTOMERS)
        self.assertEqual((revised.actor, revised.value), ("translate", CUSTOMERS))
        self.assertIsNone(revised.adopted_from)
        self.assertIs(revised.request.target, prior)
        feedback = revised.request.feedback
        self.assertEqual((feedback.actor, feedback.value), ("check", SYNTAX_FEEDBACK))
        self.assertEqual(dict(feedback.inputs)["query"], ("ref", prior.id))
        self.assertEqual(revised.inputs, prior.inputs)
        self.assertEqual(checked[revised.id], "pass")
        self.assertIn(f"reuse translate() -> {revised.id}", run.trace("q03"))

    async def test_root_flow_revises_single_item_producer(self):
        received = []

        @stage(revisable=True)
        def draft(source, *, request: RevisionRequest | None):
            received.append((source, request))
            return source if request is None else request.previous.upper()

        async def workflow(ctx, source):
            result = await draft(source=source)
            if result.value.islower():
                return ctx.revise(result, feedback="uppercase")
            return result

        loop = Loop()
        result = await loop.run(workflow, source="example")
        self.assertEqual(loop.trace(workflow), loop.trace("workflow"))
        self.assertIn("finish draft() revise", loop.trace(draft))
        self.assertEqual(result.value, "EXAMPLE")
        self.assertEqual(
            received,
            [("example", None), ("example", RevisionRequest("example", "uppercase"))],
        )

    async def test_mixed_batch_delivery_preserves_only_submitted_versions(self):
        for returned in ({}, {"q01": 1}, {"q02": 2}):
            with self.subTest(returned=returned):
                await self._check_mixed_delivery(returned)

    async def _check_mixed_delivery(self, returned):
        @batch_stage
        async def mixed(batch):
            self.assertIsInstance(batch, Batch)
            await batch["q01"].submit(1)
            return returned

        async def flow(ctx, source):
            return await mixed(source=source)

        loop = Loop()
        with self.assertRaises(ExceptionGroup) as failure:
            await loop.map(flow, source={"q01": 1, "q02": 2})
        self.assertIsInstance(failure.exception.exceptions[0], BatchContractError)
        self.assertEqual(loop.batches, [("mixed", ("q01", "q02"))])
        accepted = [r for r in loop.versions.values() if r.actor == "mixed"]
        self.assertEqual([r.value for r in accepted], [1])
