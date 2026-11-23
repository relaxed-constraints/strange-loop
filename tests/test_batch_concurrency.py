import asyncio
import unittest
from collections import Counter

from strange_loop import AgentStage, Batch, Runner, flow, keyed


def workload(policy):
    generate = AgentStage(name="generate", instructions="", output=str, batching=policy)

    @flow
    async def run(ctx, items):
        return await generate.map(item=items)

    return run


class BatchConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    def test_policy(self):
        self.assertEqual(Batch(4).concurrency, 1)
        for invalid in (0, -1, 1.5, True):
            with self.assertRaises(ValueError):
                Batch(4, concurrency=invalid)

    async def test_partial_retry_and_next_batch_run_while_first_batch_waits(self):
        started = asyncio.Event()
        last = asyncio.Event()
        attempts = []
        active = peak = 0

        class Backend:
            async def generate(self, node, entries, lineage, submit):
                nonlocal active, peak
                active += 1
                peak = max(peak, active)
                attempts.append(tuple(entries))
                try:
                    if "a" in entries:
                        started.set()
                        await last.wait()
                    elif "c" in entries:
                        await started.wait()
                        submit("c", "c")
                        return {"d": "missing"}
                    elif "e" in entries:
                        last.set()
                    for key in entries:
                        submit(key, key)
                finally:
                    active -= 1

        runner = Runner(backend=Backend())
        items = keyed({k: k for k in "abcde"})
        result = await asyncio.wait_for(
            runner.run(workload(Batch(2, concurrency=2)), items=items), 2
        )
        self.assertEqual(peak, 2)
        self.assertEqual(active, 0)
        self.assertEqual(attempts, [("a", "b"), ("c", "d"), ("d",), ("e",)])
        self.assertEqual(list(result), list(items))
        self.assertEqual(
            [runner.store.read(ref) for ref in result.values()], list(items)
        )

    async def test_fatal_error_waits_for_sibling_cleanup(self):
        started = asyncio.Event()
        cleaned = asyncio.Event()
        called = Counter()

        class Backend:
            async def generate(self, node, entries, lineage, submit):
                (key,) = entries
                called[key] += 1
                if key == "a":
                    try:
                        started.set()
                        await asyncio.Event().wait()
                    finally:
                        await asyncio.sleep(0)
                        cleaned.set()
                await started.wait()
                raise ValueError("backend failed")

        runner = Runner(backend=Backend())
        with self.assertRaises(ExceptionGroup) as caught:
            await asyncio.wait_for(
                runner.run(
                    workload(Batch(1, concurrency=2)), items=keyed(a=1, b=2, c=3)
                ),
                2,
            )
        self.assertIsInstance(caught.exception.exceptions[0], ValueError)
        self.assertTrue(cleaned.is_set())
        self.assertEqual(called, {"a": 1, "b": 1})
