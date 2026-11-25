import unittest

from examples.workload import benchmark
from strange_loop import Loop


class WorkloadTests(unittest.IsolatedAsyncioTestCase):
    async def test_targeted_replacement_invalidates_full_workload(self):
        run = Loop()
        results = await run.map(benchmark, source={"workload": "orders fixture"})

        timings = [r for r in run.versions.values() if r.actor == "time_workload"]
        grades = [r for r in run.versions.values() if r.actor == "grade"]
        self.assertEqual([r.value for r in timings], [130, 50])
        self.assertEqual([r.value for r in grades], [False, True])
        self.assertIs(results["workload"], timings[1])
        for timing, grade in zip(timings, grades, strict=True):
            self.assertEqual(run.inputs(grade), {"elapsed": timing, "threshold": 75})

        before, after = [run.input(r, "queries") for r in timings]
        self.assertEqual(list(before), ["q01", "q02", "q03"])
        self.assertEqual(list(after), list(before))
        self.assertEqual(before["q01"], after["q01"])
        self.assertEqual(before["q03"], after["q03"])
        replacement = after["q02"]
        self.assertEqual(replacement.previous, before["q02"].id)
        self.assertEqual(run.versions[replacement.adopted_from].actor, "optimize")
        self.assertEqual(len(run.batches), 1)
        self.assertEqual(len(run.batches[0][1]), 3)
