import tempfile
import unittest
from collections import Counter
from pathlib import Path

from examples.nested_queries import benchmark, check_query
from strange_loop.replay import Loop, flow


class NestedQueryTests(unittest.IsolatedAsyncioTestCase):
    async def test_only_replaced_member_is_rechecked(self):
        with tempfile.TemporaryDirectory() as directory:
            loop = Loop(store=Path(directory) / "run.sqlite")
            await loop.run(benchmark, schema={"_id": "int", "name": "str", "age": "int"})
            saved = loop.load(loop.run_id)
            checked = Counter(key.split("/")[-1] for key, message in saved.events
                              if message == "start explain()")
            self.assertEqual(checked, {"q01": 1, "q02": 2, "q03": 1})
            self.assertEqual(sum(m == "start generate_queries()" for _, m in saved.events), 1)
            self.assertEqual(sum(m == "start fix_query()" for _, m in saved.events), 1)
            self.assertEqual(list(saved.result.succeeded), ["q01", "q02", "q03"])
            repaired = saved.result["q02"]
            self.assertEqual(repaired.value["document"]["name"], "1")
            self.assertEqual(saved.versions[repaired.previous].value["document"]["name"], 1)
            self.assertEqual(saved.versions[repaired.adopted_from].actor, "fix_query")

    async def test_direct_nested_call(self):
        @flow
        async def parent(ctx, source):
            return await check_query(query=source)

        loop = Loop()
        result = await loop.run(parent, source={"document": {"name": 1}})
        self.assertEqual(result.value, {"document": {"name": "1"}})
        self.assertEqual(loop.trace(check_query).splitlines().count("replay"), 1)
