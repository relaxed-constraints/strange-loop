import asyncio
import tempfile
import unittest
from pathlib import Path

from strange_loop.replay import Collection, Loop, Version, batch_stage, stage


class VersionIsolationTests(unittest.IsolatedAsyncioTestCase):
    async def test_two_repairs_share_a_draft_without_sharing_mutations(self):
        for mode in ("memory", "sqlite", "resume"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                path = None if mode == "memory" else Path(directory) / "runs.sqlite"
                edited = asyncio.Event()
                pause = mode == "resume"
                original = {"doc": {"name": 42}}
                metadata = {"messages": ["draft"]}

                @stage
                def draft(source):
                    return Version(source, attachments={"trace": metadata})

                @stage
                async def first(query):
                    query["doc"]["name"] = "42"
                    edited.set()
                    return query

                @stage
                async def second(query):
                    await edited.wait()
                    self.assertEqual(query["doc"]["name"], 42)
                    query["doc"]["reviewed"] = True
                    return query

                async def workflow(ctx, source):
                    saved = await draft(source=source)
                    original["doc"]["name"] = "changed by caller"
                    metadata["messages"].append("changed by caller")
                    saved.value["doc"].clear()
                    saved.attachments["trace"]["messages"].clear()
                    if pause:
                        raise RuntimeError("stop after committing the draft")
                    a, b = await asyncio.gather(first(query=saved), second(query=saved))
                    return Collection(draft=saved, first=a, second=b)

                loop = Loop(store=path)
                if pause:
                    with self.assertRaises(ExceptionGroup):
                        await loop.run(workflow, source=original, run_id="branches")
                    pause = False
                    loop = Loop(store=path).load("branches")
                    await loop.resume(workflow)
                else:
                    await loop.run(workflow, source=original, run_id="branches")

                readers = [loop]
                if path:
                    readers.append(Loop(store=path).load("branches"))
                for reader in readers:
                    versions = reader.result
                    self.assertEqual(versions["first"].value, {"doc": {"name": "42"}})
                    self.assertEqual(versions["second"].value, {"doc": {"name": 42, "reviewed": True}})
                    self.assertEqual(versions["draft"].value, {"doc": {"name": 42}})
                    self.assertEqual(versions["draft"].attachments["trace"], {"messages": ["draft"]})
                    for branch in ("first", "second"):
                        self.assertIs(reader.input(versions[branch], "query"), versions["draft"])
                    self.assertEqual(reader.input(versions["draft"], "source").value, {"doc": {"name": 42}})

    async def test_batch_producer_can_reuse_a_submission_buffer(self):
        @batch_stage
        async def build(batch):
            buffer = {"keys": []}
            for item in batch:
                buffer["keys"].append(item.key)
                await item.submit(buffer)
            buffer["keys"].clear()

        async def workflow(ctx, source):
            return await build(source=source)

        for path_mode in ("memory", "sqlite"):
            with self.subTest(path_mode=path_mode), tempfile.TemporaryDirectory() as directory:
                path = None if path_mode == "memory" else Path(directory) / "runs.sqlite"
                loop = Loop(store=path)
                result = await loop.map(workflow, source={"a": 1, "b": 2})
                self.assertEqual(result["a"].value, {"keys": ["a"]})
                self.assertEqual(result["b"].value, {"keys": ["a", "b"]})
