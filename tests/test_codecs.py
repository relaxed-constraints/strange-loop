import sqlite3
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path

from strange_loop import CustomCodec, FileCodec, Loop


async def identity(ctx, source):
    return source


@dataclass
class Number:
    n: int

    def save(self, path):
        path.write_text(str(self.n))

    @classmethod
    def load(cls, path):
        return cls(int(path.read_text()))


class CodecTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "runs.db"

    async def test_file_and_custom(self):
        for codec in [
            FileCodec(Number, name="number-file"),
            CustomCodec(
                type=Number,
                name="number-bytes",
                save=lambda v: bytes([v.n]),
                load=lambda data: Number(data[0]),
            ),
        ]:
            loop = Loop(store=self.path, codecs=[codec])
            await loop.run(identity, source=Number(17), run_id=codec.name)
            loaded = Loop(store=self.path).load(codec.name)
            self.assertEqual(loaded.trace(), loop.trace())
            with self.assertRaisesRegex(ValueError, codec.name):
                _ = loaded.result.value
            self.assertEqual(loop.load(codec.name).result.value, Number(17))

    async def test_bson_uses_pickle_fallback(self):
        try:
            from bson import Decimal128, ObjectId
        except ImportError:
            self.skipTest("requires pymongo")
        value = {"items": [ObjectId(), Decimal128("1.25")]}
        loop = Loop(store=self.path)
        await loop.map(
            identity,
            source={
                "document": value,
                "scalar": value["items"][0],
                "nested": (value, (3, 4)),
            },
            run_id="bson",
        )
        for ref in loop.load("bson").result.values():
            with self.assertRaisesRegex(ValueError, "pickle"):
                _ = ref.value
        result = Loop(store=self.path, allow_pickle=True).load("bson").result
        self.assertEqual(result["document"].value, value)
        self.assertEqual(result["scalar"].value, value["items"][0])
        self.assertEqual(result["nested"].value, (value, (3, 4)))

    async def test_scientific_values(self):
        try:
            import numpy as np
            import pandas as pd
            from matplotlib.figure import Figure
        except ImportError:
            self.skipTest("requires scientific packages")
        array = np.arange(12, dtype=">i4").reshape(3, 4)
        frame = pd.DataFrame({"kind": pd.Categorical(["a", "b"])})
        fig = Figure()
        fig.subplots().plot([1, 2], [4, 6])
        loop = Loop(store=self.path)
        await loop.map(
            identity, source={"a": array, "df": frame, "fig": fig}, run_id="science"
        )
        results = Loop(store=self.path, allow_pickle=True).load("science").result
        np.testing.assert_array_equal(results["a"].value, array, strict=True)
        pd.testing.assert_frame_equal(results["df"].value, frame)
        self.assertEqual(
            list(results["fig"].value.axes[0].lines[0].get_ydata()), [4, 6]
        )
        restricted = Loop(store=self.path).load("science")
        self.assertEqual(restricted.trace(), loop.trace())
        for ref in restricted.result.values():
            with self.assertRaisesRegex(ValueError, "pickle"):
                _ = ref.value
        with sqlite3.connect(self.path) as db:
            self.assertEqual(
                db.execute("SELECT codec FROM versions ORDER BY rowid").fetchall(),
                [("pickle",)] * 3,
            )

    async def test_default_pickle_writer_and_unpickleable_value(self):
        writer = Loop(store=self.path)
        await writer.run(identity, source=Number(42), run_id="fallback")
        restricted = writer.load("fallback")
        self.assertEqual(restricted.trace(), writer.trace())
        with self.assertRaisesRegex(ValueError, "pickle"):
            _ = restricted.result.value
        reader = Loop(store=self.path, allow_pickle=True)
        self.assertEqual(reader.load("fallback").result.value, Number(42))
        with self.assertRaisesRegex(TypeError, "pickle failed"):
            await Loop(store=self.path).run(
                identity, source=lambda: 42, run_id="invalid"
            )
        self.assertEqual(reader.runs(), ["fallback"])
