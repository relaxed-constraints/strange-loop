import asyncio
import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from strange_loop import AgentStage, CodexBackend
from strange_loop.codex import _output_file, _prepare_work


class WorkspaceContract(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.lineage = {
            1: {"value": {"field": "{task}"}, "inputs": {}},
            2: {"value": "inspect", "inputs": {}},
            3: {"value": ["old"], "inputs": {}},
        }
        self.entry = {"inputs": {"schema": 1, "task": 2}}

    def test_named_inputs(self):
        prompt, work = _prepare_work(
            'Schema {schema}. Keep {{example}} and {"key": 1}.',
            {"q01": self.entry},
            self.lineage,
        )
        self.assertEqual(
            prompt, 'Schema {"field": "{task}"}. Keep {example} and {"key": 1}.'
        )
        self.assertEqual(
            work["q01"], {"output_file": "q01.json", "inputs": {"task": "inspect"}}
        )

    def test_preserve_order_in_structured_prompt_input(self):
        schema = {"z": 1, "a": 2}
        prompt, work = _prepare_work(
            "{schema}", {"": {"inputs": {"schema": 1}}}, {1: {"value": schema}}
        )
        self.assertEqual(prompt, '{"z": 1, "a": 2}')
        self.assertEqual(work[""], {"output_file": "result.json"})

    def test_invalid_batch_prompts(self):
        with self.assertRaisesRegex(ValueError, "Unknown prompt input"):
            _prepare_work("{typo}", {"a": self.entry}, self.lineage)
        with self.assertRaisesRegex(ValueError, "varies"):
            _prepare_work(
                "{task}", {"a": self.entry, "b": {"inputs": {"task": 3}}}, self.lineage
            )

    def test_safe_distinct_filenames(self):
        keys = ["", "q01", "Q01", "work", "result", "../q01", "a/b", "α"]
        files = [_output_file(key) for key in keys]
        self.assertEqual(files[:2], ["result.json", "q01.json"])
        self.assertEqual(len(set(name.casefold() for name in files)), len(keys))
        self.assertNotIn("work.json", files)
        self.assertTrue(all("/" not in name for name in files))

    async def test_repair_workspace_and_submission(self):
        observed = {}

        class Agent:
            @classmethod
            async def launch(cls, *, workspace):
                observed["workspace"] = workspace
                return cls()

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                pass

            async def send(self, message):
                workspace = observed["workspace"]
                observed["files"] = sorted(p.name for p in workspace.iterdir())
                observed["prompt"] = message
                work = json.loads((workspace / "work.json").read_text())
                observed["work"] = work
                (workspace / work[0]["output_file"]).write_text('["corrected"]')

            async def events(self):
                yield SimpleNamespace(status="completed")

        results = {}
        with patch.dict(
            sys.modules,
            codex_box=SimpleNamespace(Agent=Agent, TurnCompleted=SimpleNamespace),
        ):
            await CodexBackend().generate(
                AgentStage(name="revise", instructions="Fix {task}", output=list),
                {"q01": {**self.entry, "previous": 3, "feedback": "Use an array"}},
                self.lineage,
                results.__setitem__,
            )
        self.assertEqual(observed["files"], ["work.json"])
        self.assertIn("Fix inspect", observed["prompt"])
        self.assertEqual(
            observed["work"],
            [
                {
                    "output_file": "q01.json",
                    "inputs": {"schema": {"field": "{task}"}},
                    "previous_output": ["old"],
                    "feedback": "Use an array",
                }
            ],
        )
        self.assertEqual(results, {"q01": ["corrected"]})

    async def test_retention_is_per_attempt_and_preserves_failed_workspace(self):
        class Agent:
            @classmethod
            async def launch(cls, *, workspace, artifacts_dir):
                agent = cls()
                agent.workspace = workspace
                (artifacts_dir / "trace.json").write_text("{}")
                return agent

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                pass

            async def send(self, message):
                (self.workspace / "good.json").write_text("{}")
                (self.workspace / "invalid.json").write_text("{")
                (self.workspace / "wrong.json").write_text("[]")
                (self.workspace / "link.json").symlink_to("good.json")
                (self.workspace / "work.json").write_text("changed during turn")

            async def events(self):
                yield SimpleNamespace(status="completed")

        with (
            TemporaryDirectory() as directory,
            patch.dict(
                sys.modules,
                codex_box=SimpleNamespace(Agent=Agent, TurnCompleted=SimpleNamespace),
            ),
        ):
            root = Path(directory)
            backend = CodexBackend(attempts_dir=root)
            results = {}
            for keys in (["good"], ["good", "invalid", "wrong", "missing", "link"]):
                diagnostics = await backend.generate(
                    AgentStage(name="test", instructions="Inspect {task}", output=dict),
                    dict.fromkeys(keys, self.entry),
                    self.lineage,
                    results.__setitem__,
                )
            _, work = _prepare_work(
                "",
                {
                    "missing": {
                        **self.entry,
                        "submission_feedback": diagnostics["missing"],
                    }
                },
                self.lineage,
            )
            self.assertEqual(
                json.loads(work["missing"]["submission_feedback"]),
                {"output_file": "missing.json", "status": "missing"},
            )
            self.assertEqual(results, {"good": {}})
            attempts = list(root.iterdir())
            self.assertEqual(len(attempts), 2)
            for attempt in attempts:
                self.assertTrue((attempt / "trace.json").is_file())
                self.assertIn("Inspect inspect", (attempt / "prompt.txt").read_text())
                work = json.loads((attempt / "work.json").read_text())
                self.assertEqual((attempt / "workspace").exists(), len(work) > 1)
                if len(work) > 1:
                    collection = json.loads((attempt / "collection.json").read_text())
                    self.assertEqual(
                        {k: v["status"] for k, v in collection.items()},
                        {
                            "good": "submitted",
                            "invalid": "invalid_json",
                            "wrong": "wrong_type",
                            "missing": "missing",
                            "link": "not_regular_file",
                        },
                    )
                    self.assertEqual(
                        (attempt / "workspace/invalid.json").read_text(), "{"
                    )

    async def test_cancel_during_launch_closes_box_before_workspace_removal(self):
        started, release = asyncio.Event(), asyncio.Event()
        closed = []

        class Agent:
            @classmethod
            async def launch(cls, *, workspace):
                started.set()
                await release.wait()
                agent = cls()
                agent.workspace = workspace
                return agent

            async def close(self):
                closed.append(self.workspace.is_dir())

        with patch.dict(
            sys.modules,
            codex_box=SimpleNamespace(Agent=Agent, TurnCompleted=SimpleNamespace),
        ):
            task = asyncio.create_task(
                CodexBackend().generate(
                    AgentStage(name="generate", instructions="", output=dict),
                    {"a": self.entry},
                    self.lineage,
                    lambda key, value: None,
                )
            )
            await asyncio.wait_for(started.wait(), 2)
            task.cancel()
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, 2)
            self.assertEqual(closed, [True])
