"""codex-box adapter. Each turn receives a fresh workspace."""

import asyncio
import hashlib
import json
import re
import shutil
import stat
from contextlib import nullcontext
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4


# Only standalone named fields are templates. JSON objects and SQL expressions
# can contain other braces; preserve those verbatim.
_FIELD = re.compile(r"\{\{([a-zA-Z_]\w*)\}\}|\{([a-zA-Z_]\w*)\}")
_JSON_TYPES = {
    str: "string",
    list: "array",
    dict: "object",
    int: "integer",
    float: "floating-point number",
    bool: "boolean",
}


def _output_file(key):
    if key == "":
        return "result.json"
    # This canonical subset also avoids case-fold collisions on mounted volumes.
    if re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,119}", key) and key not in {
        "work",
        "result",
    }:
        return f"{key}.json"
    digest = hashlib.sha256(key.encode()).hexdigest()
    return f"_item-{digest}.json"


def _prepare_work(template, entries, lineage):
    rendered = {}
    for match in _FIELD.finditer(template):
        name = match[2]
        if name is None or name in rendered:
            continue
        values = []
        for entry in entries.values():
            if name not in entry["inputs"]:
                raise ValueError(f"Unknown prompt input {{{name}}}")
            value = lineage[entry["inputs"][name]]["value"]
            values.append(json.dumps(value, ensure_ascii=False))
        if len(set(values)) != 1:
            raise ValueError(
                f"Prompt input {{{name}}} varies across this batch; keep it in work.json or use Batch(1)"
            )
        rendered[name] = value if isinstance(value, str) else values[0]

    prompt = _FIELD.sub(
        lambda m: "{" + m[1] + "}" if m[1] else rendered[m[2]], template
    )
    work = {}
    for key, entry in entries.items():
        item = {"output_file": _output_file(key)}
        inputs = {
            name: lineage[ref]["value"]
            for name, ref in entry["inputs"].items()
            if name not in rendered
        }
        if inputs:
            item["inputs"] = inputs
        if "previous" in entry:
            item.update(
                previous_output=lineage[entry["previous"]]["value"],
                feedback=entry["feedback"],
            )
        if "submission_feedback" in entry:
            item["submission_feedback"] = entry["submission_feedback"]
        work[key] = item
    return prompt, work


def _collect(path, expected, key, submit):
    try:
        mode = path.lstat().st_mode
        if not stat.S_ISREG(mode):
            return {"status": "not_regular_file"}
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"status": "missing"}
    except (ValueError, UnicodeError) as error:
        return {"status": "invalid_json", "error": str(error)}
    if type(value) is not expected:
        return {
            "status": "wrong_type",
            "expected": expected.__name__,
            "actual": type(value).__name__,
        }
    submit(key, value)
    return {"status": "submitted"}


class CodexBackend:
    """Run each agent attempt in a fresh codex-box workspace.

    Args:
        attempts_dir (Path | None): Save each attempt in a new subdirectory.
        **launch_options (Any): Arguments for Agent.launch, such as model or credentials.
            The adapter supplies the workspace.
    """

    def __init__(self, *, attempts_dir=None, **launch_options):
        self.attempts_dir = Path(attempts_dir) if attempts_dir is not None else None
        self.launch_options = launch_options

    async def generate(self, node, entries, lineage, submit, *, attempt_dir=None):
        """Generate outputs for one batch.

        Args:
            attempt_dir (Path | None): Exact new directory for this attempt. Overrides attempts_dir.
            node (AgentStage): Agent stage with instructions and an output type.
            entries (dict): Keyed requests with input IDs and optional repair feedback.
            lineage (dict): Upstream revision values and their named input IDs.
            submit (Callable): Callback that accepts an item key and its validated value.
        """
        prompt, work = _prepare_work(node.instructions, entries, lineage)
        from codex_box import Agent, TurnCompleted

        if attempt_dir is None and self.attempts_dir is not None:
            attempt_dir = self.attempts_dir / uuid4().hex
        options = self.launch_options.copy()
        if attempt_dir is not None:
            attempt_dir = Path(attempt_dir)
            attempt_dir.mkdir(parents=True, exist_ok=False)
            workspace_path = attempt_dir / "workspace"
            workspace_path.mkdir()
            options["artifacts_dir"] = attempt_dir
            directory_context = nullcontext(workspace_path)
        else:
            directory_context = TemporaryDirectory()
        with directory_context as directory:
            workspace = Path(directory)
            work_json = json.dumps(list(work.values()), ensure_ascii=False, indent=2)
            (workspace / "work.json").write_text(work_json, encoding="utf-8")
            instructions = (
                f"{prompt}\n\nRead /workspace/work.json. It contains {len(work)} work entries. "
                "For each entry, use the values in inputs when present. "
                "If previous_output and feedback are present, correct the previous answer. "
                f"Write one complete JSON {_JSON_TYPES[node.output]} to the exact path in output_file "
                "relative to /workspace. Copy each output_file value verbatim, including leading zeros. "
                "If submission_feedback is present, correct that file delivery problem. "
                "Write a temporary file then atomically rename it. Before finishing, check that "
                "every listed output_file exists and contains the required JSON type."
            )
            if attempt_dir is not None:
                (attempt_dir / "prompt.txt").write_text(instructions, encoding="utf-8")
                (attempt_dir / "work.json").write_text(work_json, encoding="utf-8")
            completed = False
            try:
                # Cancellation during Docker startup can precede receipt of the container handle.
                launch = asyncio.create_task(Agent.launch(workspace=workspace, **options))
                try:
                    agent = await asyncio.shield(launch)
                except asyncio.CancelledError:
                    agent = await launch
                    await agent.close()
                    raise
                async with agent:
                    await agent.send(instructions)
                    async for event in agent.events():
                        if isinstance(event, TurnCompleted):
                            if event.status != "completed":
                                message = str(event.error)
                                if "model_not_found" in message:
                                    raise RuntimeError(message)
                                raise ConnectionError(message)
                            completed = True
                            break
            finally:
                collection = {}
                for key, item in work.items():
                    filename = item["output_file"]
                    result = _collect(workspace / filename, node.output, key, submit)
                    collection[key] = {"output_file": filename, **result}
                if attempt_dir is not None:
                    (attempt_dir / "collection.json").write_text(
                        json.dumps(collection, ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )
                    if completed and all(
                        row["status"] == "submitted" for row in collection.values()
                    ):
                        shutil.rmtree(workspace)
            return {
                key: json.dumps(row, ensure_ascii=False)
                for key, row in collection.items()
                if row["status"] != "submitted"
            }
