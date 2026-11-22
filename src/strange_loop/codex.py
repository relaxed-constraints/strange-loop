"""codex-box adapter. Each turn receives a fresh workspace."""

import hashlib
import json
import re
from pathlib import Path
from tempfile import TemporaryDirectory


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
        work[key] = item
    return prompt, work


class CodexBackend:
    """Run each agent attempt in a fresh codex-box workspace.

    Args:
        **launch_options (Any): Arguments for Agent.launch, such as model or credentials.
            The adapter supplies the workspace.
    """

    def __init__(self, **launch_options):
        self.launch_options = launch_options

    async def generate(self, node, entries, lineage, submit):
        """Generate outputs for one batch.

        Args:
            node (AgentStage): Agent stage with instructions and an output type.
            entries (dict): Keyed requests with input IDs and optional repair feedback.
            lineage (dict): Upstream revision values and their named input IDs.
            submit (Callable): Callback that accepts an item key and its validated value.
        """
        prompt, work = _prepare_work(node.instructions, entries, lineage)
        from codex_box import Agent, TurnCompleted

        with TemporaryDirectory() as directory:
            workspace = Path(directory)
            (workspace / "work.json").write_text(
                json.dumps(list(work.values()), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            instructions = (
                f"{prompt}\n\nRead /workspace/work.json. It contains {len(work)} work entries. "
                "For each entry, use the values in inputs when present. "
                "If previous_output and feedback are present, correct the previous answer. "
                f"Write one complete JSON {_JSON_TYPES[node.output]} to the exact path in output_file "
                "relative to /workspace. Write a temporary file then atomically rename it."
            )
            try:
                async with await Agent.launch(
                    workspace=workspace, **self.launch_options
                ) as agent:
                    await agent.send(instructions)
                    async for event in agent.events():
                        if isinstance(event, TurnCompleted):
                            if event.status != "completed":
                                message = str(event.error)
                                if "model_not_found" in message:
                                    raise RuntimeError(message)
                                raise ConnectionError(message)
                            break
            finally:
                for key, item in work.items():
                    path = workspace / item["output_file"]
                    if path.is_file() and not path.is_symlink():
                        try:
                            value = json.loads(path.read_text())
                        except (ValueError, UnicodeError):
                            continue
                        if type(value) is node.output:
                            submit(key, value)
