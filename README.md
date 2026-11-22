# Strange Loop

A serial Python workflow runner for versioned JSON artifacts, keyed agent batches and repairs across nested workflows. The working name is provisional.

From this directory, install the sibling codex-box adapter dependency and run the examples with Python 3.12 or later:

```sh
uv sync --extra codex
uv run python -m unittest discover -s tests -v
uv run python examples/live.py
```

Set `CODEX_MODEL` to select a model supported by your credentials. The live example needs a configured codex-box Docker image and credentials. The acceptance tests run offline with the same runner and a deterministic backend.

## Milestone API

Define async orchestration with `@flow` and ordinary value transformations with `@stage`. Supply JSON values or string-keyed `keyed(...)` inputs to `Runner.run`. Within a workflow, pass artifact handles as keyword arguments. Use `.map` to align keyed inputs and broadcast scalar handles. An agent stage accepts an instruction string, a built-in JSON output type, and an optional `Batch`. A nested flow can return a producer's handle directly. A grader receives decoded values and returns `Pass()` or `Repair(ctx.ref(name), feedback)` for a declared repairable input produced by an agent.

Use `Runner(store="run.sqlite", backend=...)` to retain revision history. `runner.store.read(ref)`, `.inputs(ref)` and `.consumers(ref)` inspect values and direct edges. Each run starts a new invocation tree. Within a run, flow bodies replay after repair and should keep side effects inside stages. Use stable call order and unique stage names within each flow. Branch-dependent replay, collection joins, host-crash resume, parallel scheduling, and hill climbing are later milestones.

A backend implements async `generate(node, entries, lineage, submit)`. Entries contain current named input IDs plus previous output and feedback on repair. The lineage mapping supplies revision values and named parent IDs. Call `submit(key, value)` as each item becomes available. The runner persists each validated submission independently and retries missing keys after an interruption. The Docker adapter imports validated per-key files on turn completion or failure.

[Workflow design and execution contract](../research/agent-workflows/README.md) · [Nested live example](examples/live.py) · [Four acceptance cases](tests/test_contract.py)

The Codex adapter resolves references on the host. The agent receives a rendered prompt and one `work.json` array. Each entry names its output file and supplies any inputs omitted from the prompt. Repairs also include the previous output value and grader feedback. Use `{argument_name}` for shared prompt inputs and `{{argument_name}}` for literal braces around a name. Structured inputs render as JSON. The runner retains the lineage graph.
