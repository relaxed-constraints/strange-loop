# Inspect revisions

Lineage records which revisions a stage used to produce an output. Each reference identifies one revision in the runner's store.

Use the result from the translation flow to inspect the welcome message.

```python
welcome = results["welcome"]
print(runner.store.read(welcome))
parents = runner.store.inputs(welcome)
print(runner.store.read(parents["text"]))
print(runner.store.consumers(welcome))
```

- `read` returns the stored value.
- `inputs` returns the direct input references by argument name.
- `consumers` returns the revisions that used this revision. These can include grader verdicts.

The host retains input references for every revision. You can inspect earlier inputs even after a shared input changes.

The agent reads one file, `work.json`. Each entry gives the output filename and any input values that the prompt omits. A repair entry also contains the previous answer and the grader's feedback.

The default store keeps revisions in memory. Set `store="runs.sqlite"` on the runner to retain revisions on disk. Each call to `run` starts a new run.
