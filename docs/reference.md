# Prototype API reference

Import the following names from `strange_loop.replay`.

| Call | Contract |
| --- | --- |
| `Loop(attempts=3)` | Create an in-memory runtime for one root invocation. Count the initial batch execution as attempt one. |
| `await loop.run(flow, *, max_revisions=3, **inputs)` | Bind named inputs as versions and return the version returned by the flow. |
| `await loop.map(flow, *, source, max_revisions=3)` | Run one invocation per string key in a dictionary and return keyed version handles. |
| `loop.trace(subject=None)` | Show the entire run in observed order, or return filtered events for a flow function, decorated stage, or invocation key. |
| `loop.input(version, name)` | Retrieve the exact version, literal value, or ordered dictionary of member versions supplied to a named input. An unknown name raises KeyError. |
| `loop.inputs(version)` | Retrieve all named inputs in a dictionary. A root input has an empty dictionary. |
| `@stage`, `@stage(revisable=True)` | Wrap a sync or async per-item function. A revisable function declares a keyword-only request parameter supplied by the runtime. |
| `@batch_stage`, `@batch_stage(revisable=True)` | Receive a Batch. Return a dictionary keyed by item.key, or await item.submit(value) and return None. |
| `batch[key]` | Look up a batch item. Iterating the batch yields items with key, inputs, request, and submit(value). |
| `await ctx.map(flow, *, source)` | Map over a stage output containing keyed values. Return an ordered collection of handles that can be passed to a stage. |
| `return ctx.revise(target, feedback=...)` | Request a new version from the target’s producer. A RevisionRequest carries the decoded previous value and feedback. |
| `return ctx.replace(target, with_=fixed)` | Adopt an existing version as the next value of the target artifact. |

A Version exposes `id`, `artifact`, `value`, `actor`, and named `inputs`. Its optional `previous`, `adopted_from`, and `request` fields describe a correction. Look up IDs in `loop.versions`. Inspect `loop.trace(key)` for one invocation, `loop.trace("batch")` for submission attempts, or `loop.events` for chronological events.
