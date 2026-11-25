# API Reference

<style>
.md-typeset #api-reference,
.md-typeset #api-reference ~ h2,
.md-typeset #api-reference ~ h2 > a {
  font-weight: 400;
}
</style>

Import the public API from `strange_loop`. Select a name to jump to its signature and docstring.

## [Define flows and stages](define-flows-and-stages.md)

- Declare a flow or stage — [`@flow`](define-flows-and-stages.md#strange_loop.flows.flow), [`@stage`](define-flows-and-stages.md#strange_loop.flows.stage), [`@batch_stage`](define-flows-and-stages.md#strange_loop.flows.batch_stage).
- Call a stage in a flow — [`Context`](define-flows-and-stages.md#strange_loop.flows.Context), [`Context.call`](define-flows-and-stages.md#strange_loop.flows.Context.call), [`Stage`](define-flows-and-stages.md#strange_loop.flows.Stage), [`Stage.__call__`](define-flows-and-stages.md#strange_loop.flows.Stage.__call__).
- Process a batch — [`Batch`](define-flows-and-stages.md#strange_loop.batch.Batch), [`BatchItem`](define-flows-and-stages.md#strange_loop.batch.BatchItem), [`BatchItem.submit`](define-flows-and-stages.md#strange_loop.batch.BatchItem.submit), [`BatchContractError`](define-flows-and-stages.md#strange_loop.batch.BatchContractError).

## [Run, map, and resume](run-map-and-resume.md)

- Start a run — [`Loop`](run-map-and-resume.md#strange_loop.loop.Loop), [`Loop.run`](run-map-and-resume.md#strange_loop.loop.Loop.run).
- Map items at the top level or inside a flow — [`Loop.map`](run-map-and-resume.md#strange_loop.loop.Loop.map), [`Context.map`](run-map-and-resume.md#strange_loop.flows.Context.map).
- Reopen a saved run — [`Loop.runs`](run-map-and-resume.md#strange_loop.loop.Loop.runs), [`Loop.load`](run-map-and-resume.md#strange_loop.loop.Loop.load), [`Loop.resume`](run-map-and-resume.md#strange_loop.loop.Loop.resume).
- Invoke a stage with resolved inputs — [`Loop.invoke`](run-map-and-resume.md#strange_loop.loop.Loop.invoke).

## [Inspect results and lineage](inspect-results-and-lineage.md)

- Read a version — [`Version`](inspect-results-and-lineage.md#strange_loop.versions.Version), [`Version.value`](inspect-results-and-lineage.md#strange_loop.versions.Version.value), [`Version.attachments`](inspect-results-and-lineage.md#strange_loop.versions.Version.attachments).
- Inspect keyed outcomes — [`Collection`](inspect-results-and-lineage.md#strange_loop.versions.Collection), [`Collection.succeeded`](inspect-results-and-lineage.md#strange_loop.versions.Collection.succeeded), [`Collection.failed`](inspect-results-and-lineage.md#strange_loop.versions.Collection.failed), [`Collection.raise_for_failures`](inspect-results-and-lineage.md#strange_loop.versions.Collection.raise_for_failures), [`MapResult`](inspect-results-and-lineage.md#strange_loop.versions.MapResult), [`Members`](inspect-results-and-lineage.md#strange_loop.versions.Members).
- Follow inputs or read a trace — [`Loop.input`](inspect-results-and-lineage.md#strange_loop.loop.Loop.input), [`Loop.inputs`](inspect-results-and-lineage.md#strange_loop.loop.Loop.inputs), [`Loop.trace`](inspect-results-and-lineage.md#strange_loop.loop.Loop.trace), [`Loop.events`](inspect-results-and-lineage.md#strange_loop.loop.Loop.events).
- Record a version or trace event — [`Loop.new_artifact`](inspect-results-and-lineage.md#strange_loop.loop.Loop.new_artifact), [`Loop.save`](inspect-results-and-lineage.md#strange_loop.loop.Loop.save), [`Loop.record`](inspect-results-and-lineage.md#strange_loop.loop.Loop.record).

## [Handle failures and corrections](handle-failures-and-corrections.md)

- Mark a failure — [`Context.fail`](handle-failures-and-corrections.md#strange_loop.flows.Context.fail), [`Failure`](handle-failures-and-corrections.md#strange_loop.versions.Failure).
- Ask the producer to revise — [`Context.revise`](handle-failures-and-corrections.md#strange_loop.flows.Context.revise), [`Revise`](handle-failures-and-corrections.md#strange_loop.versions.Revise), [`RevisionRequest`](handle-failures-and-corrections.md#strange_loop.versions.RevisionRequest).
- Adopt a correction or inspect a conflict — [`Context.replace`](handle-failures-and-corrections.md#strange_loop.flows.Context.replace), [`Replacement`](handle-failures-and-corrections.md#strange_loop.versions.Replacement), [`StaleVersion`](handle-failures-and-corrections.md#strange_loop.versions.StaleVersion).

## [Store custom values](store-custom-values.md)

- Use serializer functions or file-based methods — [`CustomCodec`](store-custom-values.md#strange_loop.codecs.CustomCodec), [`FileCodec`](store-custom-values.md#strange_loop.codecs.FileCodec), [`FileCodec.save`](store-custom-values.md#strange_loop.codecs.FileCodec.save), [`FileCodec.load`](store-custom-values.md#strange_loop.codecs.FileCodec.load).
