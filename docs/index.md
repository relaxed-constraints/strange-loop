# What is Strange Loop?

Strange Loop is a Python library for workflows where an intermediate result may need a second attempt. It keeps a history of the results and records which inputs produced each one.

Suppose you ask a language model to write ten database queries. Nine pass your checks. One refers to a field that does not exist. With Strange Loop, you can send that query back with feedback, check the revised query, and reuse the nine that passed. Later, you can see both drafts and the feedback that prompted the change.

## How it works

- A **stage** is a Python function that does one piece of work, such as writing a query or checking it. Add the `@stage` decorator to have Strange Loop track its calls.
- A **version** is a record of a value. When a stage returns a value, Strange Loop records who produced it and which inputs the stage used.
- A **flow** is a Python function that calls stages and decides what happens next. Use ordinary Python conditions to accept a result or ask for a revision.
- A **loop** runs the flow and keeps its records. You can follow a result back to its inputs, or save the run in SQLite and resume it after an interruption.

Start with [Installation](getting-started/installation.md). If you already have Strange Loop installed, [run your first flow](getting-started/quickstart.md).
