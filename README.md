# Strange Loop

Build Python workflows with versioned stage outputs, targeted revisions, batch execution, and SQLite recovery.

Install with `python -m pip install relcon-strange-loop` on Python 3.12 or later. Import the library with `from strange_loop import Loop`.

To run the examples and tests from a checkout:

```sh
uv sync
uv run python -m examples.batch_repair
uv run python -m unittest discover -s tests -v
```

[Overview](docs/index.md) · [Installation](docs/getting-started/installation.md) · [Quickstart](docs/getting-started/quickstart.md)

## Releases

The package version lives in `pyproject.toml`. Update the version and lockfile together.
After CI passes on the release commit, publish a GitHub release with the matching
tag (for example, `v0.1.0`). The release workflow checks the tag and publishes
the tested wheel and source distribution to PyPI. The documentation workflow
deploys the site from that release to [GitHub Pages](https://relaxed-constraints.github.io/strange-loop/).

Before the first release, register a [pending PyPI publisher](https://pypi.org/manage/account/publishing/)
for project `relcon-strange-loop`, owner `relaxed-constraints`, repository `strange-loop`,
workflow `release.yml`, and environment `pypi`.
