# Installation

Strange Loop requires Python 3.12 or later. Check the interpreter in your project environment.

```sh
python --version
```

Install Strange Loop from PyPI with that interpreter.

```sh
python -m pip install relcon-strange-loop
```

Import the library as `strange_loop`. To verify the installation, import `Loop` and print the distribution version.

```sh
python -c "from importlib.metadata import version; from strange_loop import Loop; print(version('relcon-strange-loop'))"
```

Continue to the [Quickstart](quickstart.md) to run your first flow.
