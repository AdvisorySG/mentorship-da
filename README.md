# mentorship-da

## Development Setup

### Package Building

First install [Poetry](https://python-poetry.org/docs/#installation). Dependencies can now be installed in a virtual environment with the following command:

```bash
$ poetry install
```

### `nbdime` Setup

To view Jupyter notebooks with [nbdime](https://github.com/jupyter/nbdime), configure the diff/merge drivers for this repository with the following command:

```bash
$ nbdime config-git --enable
```

### `pre-commit` Setup

To enable the use of Git pre-commit hooks, install the [pre-commit](https://pre-commit.com/) package with the following command:

```bash
$ pre-commit install
```
