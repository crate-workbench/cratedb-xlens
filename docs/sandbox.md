# Development sandbox

The project uses [uv](https://docs.astral.sh/uv/) for dependency management.

## Install
```shell
git clone https://github.com/crate-workbench/cratedb-xlens.git
cd cratedb-xlens
uv venv --python 3.13 --seed .venv
uv pip install --upgrade --editable='.[dev,docs]'
```

## Software tests
```shell
uv run pytest
```

## Documentation
```shell
uv run poe docs-autobuild
```
