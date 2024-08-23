denormalized-python
===

Python bindings for [denormalized](https://github.com/probably-nothing-labs/denormalized)

## Development

Make sure you're in the `py-denormalized/` directory.

We currently use [rye](https://rye.astral.sh/) to manage python dependencies.
`rye sync` to create/update the virtual environment

We use [maturin](https://www.maturin.rs/) for developing and building:
- `maturin develop` - build and install the python bindings into the current venv
- Run `ipython`, then import the library: `from denormalized import *`
- `maturin build` - compile the library

