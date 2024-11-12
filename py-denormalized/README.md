denormalized-python
===

Python bindings for [denormalized](https://github.com/probably-nothing-labs/denormalized)

Denormalized is a single node stream processing engine written in Rust.

## Getting Started

The easiest way to get started is to look at some of the examples in the [python/examples/](python/examples/) folder.

- We're currently using [rye](https://rye.astral.sh/guide/) to manage dependencies. Run `rye sync` in the current directory create and install the venv
- If you have rust/cargo installed, you can run `maturin develop` to build the core rust code and install the bindings in the current virtual environment.
- Alternatively, 


## Development

Make sure you're in the `py-denormalized/` directory.

We currently use [rye](https://rye.astral.sh/) to manage python dependencies.
`rye sync` to create/update the virtual environment

We use [maturin](https://www.maturin.rs/) for developing and building:
- `maturin develop` - build and install the python bindings into the current venv
- Run `ipython`, then import the library: `from denormalized import *`
- `maturin build` - compile the library

