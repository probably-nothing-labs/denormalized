denormalized-python
===

Python bindings for [denormalized](https://github.com/probably-nothing-labs/denormalized)

Denormalized is a single node stream processing engine written in Rust. This directory contains the bindings for building pipelines using python.

## Getting Started

1. Install denormalized `pip install denormalized`
2. Start the custom docker image that contains an instance of kafka along with with a script that emits some sample data to kafka `docker run --rm -p 9092:9092 emgeee/kafka_emit_measurements:latest`
3. Copy the [stream_aggregate.py](python/examples/stream_aggregate.py) example

This script will connect to the kafka instance running in docker and aggregate the metrics in realtime.

There are several other examples in the [examples/ folder](python/examples/) that demonstrate other capabilities including stream joins and UDAFs.


## Development

Make sure you're in the `py-denormalized/` directory.

We currently use [rye](https://rye.astral.sh/) to manage python dependencies.
`rye sync` to create/update the virtual environment

We use [maturin](https://www.maturin.rs/) for developing and building:
- `maturin develop` - build and install the python bindings into the current venv
- Run `ipython`, then import the library: `from denormalized import *`
- `maturin build` - compile the library

