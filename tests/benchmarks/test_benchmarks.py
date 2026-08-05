"""Benchmarks: msg/s with validation on and off, spawn cost, ask latency, RSS.

To be filled in for the 0.1.0 release. The fixture check here keeps `make
bench` working in the meantime. Skipped by default through `--benchmark-skip`
in pyproject.
"""


def test_benchmark_fixture_available(benchmark):
    assert benchmark(lambda: 1 + 1) == 2
