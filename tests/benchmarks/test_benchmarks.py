"""Benchmarks — msg/s with validation on and off, spawn cost, ask latency, RSS.

Filled in at M7; the fixture check here keeps `make bench` wired up meanwhile.
Skipped by default via `--benchmark-skip` in pyproject.
"""


def test_benchmark_fixture_available(benchmark):
    assert benchmark(lambda: 1 + 1) == 2
