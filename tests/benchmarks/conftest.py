"""Print what the benchmarks are running on, when they are actually running."""

import pytest
from tests.benchmarks.machine import as_text


def pytest_report_header(config: pytest.Config) -> list[str] | None:
    """Put the machine description at the top of a benchmark run.

    Only when the benchmarks are going to run. The normal test run skips them,
    and a header describing a CPU would be noise in front of a suite that is
    about to measure nothing.

    Args:
        config: The session's configuration, which knows whether benchmarks
            were skipped.

    Returns:
        The lines to print, or `None` when nothing is being measured.
    """
    # `--benchmark-skip` lives in addopts and stays set even when
    # `--benchmark-only` overrides it on the command line, so asking about the
    # skip flag alone would never print this.
    skipped = getattr(config.option, "benchmark_skip", False)
    only = getattr(config.option, "benchmark_only", False)
    if skipped and not only:
        return None
    return ["measured on:", *[f"  {line}" for line in as_text().splitlines()]]
