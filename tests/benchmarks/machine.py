"""What the numbers were measured on, printed by whatever measured them.

A benchmark result without the machine under it is not a result. The README
quotes these lines, and they come from the run rather than from somebody
typing them once and forgetting, which is how a provenance line ends up
describing a laptop that was replaced two years ago.

Deliberately not the hostname. pytest-benchmark records it, the README is
public, and nobody reading a throughput figure needs to know what you call
your computer.
"""

import os
import platform
from datetime import UTC, datetime

__all__ = ["as_text", "describe"]


def _cpu() -> str:
    """Name the processor as specifically as the platform will say.

    Returns:
        The CPU's brand string, or the vaguer thing `platform` offers when
        py-cpuinfo is not installed.
    """
    try:
        # Imported here rather than at the top: it is optional, and reading
        # CPU details is the only thing that wants it.
        import cpuinfo
    except ImportError:  # pragma: no cover - py-cpuinfo ships with the runner
        return platform.processor() or platform.machine()
    info = cpuinfo.get_cpu_info()
    brand = str(info.get("brand_raw") or platform.machine())
    count = info.get("count")
    return f"{brand}, {count} cores" if count else brand


def _memory() -> str:
    """Read total physical memory, where the platform exposes it.

    Returns:
        The size in gigabytes, or an empty string on a platform that does not
        answer. It is context for the resident-actor table rather than an
        input to any measurement, so not knowing is survivable.
    """
    try:
        total = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (ValueError, OSError, AttributeError):  # pragma: no cover - platform
        return ""
    return f"{total / 1e9:.0f} GB RAM"


def _pydantic() -> str:
    """Report the Pydantic version, since validation is what is being priced."""
    # Read here so the version reported is the one that ran, not one
    # captured when this module was imported.
    import pydantic

    return f"pydantic {pydantic.VERSION}"


def describe() -> list[str]:
    """Describe the machine and the versions that produced a measurement.

    Returns:
        One fact per line, in the order the README prints them.
    """
    parts = [_cpu(), _memory()]
    return [
        ", ".join(part for part in parts if part),
        f"{platform.system()} {platform.release()}",
        f"{platform.python_implementation()} {platform.python_version()}, "
        f"{_pydantic()}",
        datetime.now(UTC).strftime("measured %Y-%m-%d"),
    ]


def as_text(prefix: str = "") -> str:
    """Render the description as lines, for printing above a table.

    Args:
        prefix: Put in front of every line, for a report header that indents.

    Returns:
        The block, without a trailing newline.
    """
    return "\n".join(f"{prefix}{line}" for line in describe())
