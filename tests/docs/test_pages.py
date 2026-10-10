"""Every Python block on the docs site is included from a file CI checks."""

import re
from pathlib import Path

import pytest

DOCS = Path(__file__).parents[2] / "docs"

_BLOCK = re.compile(r"^```python\n(.*?)^```", re.MULTILINE | re.DOTALL)
_INCLUDE = re.compile(r'--8<-- "(examples|tests/docs)/[^"]+"')


def _pages() -> list[Path]:
    return sorted(DOCS.glob("*.md"))


@pytest.mark.parametrize("page", _pages(), ids=lambda page: page.name)
def test_every_python_block_is_one_include(page: Path):
    # A block written into the page is a block nothing runs or type-checks,
    # and two of those were wrong before this check existed.
    inline = [
        block.strip()
        for block in _BLOCK.findall(page.read_text())
        if not _INCLUDE.fullmatch(block.strip())
    ]

    assert inline == []
