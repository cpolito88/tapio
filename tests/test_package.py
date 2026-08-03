"""M0 exit criterion: the package imports and the toolchain is wired up."""

import asyncio
import sys

import tapio


def test_package_imports():
    assert tapio.__version__


def test_examples_package_importable():
    import tapio_examples

    assert tapio_examples.__all__ == []


def test_minimum_python_is_311():
    # 3.11 is a hard floor: asyncio.timeout(), typing.Self, add_note (PLAN §2).
    assert sys.version_info >= (3, 11)
    assert hasattr(asyncio, "timeout")


async def test_asyncio_mode_auto_is_configured():
    # Guards the pytest-asyncio wiring the whole runtime suite will rely on.
    await asyncio.sleep(0)
