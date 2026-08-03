"""Asserts every example's observable output.

Empty of examples until M2 lands Tier 1. The module exists now so `make
examples` is a real (passing) target rather than a pytest "no tests collected"
error, and so the first example has a home to land in.
"""

import pkgutil

import tapio_examples


def test_examples_package_is_importable():
    modules = [m.name for m in pkgutil.iter_modules(tapio_examples.__path__)]
    assert modules == [], f"unasserted examples: {modules}"
