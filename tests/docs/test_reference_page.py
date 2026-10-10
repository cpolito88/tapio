"""The examples in docstrings, which the reference page renders, call tapio right.

A docstring example is not executed, because it leans on names the reader
supplies, such as a `worker()` behavior or a `ctx`. What can be checked without
running it is every call it makes to tapio: the arguments have to bind to the
real signature.
"""

import ast
import inspect
import re
import textwrap
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

import tapio
import tapio.actor
import tapio.cluster
import tapio.testkit
from tapio.actor.context import ActorContext

SOURCE = Path(tapio.__file__).parent

_BLOCK = re.compile(r"^\s*```python\n(.*?)^\s*```", re.MULTILINE | re.DOTALL)

PUBLIC: dict[str, Any] = {
    name: getattr(module, name)
    for module in (tapio, tapio.actor, tapio.cluster, tapio.testkit)
    for name in module.__all__
}

# A ref or a context in an example is a name the reader supplies, so these
# methods are matched by name whatever they are called on.
METHODS: dict[str, Callable[..., Any]] = {
    "spawn": ActorContext.spawn,
    "spawn_anonymous": ActorContext.spawn_anonymous,
}


def examples() -> Iterator[tuple[str, str]]:
    """Yield each `python` block in a docstring, with where it was found."""
    for path in sorted(SOURCE.rglob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(
                node,
                ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef,
            ):
                continue
            doc = ast.get_docstring(node)
            if doc is None:
                continue
            where = f"{path.relative_to(SOURCE.parent)}:{getattr(node, 'lineno', 1)}"
            for block in _BLOCK.findall(doc):
                yield where, textwrap.dedent(block)


def callee(call: ast.Call) -> Callable[..., Any] | None:
    """The tapio callable a call names, or `None` for one this cannot tell."""
    match call.func:
        case ast.Name(id=name):
            return PUBLIC.get(name)
        case ast.Attribute(value=ast.Name(id=owner), attr=attr) if owner in PUBLIC:
            found = getattr(PUBLIC[owner], attr, None)
            return found if callable(found) else None
        case ast.Attribute(attr=attr):
            method = METHODS.get(attr)
            # Bound, so the receiver is not one of the arguments to bind.
            return None if method is None else method.__get__(object())
    return None


def misbound(code: str) -> list[str]:
    """Describe each call in `code` whose arguments do not fit its signature."""
    problems = []
    for node in ast.walk(ast.parse(code)):
        if not isinstance(node, ast.Call):
            continue
        if any(isinstance(a, ast.Starred) for a in node.args) or any(
            k.arg is None for k in node.keywords
        ):
            continue
        target = callee(node)
        if target is None:
            continue
        try:
            signature = inspect.signature(target)
        except (TypeError, ValueError):
            continue
        try:
            signature.bind(
                *node.args, **{k.arg: k.value for k in node.keywords if k.arg}
            )
        except TypeError as error:
            problems.append(f"{ast.unparse(node)}: {error}")
    return problems


EXAMPLES = list(examples())


def test_there_are_examples_to_check():
    assert len(EXAMPLES) > 10


@pytest.mark.parametrize(("where", "code"), EXAMPLES, ids=[w for w, _ in EXAMPLES])
def test_an_example_calls_tapio_with_arguments_that_bind(where: str, code: str):
    assert misbound(code) == []
