# Working in this repository

tapio is a Pekko-inspired actor toolkit for Python: local, typed,
asyncio-native actors with supervision, and Pydantic models throughout.

## Commands

`make` with no target lists everything. The Makefile is the single source of
truth for what CI runs, so use it rather than re-spelling commands.

```bash
make install    # create the venv, install deps
make check      # pre-push gate: lint + types + tests
make test       # pytest
make examples   # run and assert every example
make ci         # exactly what GitHub Actions runs
```

Never invoke `python` or `pytest` directly: everything goes through `uv run`,
so there is no ambiguity about which interpreter ran.

CI calls `make ci` and nothing else, then publishes the built site to GitHub
Pages from `main`. That publish step waits on the repository variable
`PUBLISH_DOCS` being `"true"`, since Pages has to be enabled on the repository
first.

## Layout

| Path | What lives there |
|---|---|
| `src/tapio/` | the library, shipped in the wheel |
| `src/tapio/actor/` | paths, refs, behaviors, cell, mailbox, system |
| `src/tapio/dispatch/` | the event loop a system's tasks run on |
| `src/tapio/testkit/` | helpers for testing actor code, shipped |
| `tests/` | unit tests, plus `tests/examples/` and `tests/benchmarks/` |
| `examples/tapio_examples/` | runnable examples, dev-only, never shipped |
| `docs/` | mkdocs-material, code included from `examples/` |

## Style

These are settled preferences. Follow them in new code and fix them in code
you are already editing.

- **No em dashes.** Not in prose, docstrings, comments, commit messages, or PR
  bodies. Use a comma, a colon, parentheses, or a full stop.
- **No references to internal planning documents.** No section numbers, no
  milestone labels, no "see the plan". Write the reasoning out in place. A
  reader of this repository cannot follow those pointers.
- **Absolute imports only**, enforced by ruff. `from tapio.actor.path import
  ActorPath`, never a relative import, including in tests.
- **Single backticks in docstrings.** Markdown, not RST: no double backticks
  and no roles. Cross-reference with mkdocstrings syntax,
  `[Name][tapio.module.Name]`.
- **No blanket `from __future__ import annotations`.** The floor is Python
  3.11. Quote the few genuinely forward references instead.
- Google-style docstrings on every public module, class, and function. Ruff
  enforces the convention; mypy runs `--strict` over `src` and `examples`.
- Explain *why* in comments, not *what*. The code says what it does.

## Testing

- `pytest-asyncio` runs in auto mode, so an `async def test_...` needs no
  marker. Warnings are errors.
- Anything that starts a system wraps itself in
  `tapio.testkit.assert_no_leaked_tasks()`. The runtime deliberately does not
  use `TaskGroup`, so "no orphaned tasks" is an invariant this suite has to
  keep honest.
- Tests use the `system` fixture from `tests/conftest.py` when they need a live
  tree; it terminates whatever the test leaves behind.
- Examples are tests. Every module in `examples/tapio_examples/` has an
  assertion in `tests/examples/test_suite.py`, and a new example with no
  assertion fails the suite on purpose.

## Design invariants worth knowing before changing the runtime

- **A message's identity never depends on a setting.** Delivery-time validation
  discards its result; the recipient always gets the object the sender passed.
- **`tell` never blocks and never raises about the recipient.** Errors about
  the message belong to the sender. Errors about the recipient become dead
  letters.
- **The mailbox has one consumer and one waiter.** Signals drain before user
  messages, and the system lane is always unbounded so a stop cannot be
  refused.
- **Shutdown races one deadline for the whole tree**, not one per actor, so
  worst-case shutdown does not multiply by depth.
- **Every task belongs to a cell and is cancelled in its termination
  sequence.** If you add a task, say which cell owns it.
