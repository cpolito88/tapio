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
| `src/tapio/remote/` | addressing, the wire format, the link and the associations over it |
| `src/tapio/cluster/` | membership by gossip and its merge, the reachability a ring of monitors builds, and the strategies that down a partition's losing side |
| `src/tapio/dispatch/` | the event loop a system's tasks run on |
| `src/tapio/testkit/` | helpers for testing actor code, shipped |
| `tests/` | unit tests, mirroring the source packages, plus `tests/examples/` and `tests/benchmarks/` |
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
- **Plain English, everywhere.** Docstrings, comments, `docs/`, the README,
  commit messages and pull request bodies. Subject, verb, object. One idea per
  sentence. No inverted clauses, no aphorisms, no sentence written for its
  cadence, nothing a reader has to decode before they can use it. Say the fact
  and stop. Keep the reasons: *why* a decision was made is worth a sentence,
  it just has to be a plain one. In tests this is strictest, since a test is
  usually read while it is failing: a one-line module docstring, and a comment
  only where the code is genuinely surprising.
- Google-style docstrings on every public module, class, and function. Ruff
  enforces the convention; mypy runs `--strict` over `src` and `examples`.
- Explain *why* in comments, not *what*. The code says what it does.

## Commit messages and pull requests

- **The commit type decides the version.** python-semantic-release reads the
  history on `main`, so `feat:` moves the minor, `fix:` moves the patch, and
  anything else moves nothing. Write the type you mean: a `fix:` that is
  really a feature ships under a version number that lies about it. A
  `BREAKING CHANGE:` footer moves the minor too while this is 0.x, since
  going to 1.0.0 is a decision rather than a side effect. Nobody edits a
  version by hand, and `make next-version` says what the current history
  would produce.
- **Nothing in the repository holds a version number.** It is derived from the
  git tag at build time, so a release is a tag and nothing else. Do not add a
  literal version to `pyproject.toml` or `version.py`: the second copy is the
  one that goes stale, and writing one would put the release job back to
  pushing commits at a protected branch.
- **Wrap commit messages at 72 columns.** `git log` does not reflow, so an
  unwrapped paragraph becomes one very long line in a terminal.
- **Do not wrap pull request bodies.** One line per paragraph and one per list
  item, however long. GitHub renders Markdown, so a hard-wrapped body and an
  unwrapped one look identical in the browser, and the wrapping only shows in
  the raw editor. What it costs is in the permanent history: GitHub generates a
  squash merge's message from the pull request body and wraps it at 72, so a
  body already wrapped at 79 gets wrapped a second time and lands ragged, with
  stray two-word lines. Wrapping once, at merge time, is the point. PR #3's
  merge commit has the artifact, with "and a / shutdown / that drains the tree"
  split across three lines.
- **Let the merge generate its own message.** `gh pr merge --squash` with no
  `--body-file` uses the pull request body and applies the wrapping above.
  Passing a body explicitly sends it verbatim and skips the wrapping entirely,
  so an unwrapped body becomes an unwrapped commit: one paragraph, one
  several-hundred-character line, unreadable in `git log`. PR #4's merge commit
  has *that* artifact, which is how this rule got its second half. If a merge
  message really has to be supplied by hand, wrap it at 72 first.
- **End the pull request body with the `Co-Authored-By` trailer**, not just the
  branch commits. The squash message is built from the body and the branch
  commits' trailers are discarded with their messages, so a trailer on every
  commit of the branch still does not reach `main`. PR #4's merge commit has
  none, while all four of its branch commits did.
- Say what changed and why, and where a decision contradicts the design notes,
  say so explicitly. That is a decision, not an oversight.
- Name the tests that would fail if the change regressed. "CI is green" is not
  verification, it is a precondition.

## Testing

- **The test tree mirrors the source tree.** A test for `tapio.remote.codec`
  lives in `tests/remote/test_codec.py`. Shared fixtures and fakes stay at
  `tests/`, since they are shared by every package.
- `pytest-asyncio` runs in auto mode, so an `async def test_...` needs no
  marker. Warnings are errors.
- **Stop an actor through its behavior, not with `cell.abort()`.** Abort
  cancels the cell's task, and the `CancelledError` that follows lands in the
  test rather than in the runtime. Send a message the behavior answers with
  `Behaviors.stopped()` and wait for the effect with `eventually`.
- Anything that starts a system wraps itself in
  `tapio.testkit.assert_no_leaked_tasks()`. The runtime deliberately does not
  use `TaskGroup`, so "no orphaned tasks" is an invariant this suite has to
  keep honest.
- Tests use the `system` fixture from `tests/conftest.py` when they need a live
  tree; it terminates whatever the test leaves behind. A fixture only one
  package needs lives in that package's own `conftest.py`, with its helpers
  beside it: `tests/remote/conftest.py` holds the two systems every remoting
  test starts, and `tests/remote/peers.py` the messages, behaviors and
  misbehaving peer they share.
- **A test that listens binds port 0.** The OS picks, the canonical address
  follows what it picked, and no two tests argue over a number somebody chose.
- Examples are tests. Every module in `examples/tapio_examples/` has an
  assertion in `tests/examples/test_suite.py`, and a new example with no
  assertion fails the suite on purpose.

## Design invariants worth knowing before changing the runtime

- **A message's identity never depends on a setting.** Delivery-time validation
  discards its result; the recipient always gets the object the sender passed.
  The guarantee is local: a message that crossed a link was rebuilt from JSON,
  so it is `==` to what was sent and never `is` it. Say which side of that line
  a test is on.
- **`tell` never blocks and never raises about the recipient.** Errors about
  the message belong to the sender. Errors about the recipient become dead
  letters.
- **The mailbox has one consumer and one waiter.** Signals drain before user
  messages, and the system lane is always unbounded so a stop cannot be
  refused.
- **Shutdown races one deadline for the whole tree**, not one per actor, so
  worst-case shutdown does not multiply by depth.
- **Every task belongs to a cell and is cancelled in its termination
  sequence.** If you add a task, say which cell owns it. Remoting adds no
  exception: an association is an actor whose reader is its own task, and the
  endpoint actor owns the listener and any connection still mid-handshake.
- **The handshake pins the protocol version, not the package version.**
  `PROTOCOL_VERSION` in `remote/protocol.py` describes the wire contract and
  is what both ends must agree on. `__version__` travels in the hello as a
  diagnostic and is never compared, so two nodes on different releases
  interoperate and a rolling deploy is possible. Raising the protocol version
  is a deployment event and belongs in the pull request body.
- **A type key on a frame is a registry key, never an import path.** Resolving
  a dotted name that arrived on a socket into an importable object is remote
  code execution. An unregistered key is a dead letter naming the key, and
  nothing is imported to find out what it might have meant.
- **Remoting is off unless it is configured**, and when it is, the port is
  bound while the system is being constructed. That is what settles the
  canonical address before any ref can write itself down, and what makes a
  configuration that would listen to the world fail to start rather than fail
  to be secure.
- **Delivery across a link is at-most-once, FIFO per association**, the same
  guarantee as a local send. No acks and no retries: upgrading that belongs to
  the user's protocol, where they know what is safe to repeat.
