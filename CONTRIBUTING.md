# Contributing to tapio

Contributions are welcome, from people and from coding agents alike. Both go
through the same gate and the same review.

This file covers the process: how to set up, what has to pass, and what
happens to a pull request. The conventions for the code itself live in
[AGENTS.md](AGENTS.md). That file is written for agents, but it is the one set
of rules for everybody, and review checks against it. It is kept in one place
so that it cannot drift from a second copy here.

## Before you start

- Read the scope in the [README](README.md#what-it-is-not). Sharding and
  distributed data are out of scope permanently, so a pull request that adds
  either will be closed.
- For anything larger than a bug fix, open an issue first. A design is easier
  to change before the code exists.

## Setting up

You need [uv](https://docs.astral.sh/uv/) and Python 3.11 or later. The
`Makefile` is the entry point, and `make` with no target lists every target.

```bash
make install    # create the venv, install deps
make hooks      # optional: install the pre-commit hooks
```

Run everything through `make` or `uv run`, never with a bare `python` or
`pytest`. That way there is no doubt about which interpreter ran.

## The gate

```bash
make check      # lint, mypy --strict, tests: run it before every push
make ci         # exactly what GitHub Actions runs, examples and docs included
```

CI runs `make ci` on Python 3.11, 3.12, 3.13, 3.14 and 3.15.

`make ci` ends with `make audit`, which checks the versions in `uv.lock`
against the OSV advisory database. It needs network access, which is why
`make check` leaves it out.

Two things fail the suite on purpose:

- a new example in `examples/tapio_examples/` with no assertion in
  `tests/examples/test_suite.py`;
- any warning, since warnings are errors.

## When the audit fails

`make audit` fails when a version pinned in `uv.lock` has a published
advisory. A new advisory can turn every open pull request red at once, with
nothing wrong in any of them. The scheduled run of the same scan opens an
issue titled "Scheduled dependency audit failed" when that happens.

The report has two parts, and the part a finding is in decides what to do:

- **Runtime dependencies** are what a user of tapio-py installs. Users get the
  floors in `pyproject.toml`, not our lock, so bumping the lock protects only
  CI. If the floor still allows the affected version, raise the floor above
  it, run `make lock`, and ship the change as a `fix:`, so the release tells
  users to upgrade.
- **Every locked dependency** adds the development tools. A finding that is
  only in this part affects CI and contributors, and never reaches a user.
  Bump the lock with `uv lock --upgrade-package <name>` in a `build:` commit,
  or take the Dependabot pull request if one is already open.

Once the fix is on `main`, merge `main` into the open pull requests to turn
them green again.

When a finding cannot be fixed yet, or does not apply to how tapio uses the
package, add its ID to `[tool.uv.audit]` in `pyproject.toml`. Put a comment
above the entry with the date and the reason. Use `ignore-until-fixed` when
no fixed release exists, so the finding comes back once one does. Remove the
entry when the lock moves past the affected version.

If the failure is an error reaching the advisory database rather than a
finding, nothing was checked. Re-run the job. A run that fails that way twice
is worth an issue of its own.

## Commits and versions

Releases are automatic. python-semantic-release reads the commit types on
`main` and derives the version from them: `feat:` moves the minor, `fix:` moves
the patch, and anything else (`docs:`, `refactor:`, `test:`, `chore:`) moves
nothing. Nothing in the repository holds a version number, so never write one
by hand.

Pull requests are squash-merged. The title becomes the commit subject on
`main` and the body becomes its message. That means:

- the pull request title needs the right type, since it is what the release
  reads;
- the pull request body is permanent history, so write it for someone reading
  `git log` in a year.

The commits on your branch are discarded by the squash, so their messages
matter less. Wrap them at 72 columns anyway.

## Pull requests

Fill in the [template](.github/pull_request_template.md). The parts review
looks at hardest:

- **Why**, not only what. The diff already says what changed.
- **Verification.** Name the tests that would fail if the change regressed.
  "CI is green" is a precondition, not verification.
- **Decisions.** If the change goes against a design invariant in AGENTS.md,
  say so and say why.

Do not hard-wrap the body: one line per paragraph and per list item. GitHub
wraps it at 72 when it builds the squash commit, and a body that is already
wrapped comes out ragged.

## License

tapio is licensed under [Apache-2.0](LICENSE). Under section 5 of that
license, a contribution you submit is licensed under the same terms, with no
separate agreement to sign.
