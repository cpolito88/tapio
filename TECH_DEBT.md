# Technical Debt Backlog

**Repository**: cpolito88/tapio
**Analysis date**: 2026-09-12
**Scope analyzed**: every module under `src/tapio/` (`actor`, `remote`, `cluster`, `dispatch`, `testkit`, `settings`, `validation`, `logging`, `version`, `errors`); the tooling that gates it (`pyproject.toml`, `Makefile`, `.github/workflows/ci.yml`, `.github/dependabot.yml`, `.pre-commit-config.yaml`, `uv.lock`); the test scaffolding (`tests/conftest.py`, `tests/cluster/conftest.py`, `tests/failures.py`, `tests/examples/test_suite.py`, `tests/docs/test_testing_page.py`, `tests/actor/test_supervision.py`) and the coverage report of a full `make lint`, `make type` and `make test` run (805 passed, 8 skipped, 94% line coverage). Three findings were confirmed by running code, and are marked as such.
**Excluded**: the bodies of `examples/tapio_examples/*.py` beyond a skim (dev-only, every module is asserted end to end by `tests/examples/test_suite.py`); the prose of `docs/*.md` (every code block is a snippet include from a tested example, and `docs/testing.md` is executed by `tests/docs/test_testing_page.py`); `tests/benchmarks/` (skipped in CI by design, `--benchmark-skip`); the Pyodide playground page under `docs/playground/`.
**Tracking**: every task is filed as a GitHub issue under [#90](https://github.com/cpolito88/tapio/issues/90), and the numbering lines up: TD-NN is issue 90 + NN.

## Summary
The codebase is in good shape for a 50-commit project: mypy strict, ruff with a wide rule set, 94% coverage, property tests on the gossip merge, and a leak invariant every lifecycle test asserts. Design decisions are written down next to the code and the invariants in `AGENTS.md` match what the runtime does.
The three most urgent items are all confirmed by execution, not by reading:
1. `tapio.__version__` is always `0.0.0+unknown` on every install, because `version.py` looks up the wrong distribution name (TD-01). The handshake diagnostic that carries it is therefore useless in the field.
2. The shipped pytest fixtures and the project's own `system` fixture promise to ignore `TAPIO_*` environment variables and do not: `_env_file=None` only disables the dotenv file (TD-02). A developer's shell silently changes what the suite asserts.
3. A `Cluster` constructed with management settings leaks its bound management socket when the daemon spawn fails afterwards (TD-03).
The remaining debt is concentrated in one place: the link lifecycle in `remote/association.py` and `remote/endpoint.py`, which has absorbed seven `fix:` commits for shutdown and cancellation races and still has its closing-state branches uncovered (TD-13, TD-16).
Twenty-two tasks in all. The first pass produced sixteen plus six open questions; the questions were settled in a review session, two of them after measuring, and are now TD-17 through TD-22.

## Conventions in use
- **Errors**: every library error subclasses `TapioError` (`errors.py`); an error about the message raises at the sender, an error about the recipient becomes a dead letter (`cell.py:239-265`, `ref.py:24-50`). Broad `except Exception` appears only at supervision and subscriber boundaries, each with a comment (`cell.py:859-891`, `events.py:101-104`, `dead_letters.py:280-283`). `contextlib.suppress(BaseException)` appears only in association cleanup, justified in place (`association.py:629, 1092`).
- **Logging**: one module-level `_log = runtime_logger("<area>")` per module; actor code logs through `ctx.log`, which prefixes the path (`logging.py`). No divergence found.
- **Tasks and clocks**: every task is created through `Dispatcher.spawn_task(..., name="tapio-...")` and owned by a cell; time is read through `Dispatcher.now()`. Divergence: `cluster/management.py:196-199, 235-239`, `cluster/cluster.py:181` and `cluster/daemon.py:1139-1146` (TD-09).
- **Typing**: mypy `--strict` over `src` and `examples` only (`Makefile:45-46`); `Protocol` + `@runtime_checkable` for every pluggable seam; `Final` for constants; five `# type: ignore` in the whole of `src`, all in `testkit`. Tests are not type-checked.
- **Docstrings**: Google style, enforced by ruff `D`; a module docstring explains the why, every public name is documented, every module declares `__all__`. Open-ended enumerations are namespaces of string constants (`DeadLetterReason`, `SpawnFailure`); closed ones are `StrEnum` (`MemberStatus`, `OverflowStrategy`).
- **Settings**: `pydantic-settings` classes with `TAPIO_*` prefixes. Tests pass `_env_file=None` (`tests/conftest.py:100`, `testkit/plugin.py:55`); examples pass nothing (`examples/tapio_examples/cluster_join.py:52`). Neither actually blocks environment variables (TD-02).
- **Tests**: the `system` fixture, `eventually` from `tests/failures.py:81-102`, `assert_no_leaked_tasks` around anything that starts a system, and `hypothesis` for the four cluster value types. Fixed `asyncio.sleep` appears 37 times across 10 files (TD-15).

## Quick wins (< 1 day)
- [ ] **[TD-01]** `src/tapio/version.py:31-34`: Look the version up under the distribution name the wheel actually has
  - **Issue**: #91
  - **Severity**: High
  - **Category**: Bug
  - **Problem**: `importlib.metadata.version("tapio")` names the import package, but the distribution is `tapio-py` (`pyproject.toml:5`). The lookup raises `PackageNotFoundError` on every install, editable or wheel, so `__version__` is always the `_UNKNOWN` fallback. Confirmed by running: `tapio.__version__` prints `0.0.0+unknown` while `importlib.metadata.version("tapio-py")` prints `0.1.dev50+gb76b43512` in the same interpreter.
  - **Impact**: The `version` field in `_ClientHello` and `_Welcome` (`remote/handshake.py:111, 128`) always says `0.0.0+unknown`, so the one diagnostic the handshake exists to carry ("which release is the other node running") is useless. `tests/test_package.py:9-10` only asserts truthiness, so the bug is invisible to CI, and the `# pragma: no cover` on line 33 hides that the fallback branch is the one that always runs.
  - **Fix**: Call `_installed_version("tapio-py")`. Replace the tautological test with one that asserts `tapio.__version__ != "0.0.0+unknown"` and that it parses as a PEP 440 version. Drop the `pragma: no cover`.
  - **Estimate**: 1 hour
  - **Confidence**: Verified
  - **Depends on**: -

- [ ] **[TD-02]** `src/tapio/testkit/plugin.py:45-55`, `src/tapio/testkit/remote.py:311-320`, `tests/conftest.py:98-100`: Make the test settings actually ignore `TAPIO_*` environment variables
  - **Issue**: #92
  - **Severity**: High
  - **Category**: Bug
  - **Problem**: All three sites document that the environment is switched off ("Reading `TAPIO_` variables is deliberately switched off", `plugin.py:49-50`) and implement it with `TapioSettings(_env_file=None)`. In pydantic-settings `_env_file` only disables the dotenv source; environment variables are still read. Confirmed by running: with `TAPIO_ASK_TIMEOUT=PT1S TAPIO_VALIDATE_ON_TELL=0` set, `TapioSettings(_env_file=None)` reports `ask_timeout=0:00:01` and `validate_on_tell=False`. The nested `RemoteSettings` and `ClusterSettings` (`tests/cluster/conftest.py:27-36, 73-78`) have the same exposure.
  - **Impact**: A developer or CI runner with any `TAPIO_*` variable exported runs a different suite from everyone else, silently. `TAPIO_VALIDATE_ON_TELL=0` would turn off the delivery-time validation half the actor tests exercise. The shipped `actor_system` fixture makes the same false promise to every downstream project.
  - **Fix**: Build test settings from init values only. Either override `settings_customise_sources` on a small `TestSettings` subclass used by the fixtures (returning `init_settings` alone), or pass the `_env_prefix` init keyword pointed at a prefix nothing sets. Do the same for `RemoteSettings` and `ClusterSettings` in `tests/cluster/conftest.py`. Add one test that `monkeypatch.setenv("TAPIO_ASK_TIMEOUT", "PT1S")` and asserts the fixture still yields the default. Correct the three docstrings to describe what the code now does.
  - **Estimate**: Half a day
  - **Confidence**: Verified
  - **Depends on**: -

- [ ] **[TD-03]** `src/tapio/cluster/cluster.py:115-151`: Close the management listener when the rest of `Cluster.__init__` fails
  - **Issue**: #93
  - **Severity**: Medium
  - **Category**: Bug
  - **Problem**: `open_management_listener` binds the socket first (line 115-117) so that a bad configuration fails the whole construction. If `spawn_system_actor` then raises (`ActorNameError` for a second `Cluster` on the same system, or `ActorSystemTerminating`), nothing closes that socket. `ActorSystem.__init__` guards the equivalent case for the remoting listener (`actor/system.py:143-152`); `Cluster` does not. Confirmed by running: after a second `Cluster(system, management=ManagementSettings(bind_port=0))` raised `ActorNameError`, a listening socket on `127.0.0.1:<port>` was still open in the process.
  - **Impact**: A fixed management port stays bound until the interpreter exits, so the next attempt fails with "address in use" and reads as a configuration problem rather than the spawn failure that caused it.
  - **Fix**: Wrap everything after the bind in `try/except BaseException` that closes `self._management_listener` and re-raises, mirroring `ActorSystem.__init__`. Add a test that builds two clusters on one system with `bind_port=0` and asserts the second bind is released.
  - **Estimate**: 1 hour
  - **Confidence**: Verified
  - **Depends on**: -

- [ ] **[TD-04]** `.pre-commit-config.yaml:4-5, 14-15`: Align the pre-commit ruff pin with the ruff the gate runs
  - **Issue**: #94
  - **Severity**: Medium
  - **Category**: Consistency
  - **Problem**: The hook pins `ruff-pre-commit` at `v0.5.7` and `pre-commit-hooks` at `v4.6.0`, while `uv.lock` resolves `ruff` to `0.16.1` for `make lint` (`pyproject.toml:76`). Eleven minor releases separate the two, with rule additions and formatter changes in between. Dependabot watches only GitHub Actions (`.github/dependabot.yml:6-11`), so nothing moves this pin.
  - **Impact**: A commit the hook accepts can fail `make lint` in CI, and the hook's `--fix` can rewrite code in a way the locked ruff then reformats back. Two formatters disagreeing is a slow source of noise diffs.
  - **Fix**: Bump the two `rev` lines to match the lock, and either add `make hooks-update` that runs `pre-commit autoupdate`, or replace the ruff hook with a `local` hook that runs `uv run ruff` so there is one ruff to pin (the mypy hook on lines 20-26 already takes that form).
  - **Estimate**: 1 hour
  - **Confidence**: Verified
  - **Depends on**: -

- [ ] **[TD-20]** `src/tapio/cluster/management.py:222-239, 59-66`: Bound the concurrent connections the management port will hold
  - **Issue**: #110
  - **Severity**: Medium
  - **Category**: Security
  - **Problem**: `_on_connection` makes a task per accepted connection and counts nothing. Each task is well bounded on its own, by a 30 s request timeout and a 16 KiB header limit, but the number of them is not, so a client that dials and sends nothing holds roughly its dial rate times thirty seconds of tasks, descriptors and stream buffers. Authentication does not close it: the head is read, and only then is the token checked, which is the right order and means an unauthenticated client still costs a task.
  - **Impact**: The management actor shares its loop with the cluster daemon. Starving that loop delays gossip and heartbeat replies, and a node that stops answering probes is a node its watchers call unreachable, which with a downing strategy configured is a node the cluster removes. So flooding this port ends in a member being downed, which is the outcome the port's own bind-beyond-loopback rule exists to protect.
  - **Fix**: Cap concurrent connections at 32 in `_on_connection`, refusing past the cap with a `503` and an immediate close rather than queueing, so a flood fails fast instead of building a backlog. Add a test that fills the cap, asserts the refusal, and asserts a freed slot is reusable. Say in `docs/security.md` and on `ManagementSettings` that the port expects loopback or a sidecar.
  - **Estimate**: Half a day
  - **Confidence**: Verified
  - **Depends on**: -

- [ ] **[TD-05]** `src/tapio/remote/association.py:418-426`: Keep a remote `tell` from another thread from raising after the loop closed
  - **Issue**: #95
  - **Severity**: Low
  - **Category**: Bug
  - **Problem**: `Association.send` hops off-loop callers back with `call_soon_threadsafe`, which raises `RuntimeError` once the loop is closed. `LocalActorRef.tell` catches exactly that and logs a dead letter (`actor/cell.py:272-280`); `PromiseRef.tell` and `AdapterRef.tell` do the same (`actor/ask.py:129-138`, `actor/adapter.py:231-237`). The remote path does not, so `RemoteRef.tell` from a worker thread after shutdown raises into the sender.
  - **Impact**: Breaks the documented rule that `tell` never raises about the recipient (`remote/ref.py:3-6`), and the exception lands in a thread that has no supervisor to take it. The `test_association.py` suite has no off-loop-after-close case, which is why the asymmetry survived the fix that added the hop (#85).
  - **Fix**: Catch `RuntimeError` around the `call_soon_threadsafe` call and log the same "sent after the loop closed" warning the local ref logs. Add a test that terminates a system and then calls `tell` on a remote ref from a thread.
  - **Estimate**: 1 hour
  - **Confidence**: Verified
  - **Depends on**: -

- [ ] **[TD-06]** `src/tapio/remote/association.py:1004-1024`: Treat a non-JSON link frame the way a malformed one is treated
  - **Issue**: #96
  - **Severity**: Low
  - **Category**: Bug
  - **Problem**: `_on_link_frame` calls `link_body(frame)` on line 1004, outside the `try` that starts on line 1006. `link_body` raises `MessageDecodingError` for a body that is not a JSON object (`remote/transport.py:184-203`). That error escapes to `_run`, is caught as a `TapioError` (line 896-898) and closes the link. The comment on lines 1019-1021 states the opposite policy for post-handshake frames: "dropping the link over one would cost every other conversation on it", and a JSON frame that fails model validation is indeed only logged.
  - **Impact**: Two malformed link frames from an authenticated peer get two different outcomes, and the one that costs the whole association is the one the comment says should not. A peer with a serialization bug in its heartbeat takes down every watch and every queued message on the link.
  - **Fix**: Move the `link_body` call inside the `try` and add `MessageDecodingError` to the except clause, so both malformed forms are logged and dropped. Add a test that writes a non-JSON `{"link":` frame and asserts the association stays up.
  - **Estimate**: 1 hour
  - **Confidence**: Verified
  - **Depends on**: -

- [ ] **[TD-07]** `src/tapio/cluster/management.py:43, 420-443`: Reuse the transport's bind and loopback check instead of copying them
  - **Issue**: #97
  - **Severity**: Low
  - **Category**: Structure
  - **Problem**: `management.py:43` imports the private `_is_loopback` from `remote/transport.py:444-460`, and `open_management_listener` (lines 438-443) repeats `bind` from `remote/transport.py:386-393` line for line: the IPv6-literal test, the bracket strip, `create_server`, `setblocking(False)`. Both functions take a settings object with the same `bind_host` and `bind_port` fields.
  - **Impact**: The two copies have already been edited for the same reason once (the empty-host case documented at `transport.py:447-451`); the next such fix has to be found and applied twice, and the private import ties `cluster` to a name `transport` can rename without warning.
  - **Fix**: Make `is_loopback` public in `transport.py` and give `bind` a signature that takes `bind_host` and `bind_port` (or a small `Protocol` both settings classes satisfy). Call it from `open_management_listener`.
  - **Estimate**: 2 hours
  - **Confidence**: Verified
  - **Depends on**: -

- [ ] **[TD-08]** `src/tapio/actor/system.py:113-124, 268-277`, `src/tapio/actor/cell.py:161-167, 339-347`, `src/tapio/actor/ask.py:248-259`, `src/tapio/actor/events.py:5-6`: Bring four docstrings back in line with what the code raises and publishes
  - **Issue**: #98
  - **Severity**: Low
  - **Category**: Docs
  - **Problem**: (1) `ActorSystem.__init__` lists `RuntimeError` and `ValueError` but binds the remoting port on line 138-142, which raises `InsecureRemoteConfig` and `OSError` (`remote/endpoint.py:910-924`); those are the two errors a deployment is most likely to hit. (2) `LocalActorRef.ask` and `ask()` list the ask failures but not `MailboxFullError`, which `deliver(request)` on `ask.py:350` raises straight through for a `FAIL` mailbox (`cell.py:522-534`). (3) `ActorSystem.events`, `ActorRuntime.events` and the `events.py` module docstring all say the stream carries only `PeerUnreachable` and `PeerReachable` ("today, that a peer became unreachable"); `cluster/daemon.py:917-926` also publishes `ClusterDowned` on it. Everything else checked against the implementation was accurate.
  - **Impact**: A caller reading the documented `Raises` sections writes an `except` that misses the error that actually arrives; a subscriber reading `events` learns about `ClusterDowned` only from the cluster docs.
  - **Fix**: Add the two missing exceptions to `ActorSystem.__init__`, add `MailboxFullError` to the two `ask` docstrings, and replace the three "today" sentences with a pointer to the event classes or the list including `ClusterDowned`.
  - **Estimate**: 1 hour
  - **Confidence**: Verified
  - **Depends on**: -

- [ ] **[TD-09]** `src/tapio/cluster/management.py:196-199, 235-239`, `src/tapio/cluster/cluster.py:181`, `src/tapio/cluster/daemon.py:1139-1146`: Create the cluster package's tasks and read its clock through the dispatcher
  - **Issue**: #99
  - **Severity**: Low
  - **Category**: Consistency
  - **Problem**: The dominant convention, inferred from every task in `actor/` and `remote/` (`cell.py:510-512`, `timers.py:218-221`, `endpoint.py:219-221, 273-275, 426`, `association.py:389-391, 554-556`), is `Dispatcher.spawn_task(coro, name="tapio-...")` and `Dispatcher.now()`. The cluster package diverges three ways: `management.py` calls `loop.create_task` directly (named, but not through the dispatcher), `cluster.py:181` calls `asyncio.ensure_future(system.terminate())` and leaves the task unnamed, and `daemon.py:_now` reads `asyncio.get_running_loop().time()` while `RingMonitor` is documented against "the loop's monotonic clock" the dispatcher owns.
  - **Impact**: `assert_no_leaked_tasks` reports leaked tasks by name (`testkit/leaks.py:42`); the unnamed shutdown task shows up as `Task-N`, which is the one failure message in the suite that does not say what leaked. The invariant "every task belongs to a cell" (`AGENTS.md`) is checked by reading, and a task that bypasses the dispatcher is one a reader has to find by grep.
  - **Fix**: Route the three task creations through `system.dispatcher.spawn_task` (exposing the dispatcher on `ActorSystem` if needed, as `runtime.dispatcher` already is), name the shutdown task `tapio-cluster-shutdown:<name>`, and have the daemon take a `now` callable from the dispatcher at construction the way `DeadLetterOffice` does (`system.py:166-172`).
  - **Estimate**: 2 hours
  - **Confidence**: Verified
  - **Depends on**: -

- [ ] **[TD-10]** `src/tapio/actor/behavior.py:526-528`, `src/tapio/remote/spawner.py:580-582`, `src/tapio/actor/adapter.py:64-66`, `src/tapio/dispatch/blocking.py:177-179`: Keep one helper for naming a callable in a message
  - **Issue**: #100
  - **Severity**: Low
  - **Category**: Consistency
  - **Problem**: Four functions implement `getattr(obj, "__qualname__", None) or repr(obj)` under four names (`_name_of` twice, `_describe_adapt`, `describe_blocking`). They are already used across module boundaries: `cell.py:50` imports `describe_blocking` from `dispatch`, and `behavior.py` and `spawner.py` each keep a private copy.
  - **Impact**: The next change to how a callable is rendered (a lambda's file and line, say, which the current form does not give) has to be made four times, and today two error messages can already describe the same lambda differently.
  - **Fix**: Keep `describe_callable` in one small module (`tapio/logging.py` already holds the other "how things are named in messages" code) and import it at the four sites.
  - **Estimate**: 1 hour
  - **Confidence**: Verified
  - **Depends on**: -

- [ ] **[TD-17]** `pyproject.toml:35-41, 44-46`, `src/tapio/cluster/cli.py`: Move the CLI to a `[cli]` extra, so `typer` is not in every install
  - **Issue**: #107
  - **Severity**: Low
  - **Category**: Structure
  - **Problem**: `typer` is a hard runtime dependency, declared beside `pydantic`, and it exists for one console script. Measured in a synced checkout, the chain costs 4.3 MB: `rich` 2.2M, `typer` 1.1M, `click` 908K, `shellingham` 52K. Nothing outside `cluster/cli.py` imports `typer`, and nothing imports `rich` or `click` at all.
  - **Impact**: The README positions this as "a library, not infrastructure. Pip-install it into the service you already have." A service that embeds the runtime and never runs the operator command still carries `rich` and `click`, in its image and in its dependency audit.
  - **Fix**: Declare `typer` under an optional `cli` extra. Keep the console script registered and have its entry point raise `SystemExit` with an install instruction when `typer` is absent, which beats an unregistered script failing with "command not found". Add a test asserting `import tapio` leaves `typer` out of `sys.modules`, so the extra cannot quietly become mandatory again.
  - **Estimate**: 2 hours
  - **Confidence**: Verified
  - **Depends on**: -
  - **Decision**: Settled in review. Rewriting on argparse was rejected: it removes the dependency entirely but costs typer's help formatting and completion for a command whose audience is operators reading `--help`.

- [ ] **[TD-19]** `.github/dependabot.yml:1-11`, `uv.lock`: Give `uv.lock` an update cadence
  - **Issue**: #109
  - **Severity**: Low
  - **Category**: Security
  - **Problem**: Dependabot watches GitHub Actions and stops, with the comment "a bot editing that lock is a separate decision". That decision was never taken, so `uv.lock` moves only when somebody touches it for another reason. The file's own opening paragraph is the argument against that state, written about the Actions it does watch: "Without this the pins go stale quietly."
  - **Impact**: No advisory in the current pins today, which is a fact about today rather than a process. This is the ecosystem that ships to users, so it is the one where staleness costs the most.
  - **Fix**: Add the `uv` ecosystem, shaped like the Actions block: weekly, minors and patches grouped, majors separate, commit prefix `build` so python-semantic-release moves no version. `make lock` stays the way a human re-resolves everything on purpose.
  - **Estimate**: 1 hour
  - **Confidence**: Verified
  - **Depends on**: -
  - **Decision**: Settled in review. The hesitation about an unreviewed bot is covered by `make ci` gating every pull request across four Python versions.

- [ ] **[TD-21]** `src/tapio/cluster/cluster.py:516-539`, `src/tapio/cluster/daemon.py:337-397`: Wake `_until` from the daemon's turn instead of polling
  - **Issue**: #111
  - **Severity**: Low
  - **Category**: Structure
  - **Problem**: `_until` backs `join_seed_nodes` and `leave` and polls every 5 ms for up to 30 s. Its docstring argues an event "would have to be published by every path that can change the state, and a missed one would hang". That does not hold: every message goes through `_receive`, which has one exit at which the state has settled, so one notification there covers every path by construction.
  - **Impact**: Small in cycles and worth fixing for the comment, which is wrong and is the kind of reasoning that gets copied into the next thing that waits on membership. `leave_timeout` defaults to 30 s, so a leave blocked by an unreachable member spins 6000 times observing nothing.
  - **Fix**: Give the daemon an `asyncio.Event` set at the single exit of `_receive`, and have `_until` clear before it checks and then wait. `Mailbox.get:256-268` already documents the clear-before-check ordering this needs, so the pattern and its explanation exist in the repo. Add a test for the wakeup that lands between the check and the wait.
  - **Estimate**: 3 hours
  - **Confidence**: Verified
  - **Depends on**: -
  - **Decision**: Settled in review. Subscribing to cluster events was rejected: they are built for a subscriber's mailbox, `_until` is not an actor, and only `REMOVED` emits one on the leave path.

- [ ] **[TD-22]** `src/tapio/actor/cell.py:800-813`: Stop `ActorCell.stop` suppressing its caller's cancellation
  - **Issue**: #112
  - **Severity**: Low
  - **Category**: Bug (latent)
  - **Problem**: Past the shutdown deadline, `stop` cancels the actor's task and awaits it under `contextlib.suppress(asyncio.CancelledError)`. That cannot distinguish the awaited task's cancellation from the caller's own, so a cancelled caller is swallowed and `stop` returns normally. `association.py:220-247` documents this exact hazard and provides `_cancel_and_wait` for it, and by its rule `stop` is the kind of caller that needs the re-raising form, since it resumes work after the wait.
  - **Impact**: Latent. The callers are `ActorSystem._drain`, a task the system owns and cancels only at loop teardown, and `stop` recursing into children. Nothing observes it today, and the trap is invisible at the call site.
  - **Fix**: Move `_cancel_and_wait` from `association.py` into `tapio/dispatch/`, which already owns the loop and the task lifecycle, and call it from `stop`. Add a test that cancels a sweep waiting on a stuck actor and asserts the cancellation propagates.
  - **Estimate**: 2 hours
  - **Confidence**: Verified
  - **Depends on**: - (pairs with TD-16, which also moves the helper)
  - **Decision**: Settled in review. Documenting why it is safe today was rejected: the comment would need re-verifying whenever a caller is added, and the helper removes the question instead.

## Targeted fixes (1-5 days)
- [ ] **[TD-11]** `src/tapio/cluster/daemon.py:688-734`: Test what a join from a restarted incarnation does to the old one
  - **Issue**: #101
  - **Severity**: Medium
  - **Category**: Tests
  - **Problem**: `_admit` has a branch for a joiner whose address is already known under a different uid (lines 700-716): the stale incarnation is moved to `Down` because "leaving it Up would block convergence on a member that no longer exists". The coverage report shows lines 704-716 never execute. `tests/cluster/test_membership.py` covers joining, leaving and downing but not a node that restarts and rejoins while its previous record is still `Up`.
  - **Impact**: This is the path a rolling restart takes when the old process died without leaving (the `rolling_restart` example leaves gracefully first, so it does not reach it). A regression here leaves a phantom `Up` member blocking convergence for ever, which is exactly the failure the branch exists to prevent, and nothing in CI would notice.
  - **Fix**: In `tests/cluster/test_membership.py`, form a cluster of two, terminate one system, start a new system on the same address (bind to the port the old one had) and join it; assert the old uid reaches `Down` then `Removed` on the survivor and the new uid reaches `Up`. A second test should send a `Join` carrying `status=UP` and assert the daemon admits it as `JOINING` (lines 717-728) rather than raising.
  - **Estimate**: 1 day
  - **Confidence**: Verified
  - **Depends on**: -

- [ ] **[TD-12]** `src/tapio/remote/handshake.py:298-343`: Test the three handshake refusals that guard the wire
  - **Issue**: #102
  - **Severity**: Medium
  - **Category**: Tests
  - **Problem**: Coverage shows `_identify` lines 310-312 (advertised address does not parse) and `_read` lines 337-338 (wrong frame kind) and 341-343 (frame fails model validation) never run. `tests/remote/test_handshake.py` is 363 lines long and exercises the protocol and secret checks, but not these. These are the branches a scanner or a mismatched peer hits first.
  - **Impact**: A change that made `_read` raise `ValidationError` instead of `HandshakeError` would escape the `except (OSError, TapioError, TimeoutError, EOFError)` in `endpoint._handshake` (line 321) and surface as an unhandled task exception at collection time. The security page promises a closed connection and a log line; nothing asserts it for these inputs.
  - **Fix**: Using the raw-socket peer in `tests/remote/peers.py`, send (a) a `client-hello` whose `address` is `"not an address"`, (b) a `heartbeat` frame where a `client-hello` is expected, and (c) a `client-hello` missing `proof`; assert each closes the connection, logs a refusal, and leaves `system.remote.associations` empty and no task behind.
  - **Estimate**: 1 day
  - **Confidence**: Verified
  - **Depends on**: -

- [ ] **[TD-13]** `src/tapio/remote/endpoint.py:731-786`, `src/tapio/remote/association.py:409-417, 456-469, 523-541`: Cover the closing-state branches that the shutdown fixes keep touching
  - **Issue**: #103
  - **Severity**: Medium
  - **Category**: Tests
  - **Problem**: Seven of the last thirty commits are `fix:` commits to these two files about shutdown and cancellation (#35, #44, #74, #81, #82, #86, #87), yet the report shows the code those fixes protect still uncovered: the endpoint's drain of mid-handshake links (`endpoint.py:761-769`), `send` on a closing association (`association.py:411-417`), `watch` on a closing association (465-469), and `adopt` on a closing association (540-541). The 885-line `tests/remote/test_association.py` reaches none of them.
  - **Impact**: Each of the seven fixes was found by a failure in the field or by review, not by a test, and the branches they added are the ones a future refactor (TD-16) will most likely break. The dead-letter reasons `QUARANTINED` versus `NO_ASSOCIATION` on line 411-415 are chosen here and nothing asserts which one a sender sees.
  - **Fix**: Add tests that (a) terminate a system while a raw peer holds a connection open before completing the handshake and assert the socket is closed and no task leaks, (b) `tell` through a remote ref after `forget_all` and assert the `NO_ASSOCIATION` reason, then after a quarantine and assert `QUARANTINED`, (c) watch through a ref whose association is closing and assert `Terminated` arrives at once, (d) have a peer dial in while the existing association is closing and assert the new link is closed by `close_link_later`.
  - **Estimate**: 2 days
  - **Confidence**: Verified
  - **Depends on**: -

- [ ] **[TD-14]** `src/tapio/cluster/singleton.py:53-61, 208-227`, `src/tapio/cluster/router.py:59-60, 171-183`, `src/tapio/actor/router.py:276-285`, `src/tapio/cluster/router.py:231-233`: Share the "subscribe once the daemon exists" retry and the dead-letter office lookup
  - **Issue**: #104
  - **Severity**: Low
  - **Category**: Structure
  - **Problem**: The singleton manager and the group router each carry an identical `_Reconcile` message, a 50 ms `_RETRY_INTERVAL`, a `_SUBSCRIBE_TIMER` key and an `_ensure_subscribed` method that resolves `local_daemon`, sends `Subscribe`, and cancels the timer. The two copies are already drifting: the singleton's constant has a docstring explaining the interval, the router's does not. Separately, both routers reach the dead-letter office by `cast(LocalActorRef, ctx.self_ref).cell.runtime.dead_letters`, once as a module function and once as a method.
  - **Impact**: The next cluster-aware actor (a sharded proxy, a metrics reporter) copies the block a third time, and a change to how the daemon is found (a `Ready` event, say) has to be made in every copy. The `cast` to `LocalActorRef` is the one place in the library where a behavior reaches into its own cell, and it is done twice with two shapes.
  - **Fix**: Move the retry into `cluster/daemon.py` as a helper that takes `(ctx, timers, events)` and returns the daemon ref once it exists, and use it from both. Give `ActorContext` a narrow way to publish a dead letter for a message it forwarded (a `dead_letter(message, recipient, reason)` method, or a `dead_letters` property), and delete both `_office` copies. The `actor/router.py:277-284` comment explains why the context does not expose the office; the second copy is the evidence that routers need it.
  - **Estimate**: 1 day
  - **Confidence**: Verified
  - **Depends on**: -

- [ ] **[TD-15]** `tests/actor/test_supervision.py:571-573, 603-629`, `tests/actor/test_ask.py:276`, `tests/actor/test_system.py:178`, `tests/remote/test_ask.py:100`: Take the timing out of the assertions that measure it
  - **Issue**: #105
  - **Severity**: Low
  - **Category**: Tests
  - **Problem**: The suite has a good `eventually` helper (`tests/failures.py:81-102`) and uses it widely, but 37 fixed `asyncio.sleep` calls remain across 10 files, and a few assertions bound wall-clock time: `assert 0.05 <= waited < 0.2` (`test_supervision.py:629`) allows a 150 ms window for a backoff on a loaded CI runner; `await asyncio.sleep(0.05); assert "PostStop" not in seen` (lines 572-573) proves a negative by waiting a guessed time; the three `elapsed <` checks are generous but still clock-based.
  - **Impact**: These are the tests most likely to fail on a slow matrix leg (four Python versions run in parallel) for a reason unrelated to the change under test. The project has no flake history yet, which is the moment to fix it.
  - **Fix**: For the backoff test, assert on the schedule `Backoff.delay` produces (already pure and tested at lines 397-421) and on the fact that a restart happened after the delay, with the upper bound widened to seconds. Replace "sleep then assert absent" with `expect_no_message`-style windows only where a negative is the point, and `eventually` everywhere else. Keep the `elapsed <` checks but state the bound in terms of the configured timeout, not a literal.
  - **Estimate**: 1 day
  - **Confidence**: Needs confirmation (the flake rate on the four-leg matrix has not been measured; run the suite under `pytest-repeat` or CPU contention first)
  - **Depends on**: -

- [ ] **[TD-18]** `Makefile:44-46`, `pyproject.toml:127-128, 138-141`: Type-check `tests/` the way ruff already lints it
  - **Issue**: #108
  - **Severity**: Low
  - **Category**: Tests
  - **Problem**: `make type` covers `src` and `examples`, so 16k lines of tests are checked only by running them, and the `type: ignore` comments in them are unverified. They have already drifted: `tests/cluster/conftest.py:28, 76-77` carry `# type: ignore[call-arg]` on a call that `tests/conftest.py:100` makes without one. Measured: mypy strict over `tests/` reports 1050 errors in 61 files, of which 765 are `no-untyped-def`, about 200 are `arg-type`, `union-attr`, `call-arg` and `attr-defined`, and 12 are `unused-ignore`.
  - **Impact**: The ~200 in the middle are the ones worth having. An `arg-type` in a test means the test passes something the code under test would never receive, so the test is not testing what it claims. The 12 confirm the dead comments.
  - **Fix**: Add `tests` to `make type` with a `[[tool.mypy.overrides]]` block disabling `no-untyped-def` and `no-untyped-call` for that tree, then clear the ~200 and delete the 12 dead ignores.
  - **Estimate**: 1 day
  - **Confidence**: Verified
  - **Depends on**: -
  - **Decision**: Settled in review. This mirrors the policy already in `pyproject.toml:127-128`, where ruff exempts tests from `ANN001` and `ANN201`, which are `no-untyped-def` in ruff's vocabulary, and from nothing else. Full strict was rejected as annotating 765 sites the project deliberately configured ruff not to ask for.

## Structural refactoring (> 5 days)
- [ ] **[TD-16]** `src/tapio/remote/association.py:220-247, 523-647, 1079-1122`, `src/tapio/remote/endpoint.py:106-124, 233-337, 419-428, 731-786`: Give a link one owner and one close path
  - **Issue**: #106
  - **Severity**: Medium
  - **Category**: Design
  - **Problem**: A socket can be closed from five places with three different rules for what to do about the caller's own cancellation: `Association._release` (on `PostStop`, suppresses `BaseException`), `Association.detach` (for an association adopted after the stop sweep, suppresses `BaseException`), `Association._resume` (closes the retired link of a lost dial race in a `finally`, via `_cancel_and_wait` which re-raises the caller's cancellation), `RemoteEndpoint.close_link_later` (a detached task held in `_closing_links`), and `RemoteEndpoint.close` (drains `_handshakes` and `_closing_links` in a loop "until they stay empty", then detaches leftovers). Four fields exist only to make sure some path finds the socket: `_accepted`, `_socket`, `_retiring`, `_link` (`association.py:301, 323-333`).
  - **Impact**: Seven `fix:` commits in fifty (#35, #44, #74, #81, #82, #86, #87) each added one more branch to this set, and TD-13 shows the branches are still untested. Every new race is found in the field, and each fix has to reason about all five paths at once.
  - **Fix**: Introduce one small owner per socket (a `LinkHandle` with `link`, `reader task`, `close()` that is idempotent and cancellation-safe, and a `closed` future). The association holds at most one current handle and one retiring handle; adopting swaps handles and asks the old one to close; the endpoint holds a set of handles it accepted and not yet handed over. `close()` on the endpoint then becomes "close every handle I still hold and await their `closed` futures", with no second drain loop. Land it behind the tests from TD-13 so the behaviour is pinned before it moves.
  - **Estimate**: 5 to 8 days
  - **Confidence**: Needs confirmation (the shape above is a proposal; the association's FIFO-across-swap guarantee at `association.py:523-556` must be preserved and needs a test before the change)
  - **Depends on**: TD-13
  - **Principle / Pattern**: Single responsibility for resource ownership; RAII-style handle (one object owns the socket and its reader)
  - **Symptom**: Five close sites, four "where is the socket" fields, seven shutdown fixes, and untested closing-state branches in both files
  - **Trade-off**: One more class in `remote/`; `Association` and `RemoteEndpoint` both change shape, so every test in `tests/remote/test_association.py` (885 lines) and `test_endpoint.py` (501 lines) that reaches into `_link`, `_accepted` or `_handshakes` has to be revisited; a migration mistake in this area is a socket leak or a hang at shutdown, so it needs the leak assertions on every new test.

## Needs investigation
All six open questions from the first pass were settled in a review session, so
none remain. Two were measured before being decided, and all six are now tasks
above: TD-17 (#107), TD-18 (#108), TD-19 (#109), TD-20 (#110), TD-21 (#111) and
TD-22 (#112). Each carries a **Decision** line recording what was chosen and
what was rejected.

The two measurements are worth keeping here, since they are what turned a
question into an answer:

- **The CLI dependency chain costs 4.3 MB** in every install: `rich` 2.2M,
  `typer` 1.1M, `click` 908K, `shellingham` 52K. That is what settled TD-17
  towards an extra rather than towards accepting the weight.
- **mypy strict over `tests/` reports 1050 errors in 61 files**, of which 765
  are `no-untyped-def`, about 200 are `arg-type`, `union-attr`, `call-arg` and
  `attr-defined`, and 12 are `type: ignore` comments that suppress nothing.
  The shape of that split is what settled TD-18 towards the relaxed gate: the
  765 are noise the project has already decided it does not want, and the 200
  are the reason to have a gate at all.
