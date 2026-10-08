# Code review of tapio, 2026-10-08

Reviewed at commit `eb4c227` on `main`. The report is the source of truth. Issues filed from it point back here by finding ID.

## 1. Summary

The core actor runtime is careful, and the gate is green: lint, `mypy --strict`, 894 tests at 95% branch coverage, every example, and the docs build. The cluster merge passes property tests on every law. The defects are in the paths that run when something has already gone wrong. 102 findings: 5 Critical, 32 High, 37 Medium, 28 Low. 99 are Confirmed, about 75 of them by a probe that fails on the current tree. 3 are Suspected.

The three things that matter most:

1. **"One deadline for the whole tree" does not hold.** Two stoppers on one wedged actor make `terminate()` raise `CancelledError` and `when_terminated()` hang for ever (CORE-1). A link close has no deadline at all (WIRE-4, LINK-3), and neither does a cancelled handler's cleanup (PERI-1).
2. **Messages are lost with no dead letter** in the association: a message in hand when a close lands (LINK-1), the losing side of a dial race (LINK-2), and every frame after one deeply nested JSON frame kills the reader (WIRE-2, LINK-5).
3. **Cluster safety promises fail under ordinary sequences.** Both sides of a split can win when a join or a removal is in flight (MEMB-1). The singleton runs twice in four reproduced sequences (CLUS-2 to CLUS-5). An empty management token opens a port that can down members (CLUS-1).

## 2. Scope, method and tool evidence

**Method.** I read the core myself first (message, validation, path, signals, ref, mailbox, dead letters, events, cell, watch, supervision, restarts, ask, timers, stash, behavior, construction, system), because every other subsystem inherits its invariants. Five parallel passes then took the remote wire layer (WIRE), the remote link layer (LINK), the cluster membership core (MEMB), the cluster daemon and its surface (CLUS), and the periphery, testkit, docs and test quality (PERI). Every pass worked to the same brief and the same rules of evidence. I re-ran every probe directory and re-read the code behind every Critical finding and a sample of the High ones before accepting them. Duplicates found by two passes were merged: LINK-6 into WIRE-2, the frame-example items in WIRE-15 and LINK-15 into PERI-7, and the periphery pass's `$1` lead into CORE-5. Finding IDs keep the prefix of the pass that found them, so gaps in a numbering are merges, not omissions.

Probes were never added to `tests/`. Each one ran from a scratch directory as `PYTHONPATH=. uv run pytest -c pyproject.toml <file>`. The test code each finding quotes is the version to add to the suite with the fix.

**Severity.** Critical means data loss, silent message loss, deadlock or a security hole. High means wrong behaviour under a realistic sequence. Medium means a correctness risk or a structural problem that will cause bugs. Low means cleanliness. Docs findings are graded by consequence: a wrong claim that would mislead a caller into a bug is High.

**Tool evidence** (all run on `eb4c227`):

| Tool | Result | What it told us |
|---|---|---|
| `make lint` | pass | Nothing. Style nits are not reported here. |
| `make type` (`mypy --strict` over src, examples and tests) | pass, 179 files | The typing findings (CORE-7) came from writing a scratch module against the API, not from the gate. |
| `make test` | 894 passed, 95% branch coverage | Lowest files: `cluster/cli.py` and `cluster/management.py` 88%, `testkit/behavior.py` 90%, `remote/association.py` 91%. Several uncovered branches are where findings sit (for example `cluster/router.py:161`, CLUS-16). |
| `make examples` | 29 passed | `split_brain` takes 3.5 s against the README's 2 s rule (PERI-9). |
| `ruff check --select ALL --statistics src tests` | 2,446 hits | Dominated by rules the project deliberately leaves off: `S101` asserts in tests (1,211), `COM812` (449), `ARG001` (169), `CPY001` (150), `PLR2004` (111). `BLE001` blind except (11) was followed up by hand. The only broad handlers that matter are the *narrow* ones that miss an exception (WIRE-1, WIRE-2, LINK-5), not the broad ones. |
| `vulture src/tapio --min-confidence 60` | 60 hits | Every hit checked. All are public API used by tests or by users (`TestProbe.expect_*`, `Mailbox.user_size`), framework callbacks (a typer command, a pydantic `field_validator`, `settings_customise_sources`), or protocol parameters. `Cluster._shutdown` is a deliberately held task reference. No genuinely dead code was found, apart from one unreachable branch listed in LINK-15 item 1. The history shows a dead-code sweep landed recently (`d77b9e4`, closed #155). |

**Existing issues.** Every one of the 60 issues on the repository is closed. Two closed issues are relevant. Closed #52 fixed the backoff exponent for strategies with `max_restarts`; CORE-6 is the path it left out. Closed #112 changed `ActorCell.stop` to shield the task, and that shield is where CORE-1 comes from. Closed #53 and #92 fixed parts of what PERI-13 and PERI-19 report. No open issue duplicates any finding.

## 3. Findings table

| ID | Severity | Category | Status | File | Title | Issue |
|---|---|---|---|---|---|---|
| CORE-1 | Critical | Bug | Confirmed | `src/tapio/actor/cell.py` | Two stoppers on one wedged actor: the second gets the first one's cancellation, `terminate()` raises `CancelledError` and `when_terminated()` hangs for ever | #177 |
| LINK-1 | Critical | Bug | Confirmed | `src/tapio/remote/association.py` | A message in hand when a close lands is dropped without a dead letter | #178 |
| LINK-2 | Critical | Bug | Confirmed | `src/tapio/remote/endpoint.py` | The losing side of a dial race loses its first frames silently, and can tear down the association | #179 |
| LINK-3 | Critical | Bug | Confirmed | `src/tapio/remote/association.py` | Every consequence of a verdict waits on a socket close that has no deadline | #180 |
| CLUS-1 | Critical | Bug | Confirmed | `src/tapio/cluster/management.py` | An empty token satisfies the beyond-loopback rule and lets anyone in | #181 |
| CORE-2 | High | Bug | Confirmed | `src/tapio/actor/cell.py` | A stop during a restart backoff delivers `PostStop` to the incarnation that already got `PreRestart` | #182 |
| CORE-3 | High | Bug | Confirmed | `src/tapio/actor/timers.py` | A timer tick queued before a restart reaches the new incarnation, which the docs say cannot happen | #183 |
| CORE-4 | High | Bug | Confirmed | `src/tapio/actor/cell.py` | A restarted actor's new incarnation gets `Terminated` for the old incarnation's children | #184 |
| CORE-5 | High | Bug | Confirmed | `src/tapio/actor/path.py` | A user-chosen name starting with `$` collides with `spawn_anonymous` and orphans a live child past `terminate()` | #185 |
| CORE-6 | High | Bug | Confirmed | `src/tapio/actor/restarts.py` | Without `max_restarts`, the backoff exponent never decays and `window` is silently ignored | #186 |
| WIRE-1 | High | Bug | Confirmed | `src/tapio/remote/handshake.py` | A non-ASCII proof crashes the handshake task and the socket is never closed | #187 |
| WIRE-2 | High | Bug | Confirmed | `src/tapio/remote/transport.py` | Deeply nested JSON raises `RecursionError` past every handler, before and after the handshake | #188 |
| WIRE-3 | High | Bug | Confirmed | `src/tapio/remote/transport.py` | `close_server` waits for every accepted connection, so a peer mid-handshake holds `terminate` | #189 |
| WIRE-4 | High | Bug | Confirmed | `src/tapio/remote/transport.py` | `FrameLink.close` waits forever on a peer that stopped reading, and swallows the caller's cancellation | #190 |
| WIRE-5 | High | Bug | Confirmed | `src/tapio/remote/transport.py` | IPv6 remoting cannot work: bracketed hosts cannot be dialled, unbracketed ones cannot be written down | #191 |
| LINK-4 | High | Bug | Confirmed, part suspected | `src/tapio/remote/association.py` | Link frames dropped under load break death watch: a watcher can wait forever | #192 |
| LINK-5 | High | Bug | Confirmed | `src/tapio/remote/association.py` | One inbound frame that raises outside the expected list kills the reader and ends in a false quarantine | #193 |
| LINK-7 | High | Bug | Confirmed | `src/tapio/remote/association.py` | `offer` does not wait while a link is coming up: it dead-letters as buffer-full | #194 |
| LINK-8 | High | Bug | Confirmed | `src/tapio/actor/cell.py` | A ref from before a peer restarted reaches the new incarnation's actor | #195 |
| LINK-9 | High | Bug | Confirmed | `src/tapio/remote/association.py` | A dialler never checks which system answered, so frames are delivered to the wrong system | #196 |
| MEMB-1 | High | Bug | Confirmed | `src/tapio/cluster/downing.py` | Downing strategies count members the two sides can disagree about, so both sides of a split can win | #197 |
| MEMB-2 | High | Bug | Confirmed, part suspected | `src/tapio/cluster/gossip.py` | A restarted node brings back its downed predecessor's unreachability claims | #198 |
| MEMB-3 | High | Bug | Confirmed | `src/tapio/cluster/reachability.py` | Reachability records are never pruned, so one healed split in a ~300-node cluster makes gossip too large to send | #199 |
| CLUS-2 | High | Bug | Confirmed | `src/tapio/cluster/daemon.py` | A singleton host that restarts at the same address is never replaced: no node runs the singleton again | #200 |
| CLUS-3 | High | Bug | Confirmed | `src/tapio/cluster/daemon.py` | A manager that subscribes after the cluster formed runs a second instance while it learns who is oldest | #201 |
| CLUS-4 | High | Bug | Confirmed, part suspected | `src/tapio/cluster/singleton.py` | A leave asked for on another node starts the successor before the host lets go; the docs promise it cannot | #202 |
| CLUS-5 | High | Bug | Confirmed | `src/tapio/cluster/singleton.py` | A host downed by a strategy keeps running its instance while the majority starts another | #203 |
| CLUS-6 | High | Bug | Confirmed, part suspected | `src/tapio/cluster/daemon.py` | A downing strategy that raises (a lease backend that cannot be reached) stops the daemon on every node | #204 |
| CLUS-7 | High | Bug | Confirmed | `src/tapio/cluster/daemon.py` | A node that learns of its own downing as `removed` is never told it was downed | #205 |
| PERI-1 | High | Bug | Confirmed | `src/tapio/dispatch/tasks.py` | The shutdown deadline is not a bound: a cancelled handler's async cleanup runs as long as it likes | #206 |
| PERI-2 | High | Bug | Confirmed | `src/tapio/remote/endpoint.py` | `remote.reconnect` returns success on a link the peer refuses, so the documented repair silently does nothing | #207 |
| PERI-3 | High | Bug | Confirmed | `src/tapio/cluster/cluster.py` | `terminate_on_down=True` is ignored unless a downing strategy is set, so an operator-downed node never shuts down | #208 |
| PERI-4 | High | Docs | Confirmed | `docs/lifecycle.md` | `docs/lifecycle.md` says `PostStop` runs on a restart's teardown; `PreRestart` says it does not | #209 |
| PERI-5 | High | Docs | Confirmed | `src/tapio/actor/router.py` | The `Routers.group` snippet calls `ctx.spawn` without the required name | #210 |
| PERI-6 | High | Docs | Confirmed | `src/tapio/testkit/behavior.py` | The `BehaviorTestKit` module example builds `Spawned("worker")`, which raises | #211 |
| PERI-7 | High | Docs | Confirmed | `docs/remoting.md` | The frame shown in `docs/remoting.md` is rejected by the decoder | #212 |
| PERI-8 | High | Bug | Confirmed | `src/tapio/actor/router.py` | One `Routers.pool(...)` value spawned twice shares one rotation, and half of each pool never gets work | #213 |
| CORE-7 | Medium | Quality | Confirmed | `src/tapio/actor/ref.py` | `ActorRef[T]` is invariant, so the type checker rejects a safe substitution and pushes users to `cast` | #214 |
| WIRE-6 | Medium | Bug | Confirmed | `src/tapio/remote/handshake.py` | The dialler signs any nonce before it checks the listener, and the proof binds nothing else | #215 |
| WIRE-7 | Medium | Bug | Confirmed | `src/tapio/remote/transport.py` | Unauthenticated peers can make the listener buffer `max_frame_bytes` each, with no limit on how many | #216 |
| WIRE-8 | Medium | Bug | Confirmed | `src/tapio/remote/transport.py` | A TLS listener ignores `handshake_timeout`: the TLS handshake gets asyncio's 60 seconds | #216 |
| WIRE-9 | Medium | Docs | Confirmed | `src/tapio/remote/codec.py` | "Strict validation" off the wire is lax validation | #236 |
| WIRE-10 | Medium | Quality | Confirmed | `tests/remote/test_transport.py` | No test ever opens a TLS link | #239 |
| LINK-10 | Medium | Bug | Confirmed | `src/tapio/remote/spawner.py` | A spawner stops, with every worker it started, when an arguments validator raises anything but `ValueError` | #217 |
| LINK-11 | Medium | Bug | Confirmed | `src/tapio/remote/association.py` | A frame being flushed by `_open` vanishes when the reader is cancelled | #218 |
| LINK-12 | Medium | Bug | Confirmed, part suspected | `src/tapio/actor/watch.py` | Releasing a stale `_PeerWatcher` removes the live one that replaced it | #219 |
| LINK-13 | Medium | Bug | Confirmed | `src/tapio/actor/cell.py` | Dead letters from the association's own mailbox: internal messages, wrong recipient, no peer | #220 |
| LINK-14 | Medium | Docs | Confirmed | `docs/unreachable.md` | Docs: "only silence quarantines", and the timings that make a dial look like silence | #236 |
| MEMB-4 | Medium | Quality | Confirmed | `src/tapio/cluster/gossip.py` | `leader` and `converged` are O(members x records) and run several times per daemon message | #221 |
| MEMB-5 | Medium | Bug | Suspected | `src/tapio/cluster/downing.py` | LeaseMajority names a side by an address the other side can also use | #222 |
| MEMB-6 | Medium | Bug | Confirmed | `src/tapio/cluster/downing.py` | `Lease.acquire` is unbounded, and the daemon awaits it inside its turn | #223 |
| MEMB-7 | Medium | Docs | Suspected | `docs/clustering.md` | "A partition drops every link across it" only covers links that exist | #237 |
| MEMB-9 | Medium | Docs | Confirmed | `src/tapio/cluster/gossip.py` | The leader rule as documented leaves out the reachability filter, and its fallback is wider than documented | #237 |
| CLUS-8 | Medium | Bug | Confirmed | `src/tapio/cluster/cluster.py` | `join_seed_nodes` reports success for a node that is removed or downed | #224 |
| CLUS-9 | Medium | Bug | Confirmed | `src/tapio/cluster/daemon.py` | A subscriber that is a remote ref breaks every later turn of the daemon | #225 |
| CLUS-10 | Medium | Bug | Confirmed | `src/tapio/cluster/messages.py` | `Leave` is a registered wire message: any peer past the handshake can walk any member out | #226 |
| CLUS-11 | Medium | Bug | Confirmed | `src/tapio/cluster/management.py` | Over TLS the 32-connection cap and the 30 s budget do not cover the handshake | #227 |
| CLUS-12 | Medium | Bug | Confirmed | `src/tapio/cluster/management.py` | A management certificate that cannot be loaded leaves a bound port that never answers, silently | #228 |
| CLUS-13 | Medium | Docs | Confirmed | `src/tapio/cluster/cluster.py` | `when_downed`, `terminate_on_down` and `ClusterDowned` say only a strategy downs a node; an operator does too | #237 |
| PERI-9 | Medium | Docs | Confirmed | `examples/README.md` | `examples/README.md` omits six of the 28 examples and says every tier has landed | #238 |
| PERI-10 | Medium | Docs | Confirmed | `src/tapio/actor/router.py` | `RoundRobin` claims a shrink never hands the last routee more work, and its own test asserts the opposite | #238 |
| PERI-11 | Medium | Bug | Confirmed, part suspected | `src/tapio/actor/adapter.py` | A released `AdapterRef` publishes dead letters on the sender's thread | #229 |
| PERI-12 | Medium | Bug | Confirmed | `src/tapio/actor/adapter.py` | An adapter made in `setup` leaves one registry entry per restart, not one per actor | #229 |
| PERI-13 | Medium | Bug | Confirmed | `src/tapio/dispatch/blocking.py` | The blocking pool finds its threads by name, so a second system with the same name waits on the first one's threads | #230 |
| PERI-14 | Medium | Bug | Confirmed | `src/tapio/settings.py` | Settings accept values that fail later, inside an actor | #231 |
| PERI-15 | Medium | Docs | Confirmed | `src/tapio/errors.py` | "Every error tapio raises derives from `TapioError`" is false; an invalid actor name raises `ValueError` | #238 |
| PERI-16 | Medium | Quality | Confirmed | `src/tapio/actor/__init__.py` | Runtime internals are exported from `tapio.actor` and `tapio.cluster`, and the top level is inconsistent | #232 |
| PERI-17 | Medium | Docs | Confirmed | `src/tapio/dispatch/__init__.py` | `BlockingPool` and `Dispatcher` are public but do not say what a caller must not do | #238 |
| PERI-18 | Medium | Bug | Confirmed, part suspected | `src/tapio/dispatch/tasks.py` | `cancel_and_wait` treats a cancellation from before the call as aimed at this wait | #233 |
| PERI-19 | Medium | Quality | Confirmed | `src/tapio/testkit/behavior.py` | `BehaviorTestKit` reads `TAPIO_*` from the environment by default | #234 |
| PERI-20 | Medium | Docs | Confirmed | `docs/index.md` | "Every code block is a snippet include from `examples/`" is false, and the broken blocks are the ones that are not | #238 |
| PERI-21 | Medium | Docs | Confirmed | `README.md` | README contradicts itself on validation cost, and names overflow strategies by values the setting rejects | #238 |
| PERI-22 | Medium | Quality | Confirmed | `tests/conftest.py` | The suite's `system` fixture does not assert the leak invariant that AGENTS.md says every system-starting test asserts | #239 |
| PERI-23 | Medium | Quality | Confirmed | `tests/actor/test_system.py` | Tests use `asyncio.sleep` as synchronisation where an `eventually` or a probe is available | #239 |
| CORE-8 | Low | Bug | Confirmed | `src/tapio/actor/cell.py` | The shutdown-deadline warning always says "handling no message" | none (Low) |
| CORE-9 | Low | Docs | Confirmed | `src/tapio/actor/system.py` | `ActorSystem.resolve` skips the `expect` check for a local address, contrary to its `Raises` section | none (Low) |
| WIRE-11 | Low | Bug | Confirmed | `src/tapio/remote/codec.py` | The version check accepts `true` and `1.0` | none (Low) |
| WIRE-12 | Low | Bug | Confirmed | `src/tapio/remote/address.py` | Uid and port parsing accept non-ASCII digits and signs | none (Low) |
| WIRE-13 | Low | Quality | Confirmed | `src/tapio/remote/registry.py` | Registering one class under a second key silently changes the key it is sent under | none (Low) |
| WIRE-14 | Low | Quality | Confirmed | `src/tapio/remote/codec.py` | `decode` parses every payload three times and rounds numbers through `float` | none (Low) |
| WIRE-15 | Low | Docs | Confirmed | `as listed` | Smaller docstring and docs inaccuracies | none (Low) |
| LINK-15 | Low | Quality | Confirmed | `` | Low findings | none (Low) |
| MEMB-8 | Low | Quality | Confirmed | `src/tapio/cluster/gossip.py` | The merge laws hold only for canonical values, and nothing enforces that a value is canonical | none (Low) |
| MEMB-10 | Low | Docs | Confirmed | `docs/clustering.md` | The docs say only the leader downs a member, but every node does | none (Low) |
| MEMB-11 | Low | Docs | Confirmed | `docs/clustering.md` | "Every member is watched by exactly that many others" is false for small clusters | none (Low) |
| MEMB-12 | Low | Quality | Confirmed | `src/tapio/settings.py` | `ClusterSettings` accepts timings that cannot work | none (Low) |
| CLUS-14 | Low | Bug | Suspected | `src/tapio/cluster/daemon.py` | The first subscriber misses events produced later in the turn that subscribed it | none (Low) |
| CLUS-15 | Low | Docs | Confirmed | `src/tapio/cluster/daemon.py` | The replay leaves out members that are leaving, contradicting "no window in which a late subscriber has missed something" | none (Low) |
| CLUS-16 | Low | Bug | Confirmed | `src/tapio/cluster/router.py` | The group router adds joining and leaving members when they become reachable again | none (Low) |
| CLUS-17 | Low | Bug | Confirmed | `src/tapio/cluster/cluster.py` | `timeout=timedelta(0)` silently means "the default" | none (Low) |
| CLUS-18 | Low | Quality | Confirmed | `src/tapio/cluster/daemon.py` | The first seed is recognised by comparing raw address strings | none (Low) |
| CLUS-19 | Low | Docs | Confirmed | `as listed` | Smaller docs inaccuracies | none (Low) |
| CLUS-20 | Low | Quality | Confirmed | `src/tapio/cluster/cli.py` | CLI: `--client-key` without `--client-cert` is ignored, and the token only travels on the command line | none (Low) |
| PERI-24 | Low | Quality | Confirmed | `tests/examples/test_suite.py` | The examples completeness check compares against a hand-kept set, not the tests | none (Low) |
| PERI-25 | Low | Docs | Confirmed | `src/tapio/logging.py` | `describe_callable` says a lambda falls back to `repr`; it does not | none (Low) |
| PERI-26 | Low | Docs | Confirmed | `docs/blocking.md` | `docs/blocking.md` misdescribes the thread check and omits that a wedged call blocks interpreter exit | none (Low) |
| PERI-27 | Low | Quality | Confirmed | `src/tapio/testkit/probe.py` | `TestProbe.expect_terminated` takes whichever signal is next, and loses it on a mismatch | none (Low) |
| PERI-28 | Low | Docs | Confirmed | `src/tapio/testkit/remote.py` | `LinkFaults.delay` throttles rather than delays, `drop` replaces, and a second `link_faults` orphans the first | none (Low) |
| PERI-29 | Low | Quality | Confirmed | `src/tapio/testkit/leaks.py` | `assert_no_leaked_tasks` reports the leak instead of the block's own failure | none (Low) |
| PERI-30 | Low | Docs | Confirmed | `CONTRIBUTING.md` | `CONTRIBUTING.md` does not carry the commit and PR rules it is said to summarise | none (Low) |
| PERI-31 | Low | Docs | Confirmed | `various` | Smaller doc inaccuracies | none (Low) |
| PERI-32 | Low | Quality | Confirmed | `tests/cluster/test_{member,reachability,clock,gossip}.py` | Property tests cover the merge laws; the leader and downing decisions have none | none (Low) |

## 4. Findings in full

Ordered by severity, then by subsystem from the core outward.

## Critical

### [CORE-1] Two stoppers on one wedged actor: the second gets the first one's cancellation, `terminate()` raises `CancelledError` and `when_terminated()` hangs for ever
**Severity:** Critical
**Category:** Bug
**Status:** Confirmed (probe fails today; proposed fix verified by monkeypatch, 545 actor and remote tests still pass)
**Location:** `src/tapio/actor/cell.py:879-898`, surfacing at `src/tapio/actor/system.py:699`
**Issue:** #177

**What's wrong.** `ActorCell.stop` waits for the actor's task through `asyncio.shield`:

```python
883        try:
884            async with asyncio.timeout_at(deadline):
885                await asyncio.shield(self._task)
886        except TimeoutError:
...
891            await cancel_and_wait(self._task)
```

Nothing stops two callers running `stop` on the same cell at once. A restart (`_restart` calls `_stop_children` with its own deadline), an actor stopping itself (`_stop_self`) and the system drain can all reach the same child. When the first stopper's deadline passes, it cancels the task. `asyncio.shield` propagates the inner task's cancellation to its outer future, so the second stopper's `await` raises `CancelledError`. That error was not aimed at the second stopper, and nothing catches it: `timeout_at` only converts its own cancellation. When the second stopper is the system drain, the drain task ends cancelled. `_drain` never reaches `self._terminated.set()`, the `/system` guardian (remoting endpoint, cluster daemon) is never stopped, and the blocking pool is never shut down.

**Why it matters.** It breaks "shutdown races one deadline for the whole tree" and turns it into a deadlock. `terminate()` raises `CancelledError` into a caller nobody cancelled, which is the caller's own cancellation as far as asyncio can tell, and so it usually ends that task too. `when_terminated()` never returns, so a service that awaits it to decide whether to exit hangs. The trigger is ordinary: an actor whose child is stuck in a handler (a slow HTTP call), and a shutdown that starts while that actor is restarting or stopping itself. History: the shield was introduced by the fix for #112. The probe output:

```
OUTCOME terminate raised CancelledError drain cancelled: True
WHEN_TERMINATED hangs
```

**Proposed fix.** Wait with `asyncio.wait`, which neither raises the task's own outcome into the caller nor cancels the task when the caller is cancelled. That is exactly the pair of properties the shield was there for:

```diff
         self._mailbox.put_system(PostStop())
-        try:
-            async with asyncio.timeout_at(deadline):
-                await asyncio.shield(self._task)
-        except TimeoutError:
-            await cancel_and_wait(self._task)
-            self._log.warning(
-                "did not stop within the shutdown deadline while handling %s; "
-                "cancelled",
-                type(self._current).__name__
-                if self._current is not None
-                else "no message",
-            )
+        remaining = max(deadline - self._runtime.dispatcher.now(), 0.0)
+        # asyncio.wait, not shield: the task may have been cancelled by another
+        # stopper, and that cancellation is not this caller's.
+        done, _ = await asyncio.wait({self._task}, timeout=remaining)
+        if not done:
+            current = self._current
+            await cancel_and_wait(self._task)
+            self._log.warning(
+                "did not stop within the shutdown deadline while handling %s; "
+                "cancelled",
+                type(current).__name__ if current is not None else "no message",
+            )
```

Combine it with the bound PERI-1 proposes for `cancel_and_wait`. Alternative considered: catch `CancelledError` around the shield and re-raise only when `asyncio.current_task().cancelling()` is set. That works, but it puts back the pattern `dispatch/tasks.py` exists to keep in one place.

**Test to prove it** (`probes/test_core_probes3.py`, fails today; passes with the fix patched in):

```python
async def test_terminate_finishes_when_a_restart_cancels_a_wedged_child():
    wedged = asyncio.Event()

    async def child(msg: Wedge) -> Behavior[Wedge]:
        wedged.set()
        await asyncio.sleep(3600)
        return Behaviors.same()

    def parent(ctx: ActorContext[Fail]) -> Behavior[Fail]:
        ctx.spawn(Behaviors.receive_message(child), "kid").tell(Wedge())

        async def on_msg(msg: Fail) -> Behavior[Fail]:
            raise RuntimeError("boom")

        return Behaviors.receive_message(on_msg)

    settings = IsolatedTapioSettings(shutdown_timeout=timedelta(milliseconds=300))
    system = ActorSystem("probe", settings=settings)
    ref = system.spawn(
        Behaviors.supervise(Behaviors.setup(parent)).on_failure(SupervisorStrategy.restart()),
        name="p",
    )
    await wedged.wait()
    ref.tell(Fail())          # the restart stops "kid" against its own deadline
    await asyncio.sleep(0.1)  # terminate races a later deadline on the same child
    await asyncio.wait_for(system.terminate(), timeout=3)
    await asyncio.wait_for(system.when_terminated(), timeout=3)
```

---

### [LINK-1] A message in hand when a close lands is dropped without a dead letter
**Severity:** Critical
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/remote/association.py:644-654`
**Issue:** #178

**What's wrong**

```python
644    async def _on_message(self, message: AssociationMessage) -> Behavior[AssociationMessage]:
648        if self._closing and not isinstance(message, Close):
...
654            return Behaviors.stopped()
```

`close()` sets `_closing` and appends `Close` to the back of the mailbox. If any `Outbound` is queued ahead of it, the next turn takes that `Outbound`, sees `_closing`, and returns `stopped()`. The `Outbound` in hand is neither written, nor put in `_pending` (which `_release` dead-letters), nor returned to the mailbox (which `_finish` drains). It is gone.

This is the ordinary path, not only the "mailbox full" one the comment describes: every `close()` that runs while the actor has a backlog hits it. Callers of `close()` include the reader on EOF, a failed write (`_write` line 732), a failed heartbeat, `_declare_unreachable`, `drop_links`, and the endpoint's restart detection.

**Why it matters** Silent message loss. `docs/remoting.md` says undeliverable messages go to dead letters "on whichever side noticed", and this side noticed. A burst of tells followed by a link failure loses exactly one message, and nothing records it.

**Proposed fix** Account for whatever is in hand before stopping:

```python
if self._closing and not isinstance(message, Close):
    if isinstance(message, Outbound):
        # `_release` dead-letters `_pending`, so this one is accounted for
        # with the rest of what never left.
        self._pending.append(message)
    return Behaviors.stopped()
```

I rejected dead-lettering inline here: `_release` already owns the accounting for frames that never left, with one reason and one detail, and putting the frame there keeps that in one place.

**Test to prove it** Ran as `probes/remote-link/test_link_close_drop.py::test_every_tick_queued_behind_a_close_is_delivered_or_dead_lettered`:

```python
async def test_every_tick_queued_behind_a_close_is_delivered_or_dead_lettered() -> None:
    with assert_no_leaked_tasks():
        async with two_nodes() as nodes:
            seen: list[int] = []
            worker = nodes.beta.spawn(counting(seen), "worker")
            ref = await nodes.alpha.resolve(uri(nodes.beta, worker), expect=Tick)
            ref.tell(Tick(n=0))
            await eventually(lambda: seen == [0])
            letters: list[DeadLetter] = []
            nodes.alpha.dead_letters.subscribe(letters.append)
            for n in range(1, 6):
                ref.tell(Tick(n=n))
            drop_links(nodes.alpha)
            await asyncio.sleep(0.2)
            dead = sorted(x.message.n for x in letters if isinstance(x.message, Tick))
            assert sorted(seen[1:] + dead) == [1, 2, 3, 4, 5]
```

Output on `main`: `assert [2, 3, 4, 5] == [1, 2, 3, 4, 5]`. Tick 1 is neither delivered nor dead-lettered.

---

### [LINK-2] The losing side of a dial race loses its first frames silently, and can tear down the association
**Severity:** Critical
**Category:** Bug
**Status:** Confirmed (with the losing side's accept delayed by 100 ms, which is ordinary network jitter)
**Location:** `src/tapio/remote/endpoint.py:385-388`, `src/tapio/remote/association.py:935-964`, `src/tapio/remote/handshake.py:166-180`
**Issue:** #179

**What's wrong** When both ends dial, the acceptor whose own dial wins finishes `accept` (which has already written `welcome`) and then closes the inbound link without reading it:

```python
385            elif _wins(existing.initiator, peer):
386                _log.debug("closing a second link from %s; ours won", peer)
387                self.close_link_later(link, peer)
```

The dialler on the other side does not know it lost. As soon as it reads the `welcome`, `_open` flushes `_pending` onto that link. `_pending` is never empty here, because a send is what started the dial. Those frames go into a socket the winner is closing unread. The writes succeed locally, so no dead letter is published. The next write or read on that link then fails with a reset or EOF, and `_run` calls `close()` on the whole association: the remaining frames are dead-lettered, every watcher is told `Terminated`, and `PeerUnreachable` is published, all for a peer that is fine.

Whether the loser's `adopt` of the winning link happens before or after its own dial completes is decided by which of two symmetric third legs arrives first, so in a real simultaneous dial this is roughly a coin flip. The existing dial-race tests (and my 10 undelayed probe runs) all happen to get the safe order inside one event loop.

**Why it matters** Silent loss of the frames that triggered the dial, including `Watch` frames (whose watcher then waits forever, see LINK-4), plus false `Terminated` and `PeerUnreachable` under a condition the code calls "normal under load". The `adopt` docstring promises "The association survives. The queue, the mailbox ... every watch across it are unchanged", which does not hold on this order.

**Proposed fix** Decide the race before any message frame can be written. The acceptor knows the answer after reading the client hello and before writing the welcome. Split `accept` so the endpoint decides between them, and send a refusal frame that says "superseded" instead of a welcome. The dialler treats that refusal as "keep `_pending`, keep the association, and wait up to `handshake_timeout` for the inbound link to be adopted", not as a link failure. Nothing is written to the losing link, so nothing is lost and FIFO holds. A peer on the old protocol sees a `HandshakeError` from an unknown frame, which is no worse than today, but the new frame kind is a wire change and the pull request should say whether `PROTOCOL_VERSION` moves.

Rejected: having the winner keep reading the losing link until the loser retires it. That stops the silent loss, but the winner then reads two links from one peer concurrently, so frames sent before and after the swap can be delivered out of order, which breaks FIFO per association.

**Test to prove it** Ran as `probes/remote-link/test_dial_race.py::test_a_dial_race_whose_losing_side_accepts_late_loses_nothing_silently`. It monkeypatches `tapio.remote.endpoint.accept` to sleep 100 ms on the losing system after the real handshake, then has both systems tell five ticks to each other in one loop turn. Output on `main`:

```
AssertionError: ([99], [('link-failed', 'the association with tapio://alpha@... stopped before it left'), ...],
  [PeerUnreachable(peer='tapio://alpha@127.0.0.1:37177', ..., quarantined=False)])
assert [2, 3, 4, 5] == [1, 2, 3, 4, 5]
```

Tick 1 is lost silently, ticks 2 to 5 are dead-lettered, and a false `PeerUnreachable` is published.

---

### [LINK-3] Every consequence of a verdict waits on a socket close that has no deadline
**Severity:** Critical
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/remote/association.py:1066-1094`, `src/tapio/remote/handle.py:118-128`, `src/tapio/remote/transport.py:323-332`
**Issue:** #180
**Related:** the unbounded close it waits on is WIRE-4.

**What's wrong**

```python
1075        try:
1076            await self._close_sockets()
1077        finally:
1082            while self._pending: ...dead-letter...
1093            self._end_watches()
1094            self._host.forget(self)
```

`_close_sockets` ends in `FrameLink.close`, which calls `writer.close()` and then `await writer.wait_closed()` with no limit. asyncio does not report the connection lost until its write buffer has drained, so for a peer that is not reading, `wait_closed` waits until TCP gives up, which with zero-window probing can be never. That is exactly the peer `_write_budget` exists to give up on, and any peer behind a real partition while traffic is buffered. Until the close returns: no `Terminated` reaches any watcher, the pending frames are not dead-lettered, `PeerUnreachable` is not published, and the association stays in the endpoint's table (so `association_for` and the cluster's `linked` still see it).

Two follow-on effects:
- `FrameLink.close` suppresses `CancelledError`, so the shutdown deadline's cancellation is swallowed rather than ending the wait cleanly. `terminate` takes the full `shutdown_timeout` with one such peer.
- `_resume` awaits the same unbounded close of the retired link before it reads the winning one. That is the hang commit eaa3dfe listed under "Not in this PR".

**Why it matters** Watchers wait indefinitely on a peer this node has already written off. `docs/unreachable.md` says the `Terminated`, the quarantine and the event "happen together". Here the quarantine happens and the rest does not.

**Proposed fix** Do the accounting first, then close, and bound the close:

```python
async def _release(self) -> None:
    self._closing = True
    self._fail_ready("the association stopped")
    self._dead_letter_pending()
    self._end_watches()
    self._host.forget(self)
    await self._close_sockets()
```

In `LinkHandle.close` (or `FrameLink.close`, which the transport reviewer owns), wait for the graceful close for a bounded time and then abort:

```python
self._writer.close()
try:
    async with asyncio.timeout(budget):
        await self._writer.wait_closed()
except TimeoutError:
    self._writer.transport.abort()
```

Re-raise the caller's own cancellation instead of suppressing it, as `cancel_and_wait` does. I rejected aborting every close unconditionally: a link that ends for an ordinary reason should still flush what it already accepted.

**Test to prove it** Ran as `probes/remote-link/test_stuck_peer.py`. A raw server completes the real `accept` handshake and then never reads. Alpha tells it 40 one-megabyte `Blob`s while a local actor watches a ref on it (`unreachable_after=300ms`).

```python
await eventually(lambda: bool(endpoint.quarantined), within=3)              # passes
await eventually(lambda: any(s.startswith("terminated") for s in seen), within=3)  # fails
```

Output on `main`: `AssertionError: ([], Association('tapio://stuck@127.0.0.1:36791', quarantined), 0, [])`. The peer is quarantined, but the watcher has heard nothing, no dead letters were published, and the association is still in the table three seconds later. The second test in the file shows `terminate()` taking `10.0027s` (the whole `shutdown_timeout`) with such a link open.

---

### [CLUS-1] An empty token satisfies the beyond-loopback rule and lets anyone in
**Severity:** Critical
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/cluster/management.py:111-115`, `management.py:353-368`, `src/tapio/settings.py:279`
**Issue:** #181

**What's wrong.** The bind guard only checks that a token exists, and the comparison accepts an empty presented value:

```python
# management.py:111
authenticates_the_operator = settings.token is not None or (...)
# management.py:356
presented = headers.get("authorization", "")
scheme, _, value = presented.partition(" ")
...
return hmac.compare_digest(value.encode("latin-1"), token.get_secret_value().encode("utf-8"))
```

`ManagementSettings(bind_host="0.0.0.0", token=SecretStr(""))` passes `verify_management_security`. Any request with `Authorization: Bearer` (or `bearer` alone, since the value is stripped) then compares `b""` to `b""` and is authorized. `TAPIO_MANAGEMENT_TOKEN=""` gives the same result. That is what a deployment template produces when its secret is unset.

**Why it matters.** The guard's documented purpose is to make "a port that can down a member, reachable from any host with nothing to prove" fail to start (settings.py:283-285, docs/security.md). Here such a port starts and serves strangers. Anyone who can reach it can `POST /down` any member.

**Proposed fix.** Refuse an empty token at the settings layer, so the error appears where the configuration is written:

```python
token: Annotated[SecretStr, Field(min_length=1)] | None = None
```

Also have `_authorized` return `False` when the configured secret is empty, as a defence in depth. Rejected alternative: treating `""` as `None`. That turns a typo into a silently unauthenticated loopback port, and beyond loopback it moves the error to a less obvious message.

**Test to prove it.** Probe `test_an_empty_token_does_not_open_the_port_to_anyone`, which fails today:
```
E   AssertionError: (200, {'address': 'tapio://node1@127.0.0.1:45663', 'leader': ..., 'converged': True, 'members': [...]})
E   assert 200 == 401
```
After the fix, the construction itself should raise, so the test becomes `with pytest.raises(ValidationError): IsolatedManagementSettings(bind_host="0.0.0.0", token=SecretStr(""))`.

---


## High

### [CORE-2] A stop during a restart backoff delivers `PostStop` to the incarnation that already got `PreRestart`
**Severity:** High
**Category:** Bug
**Status:** Confirmed (probe fails today)
**Location:** `src/tapio/actor/cell.py:1111`, `1119-1130`, `1160-1168`
**Issue:** #182

**What's wrong.** `_restart` delivers `PreRestart` to `self._signalling`, the failed incarnation's behavior. It clears `_signalling` only after the backoff (line 1130). If a `PostStop` arrives during `_backoff`, it is run through `_run_lifecycle_hook`, which delivers it to the same `_signalling`:

```python
1164                    signal = await self._mailbox.get_system()
1165                    if isinstance(signal, PostStop):
1166                        await self._run_lifecycle_hook(signal)
```

**Why it matters.** `PreRestart`'s docstring (`signals.py:41-49`) says "`PostStop` does not follow, because a restart is not a stop. Releasing resources twice would be as wrong as never releasing them." An actor that releases a resource in both handlers (which is what the docs tell it to do, see PERI-4) releases it twice: a double close, a double decrement, a double "session ended" event. The trigger is system shutdown, or a parent's stop, during any backoff window, and backoffs are long by design (seconds). Probe output: `['PreRestart', 'PostStop']`.

**Proposed fix.** The failed incarnation has had its last signal once `PreRestart` is delivered. Clear the handler before the backoff instead of after it:

```diff
         await self._run_lifecycle_hook(PreRestart())
+        # The failed incarnation has had its last signal. A stop during the
+        # backoff below must not reach it as well.
+        self._signalling = None
         self._timers.cancel_all()
         self._discard_stash()
         await self._stop_children(self._own_deadline())
         ...
-        self._signalling = None
         try:
             behavior = self._construct(self._initial)
```

Alternative considered: skip the hook in `_backoff`. That has the same effect but leaves a stale handler reachable from any other path.

**Test to prove it** (`probes/test_core_probes.py`, fails today with `['PreRestart', 'PostStop']`):

```python
async def test_stop_during_backoff_does_not_deliver_poststop_after_prerestart():
    seen: list[str] = []

    def incarnation() -> Behavior[Fail]:
        async def on_msg(ctx: ActorContext[Fail], msg: Fail) -> Behavior[Fail]:
            raise RuntimeError("boom")

        async def on_sig(ctx: ActorContext[Fail], sig: Signal) -> Behavior[Fail]:
            seen.append(type(sig).__name__)
            return Behaviors.same()

        return Behaviors.receive(on_msg, on_signal=on_sig)

    five = timedelta(seconds=5)
    strategy = SupervisorStrategy.restart(backoff=Backoff(min_backoff=five, max_backoff=five))
    system = ActorSystem("probe", settings=IsolatedTapioSettings())
    ref = system.spawn(
        Behaviors.supervise(Behaviors.setup(lambda ctx: incarnation())).on_failure(strategy),
        name="w",
    )
    ref.tell(Fail())
    await eventually(lambda: "PreRestart" in seen)
    await system.terminate()
    assert seen == ["PreRestart"]
```

---

### [CORE-3] A timer tick queued before a restart reaches the new incarnation, which the docs say cannot happen
**Severity:** High
**Category:** Bug
**Status:** Confirmed (probe fails today)
**Location:** `src/tapio/actor/timers.py:9-11`, `202-211`, `296-305`; `src/tapio/actor/behavior.py:647-650`; `src/tapio/actor/cell.py:1115`
**Issue:** #183

**What's wrong.** A restart calls `self._timers.cancel_all()`, which cancels the timer tasks. A tick that already fired is a message on the user lane, and the mailbox survives a restart by design, so the replacement incarnation handles it. `cancel` documents this ("A tick already on the mailbox is not retracted"), but three places promise more:

- `timers.py:9-11`: "so a tick from an incarnation that no longer exists can never reach the one that replaced it."
- `behavior.py:647-650`: "A tick from an incarnation that is gone cannot reach the one that replaced it."
- `docs/getting-started.md`: "A tick scheduled by the incarnation that just failed must not reach the one replacing it."

**Why it matters.** A tick queued behind the message that crashed the actor is the normal case under load, not a corner. A timeout tick ("the request you started has expired") then reaches a fresh incarnation that never started a request. An actor written to the documented guarantee has no reason to guard against that, so it acts on a timeout for state that does not exist. Pekko solves this with a generation number on each timer message. Probe: the second incarnation, which starts no timer, receives the first incarnation's tick (`ticks_seen_by == [2]`).

**Proposed fix.** Give the scheduler a generation that `cancel_all` advances, deliver ticks wrapped in a `Carrier` that records it, and drop stale ones in the cell where `AdaptedMessage` is already unwrapped:

```python
# timers.py
class TimerTick(Carrier):
    generation: int

class TimerScheduler(Generic[T]):
    def cancel_all(self) -> None:
        self._generation += 1
        ...
    def _fire(self, message: T) -> None:
        self._cell.deliver_timer(TimerTick(payload=message, generation=self._generation))

# cell.py, _on_message
if isinstance(message, TimerTick):
    if message.generation != self._timers.generation:
        return  # scheduled by an incarnation that is gone
    message = message.payload
```

`deliver_timer` skips the declared-type check, which already ran when the timer was started. Alternative considered: scan the mailbox for ticks on restart. That costs O(queue) on every restart and needs the mailbox to know about timers. The cheaper alternative is to correct the three docstrings to match `cancel`. That is honest but leaves the footgun.

**Test to prove it** (`probes/test_core_probes2.py`, fails today with `[2]`):

```python
async def test_tick_from_failed_incarnation_does_not_reach_replacement():
    incarnations: list[int] = []
    ticks_seen_by: list[int] = []
    gate, blocked = asyncio.Event(), asyncio.Event()

    def factory(timers) -> Behavior[Cmd]:
        n = len(incarnations) + 1
        incarnations.append(n)
        if n == 1:
            timers.start_single("t", Tick(), timedelta(milliseconds=10))

        async def on_msg(ctx: ActorContext[Cmd], msg: Cmd) -> Behavior[Cmd]:
            if isinstance(msg, Block):
                blocked.set()
                await gate.wait()
            elif isinstance(msg, Fail):
                raise RuntimeError("boom")
            else:
                ticks_seen_by.append(n)
            return Behaviors.same()

        return Behaviors.receive(on_msg, msg_type=Cmd)

    async with ActorSystem("probe", settings=IsolatedTapioSettings()) as system:
        ref = system.spawn(
            Behaviors.supervise(Behaviors.with_timers(factory)).on_failure(
                SupervisorStrategy.restart()
            ),
            name="w",
        )
        ref.tell(Block())
        ref.tell(Fail())
        await blocked.wait()
        await asyncio.sleep(0.05)  # the tick fires and queues behind Fail
        gate.set()
        await eventually(lambda: len(incarnations) == 2)
    assert ticks_seen_by == []
```

---

### [CORE-4] A restarted actor's new incarnation gets `Terminated` for the old incarnation's children
**Severity:** High
**Category:** Bug
**Status:** Confirmed (probe fails today)
**Location:** `src/tapio/actor/cell.py:1117` (`_restart` stops children), `1385-1389` (`notify_terminated`), `1376-1383`
**Issue:** #184

**What's wrong.** A restart stops the children with `_stop_children`. Each child's `_finish` runs `DeathWatch.release`, which calls the parent's `notify_terminated`. The parent is still alive, so a `Terminated` goes on its system lane. After the restart that signal is delivered to the new behavior, through `_signalling`. The new incarnation never watched the old child. It does not even know the child existed.

**Why it matters.** `Terminated`'s docstring says it is delivered "to everyone who called `ctx.watch` on it". The common pattern this breaks is the one the docs teach (`death_watch` example, `chat_sessions`): spawn children in `setup`, watch them, and react to `Terminated` by evicting or respawning. After a restart, the new incarnation evicts an entry it does not have (a `KeyError` on `dict.pop`, which fails the actor again and restarts it again until `max_restarts` stops it), or respawns a replacement for a child it already respawned in `setup` (`ActorNameError`). Akka Typed's restart supervisor absorbs exactly these signals while it waits for the old children to stop. Probe: incarnation 2 receives `Terminated` for `$1`, which incarnation 1 spawned.

**Proposed fix.** Unwatch the children before stopping them, so their termination is not reported to a behavior that is about to be replaced:

```diff
         self._timers.cancel_all()
         self._discard_stash()
+        # The restart stops these itself. Their Terminated would reach the next
+        # incarnation, which never watched them.
+        for child in list(self._children.values()):
+            self.unwatch(child.ref)
         await self._stop_children(self._own_deadline())
```

Alternative considered: filter `Terminated` in `notify_terminated` to refs still in `_watch`. That is right as well and covers a signal queued before the restart, but it changes the `watch` path for already-dead targets, which enqueues without registering. Do the unwatch first, and consider the filter as a follow-up.

**Test to prove it** (`probes/test_core_probes2.py`, fails today with `[(2, '$1')]`):

```python
async def test_new_incarnation_not_told_about_children_of_the_old_one():
    signals: list[tuple[int, str]] = []
    incarnations: list[int] = []

    def factory(ctx: ActorContext[Ctl]) -> Behavior[Ctl]:
        n = len(incarnations) + 1
        incarnations.append(n)
        ctx.watch(ctx.spawn_anonymous(Behaviors.receive_message(child_recv)))

        async def on_msg(ctx: ActorContext[Ctl], msg: Ctl) -> Behavior[Ctl]:
            if isinstance(msg, Fail):
                raise RuntimeError("boom")
            return Behaviors.same()

        async def on_sig(ctx: ActorContext[Ctl], sig: Signal) -> Behavior[Ctl]:
            if isinstance(sig, Terminated):
                signals.append((n, sig.ref.path.name))
            return Behaviors.same()

        return Behaviors.receive(on_msg, msg_type=Ctl, on_signal=on_sig)

    async with ActorSystem("probe", settings=IsolatedTapioSettings()) as system:
        ref = system.spawn(
            Behaviors.supervise(Behaviors.setup(factory)).on_failure(SupervisorStrategy.restart()),
            name="parent",
        )
        ref.tell(Fail())
        await eventually(lambda: len(incarnations) == 2)
        await asyncio.sleep(0.05)
        assert signals == []
```

---

### [CORE-5] A user-chosen name starting with `$` collides with `spawn_anonymous` and orphans a live child past `terminate()`
**Severity:** High
**Category:** Bug
**Status:** Confirmed (probe fails today; reported as a lead by the periphery pass)
**Location:** `src/tapio/actor/path.py:13-16`, `src/tapio/actor/cell.py:649-681`, `855`
**Issue:** #185

**What's wrong.** `_ELEMENT_RE` allows a leading `$`, and its comment says it is "reserved for generated names (spawn_anonymous)", but `spawn` never refuses one. `spawn_anonymous` builds `f"${next(self._anonymous)}"` without checking `_children`, and `_spawn_child` overwrites the map entry:

```python
855        self._children[name] = child
```

So `spawn(b, "$1")` followed by `spawn_anonymous(b)` leaves the first child running and in no map. `_stop_children` and `_finish` only reach children through `_children`, so nothing ever stops it.

**Why it matters.** It breaks AGENTS.md's "every task belongs to a cell and is cancelled in its termination sequence": the probe's `assert_no_leaked_tasks` reports `1 task(s) still running: tapio-cell:tapio://probe/user/$1#1` after `terminate()`. Names like `$1` come from code that rebuilds names from paths it saw (a ref's `path.name` after a restart, a registry keyed by name), not only from typos.

**Proposed fix.** Make the reservation real, and make `spawn_anonymous` safe on its own:

```python
# cell.py, spawn
if name.startswith("$"):
    msg = f"{name!r} is reserved: names starting with '$' are generated by spawn_anonymous"
    raise ActorNameError(msg)

# cell.py, spawn_anonymous
name = f"${next(self._anonymous)}"
while name in self._children:
    name = f"${next(self._anonymous)}"
```

**Test to prove it** (`probes/test_core_probes4.py`, fails today):

```python
async def test_named_dollar_child_is_not_orphaned_by_spawn_anonymous():
    with assert_no_leaked_tasks():
        system = ActorSystem("probe", settings=IsolatedTapioSettings())
        with pytest.raises(ActorNameError):
            system.spawn(Behaviors.receive_message(recv), "$1")
        system.spawn_anonymous(Behaviors.receive_message(recv))
        await system.terminate()
```

---

### [CORE-6] Without `max_restarts`, the backoff exponent never decays and `window` is silently ignored
**Severity:** High
**Category:** Bug
**Status:** Confirmed (probe fails today)
**Location:** `src/tapio/actor/restarts.py:489-495`
**Issue:** #186

**What's wrong.** `RestartLog.record` returns early for an unlimited strategy, before the window is consulted:

```python
489        if strategy.max_restarts is None:
494            self._counts[key] = self._counts.get(key, 0) + 1
495            return True
```

`SupervisorStrategy.restart(window=..., backoff=...)` with no `max_restarts` is accepted (`supervision.py:361-381` checks nothing about the combination). The window is then never read, and the count that drives the backoff exponent only grows.

**Why it matters.** "Restart for ever, with backoff" is the natural configuration for an actor in front of a flaky dependency. After about `log2(max_backoff / min_backoff)` failures over the actor's whole life, every later failure waits `max_backoff`, even when the failures are days apart. With `min=100 ms` and `max=60 s` that is ten failures. Closed #52 fixed this for strategies with `max_restarts`; this is the path it left out. The `window` argument the user passed has no effect at all, with no error. Probe: ten failures one day apart with a 10 s window give `count == 10`, so the eleventh waits the ceiling.

**Proposed fix.** Age the count against the window whenever there is one, and keep the timestamps only when there is a window to age them against:

```python
def record(self, key, strategy, now):
    if strategy.window is None and strategy.max_restarts is None:
        self._counts[key] = self._counts.get(key, 0) + 1
        return True
    times = self._times.setdefault(key, deque())
    if strategy.window is not None:
        horizon = now - strategy.window.total_seconds()
        while times and times[0] < horizon:
            times.popleft()
    times.append(now)
    self._counts[key] = len(times)
    return strategy.max_restarts is None or len(times) <= strategy.max_restarts
```

For the no-window, no-limit case, also consider Pekko's `reset_backoff_after`: a quiet period after which the count returns to zero. Otherwise an unlimited strategy with no window still climbs to the ceiling for ever. Alternative considered: refuse `window` without `max_restarts` in `SupervisorStrategy.__post_init__`. That would remove the silent no-op but not the unbounded climb.

**Test to prove it** (`probes/test_core_probes.py`, fails today with `assert 10 == 1`):

```python
def test_unlimited_restart_backoff_count_ages_out_of_the_window():
    log = RestartLog()
    strategy = SupervisorStrategy.restart(
        window=timedelta(seconds=10),
        backoff=Backoff(min_backoff=timedelta(seconds=1), max_backoff=timedelta(seconds=60)),
    )
    for day in range(10):
        log.record("k", strategy, now=day * 86_400.0)
    assert log.count("k") == 1
```

---

### [WIRE-1] A non-ASCII proof crashes the handshake task and the socket is never closed
**Severity:** High
**Category:** Bug
**Status:** Confirmed (probe fails today)
**Location:** `src/tapio/remote/handshake.py:375-394`, caught set at `src/tapio/remote/endpoint.py:336`
**Issue:** #187

**What's wrong.** `_check_proof` compares two `str` values with `hmac.compare_digest`:

```python
387    if hmac.compare_digest(_proof(secret, nonce), proof):
```

`compare_digest` raises `TypeError("comparing strings with non-ASCII characters is not supported")` when either string holds a non-ASCII character. `proof` is whatever the peer wrote, so `{"proof": "é"}` raises `TypeError` out of `accept`. `RemoteEndpoint._handshake` only catches `(OSError, TapioError, TimeoutError, EOFError)`, so the task ends with the exception and the handle stays in `_held` with the socket open. Nothing closes it: the handshake deadline only bounded the read, which already finished.

**Why it matters.** Any host that can reach the port, holding no secret, leaks one socket per connection with a 70-byte frame, and `handshake_timeout` (whose whole point is to bound this) does not apply. It also breaks shutdown: `close_server` then waits on `server.wait_closed()` for that connection (see WIRE-3), so `terminate()` runs to the full `shutdown_timeout` and the endpoint is cancelled before its `_held` drain and association `detach` sweep run.

**Proposed fix.** Compare bytes, which accept any content, and refuse a malformed proof by shape before comparing:

```python
def _check_proof(secret, nonce, proof, *, who):
    if secret is None:
        return
    if hmac.compare_digest(_proof(secret, nonce).encode(), proof.encode()):
        return
    ...
```

Also, as defence in depth in `endpoint._handshake`, end with `except Exception` that logs at warning and calls `self._let_go(handle)`: a pre-auth peer must never be able to pick which exception escapes. I rejected only widening the endpoint's catch: the handshake module would still lie about its `Raises:` section.

**Test to prove it** (`test_wire_probes.py::test_non_ascii_proof_is_refused_and_closed`, fails today with `AssertionError: server left the socket open`; the diagnostic run showed the task `exception=TypeError('comparing strings with non-ASCII characters is not supported')`):

```python
async def test_non_ascii_proof_is_refused_and_closed(guarded):  # secret="shh", handshake_timeout=0.3s
    link = await dial(guarded, proof="é", welcome=False)
    try:
        assert await closed_within(link, 2.0), "server left the socket open"
    finally:
        await link.close()
```

`test_wire_shutdown.py::test_terminate_after_a_crashed_handshake_closes_the_socket` fails with `terminate took 10.01s`.

---

### [WIRE-2] Deeply nested JSON raises `RecursionError` past every handler, before and after the handshake
**Severity:** High
**Category:** Bug
**Status:** Confirmed (three probes fail today)
**Location:** `src/tapio/remote/transport.py:199-202` (`link_body`), `src/tapio/remote/codec.py:228-231` (`decode`)
**Issue:** #188
**Also found by:** the link pass, as its LINK-6 (the pre-handshake half, with the same probe result). Merged here.

**What's wrong.** Both parsers catch only `ValueError`:

```python
199    try:
200        parsed = json.loads(frame[LENGTH_PREFIX:])
201    except ValueError as error:
```

`json.loads(b'{"link":' + b'[' * 200_000 + ...)` raises `RecursionError`, which is a `RuntimeError`. 400 KB is well under the 4 MB cap.

- Before the handshake, `read_link` raises it out of `accept`, with the same consequence as WIRE-1: leaked socket, and `terminate` blocked to the deadline. This path needs no secret at all.
- After the handshake, `receive_frame` (and `ActorSystem.deliver_frame`, both documented "it never raises") raises it out of `Association._read`. `_run` does not catch it, so the association's reader task dies while the association stays registered. Every later frame on that link sits unread in the kernel buffer, with no dead letter, until the failure detector calls the peer unreachable 10 seconds later. That is silent message loss.

**Why it matters.** It breaks the documented "never raises" contract of the trust boundary. It also breaks "every task belongs to a cell": a task ends with an exception nobody observes until shutdown.

**Proposed fix.** Catch it where JSON is parsed, in both places:

```python
    except (ValueError, RecursionError) as error:
        raise MessageDecodingError(f"frame body is not JSON: {error}") from error
```

Alternative: parse with `pydantic_core.from_json`, which has a fixed depth limit and raises `ValueError`. That is a sound choice too, but it changes the parser, and the one-line catch is enough.

**Test to prove it** (all fail today):

```python
async def test_receive_frame_never_raises_on_deep_nesting():
    system = ActorSystem("alpha", remoting())
    try:
        body = b'{"v":1,"to":"/user/x","t":"k","p":' + b"[" * 200_000 + b"]" * 200_000 + b"}"
        system.deliver_frame(framed(body))   # RecursionError today
    finally:
        await system.terminate()
```

`test_deeply_nested_hello_is_refused_and_closed` fails with `server left the socket open`. `test_deep_frame_does_not_kill_the_link` (an authenticated peer sends one deep frame and then a valid `Tick`) fails with `condition never held within 2.0s`.

---

### [WIRE-3] `close_server` waits for every accepted connection, so a peer mid-handshake holds `terminate`
**Severity:** High
**Category:** Bug
**Status:** Confirmed (probes fail today)
**Location:** `src/tapio/remote/transport.py:438-476`, called from `endpoint.py:738` before the `_held` drain
**Issue:** #189

**What's wrong.**

```python
473        server.close()
475    with contextlib.suppress(OSError, asyncio.CancelledError):
476        await server.wait_closed()
```

Since Python 3.12, `Server.wait_closed()` returns only when every connection the server accepted has closed. The endpoint closes its held handshake sockets after `close_server` returns (the `while self._held` loop). So a connection that is still handshaking keeps `wait_closed` waiting until that connection gives up on its own:

- a TCP peer that read the server-hello and said nothing: `terminate` waits `handshake_timeout` (probe: 3.00 s with a 3 s deadline);
- a peer that never starts TLS: asyncio's own 60 s TLS handshake timeout applies, so `terminate` hits `shutdown_timeout` (10 s), the endpoint cell is cancelled, and the drain and association sweep after `close_server` never run;
- a crashed handshake (WIRE-1, WIRE-2): forever, so it is always cancelled at the deadline.

**Why it matters.** "Shutdown races one deadline for the whole tree" assumes each step finishes. Here one unauthenticated connection decides how long shutdown takes, and when it takes the whole deadline, the cleanup the endpoint documents ("or the socket is left for the garbage collector") is skipped.

**Proposed fix.** Make `close_server` stop accepting only, without `await server.wait_closed()`. The endpoint then awaits `wait_closed()` itself, after the `_held` drain and the association detach, when every connection it owns has been closed. On 3.13 and later, call `server.close_clients()` there too, for connections still in their TLS handshake, which the endpoint has never seen. I rejected bounding `wait_closed` with a timeout: it hides the ordering bug behind a magic number.

**Test to prove it** (`test_wire_shutdown.py`, both fail today):

```python
async def test_terminate_is_not_held_by_a_peer_mid_handshake():
    system = ActorSystem("beta", remoting(handshake_timeout=timedelta(seconds=3)))
    link = await connect("127.0.0.1", system.address.port, max_frame_bytes=1024, ssl_context=None)
    try:
        await link.read_link(2.0)
        started = time.monotonic()
        await system.terminate()
        took = time.monotonic() - started
    finally:
        await link.close()
    assert took < 1.0   # AssertionError: terminate waited 3.00s for a peer that said nothing
```

`test_terminate_is_not_held_by_a_peer_that_never_starts_tls` fails with `terminate waited 10.01s`.

---

### [WIRE-4] `FrameLink.close` waits forever on a peer that stopped reading, and swallows the caller's cancellation
**Severity:** High
**Category:** Bug
**Status:** Confirmed (probes fail today)
**Location:** `src/tapio/remote/transport.py:323-332`
**Issue:** #190

**What's wrong.**

```python
330        self._writer.close()
331        with contextlib.suppress(OSError, asyncio.CancelledError):
332            await self._writer.wait_closed()
```

`StreamWriter.close()` flushes the write buffer before it closes the socket. With a peer that stopped reading, the buffer never drains, so `wait_closed()` never returns and the file descriptor stays open. When the caller is cancelled while it waits (the shutdown deadline, an `asyncio.timeout`), the `CancelledError` is suppressed, so the caller resumes as though the close had worked. The probe wraps `close()` in `asyncio.timeout(1.0)`: the call *returns* after 1.00 s, with no `TimeoutError`, and the socket is still open.

**Why it matters.** A link is closed exactly when something has gone wrong, and a stalled peer is the common case of that. `LinkHandle.close` and every waiter shielded on it inherit the hang. End to end, `terminate()` with one peer that stopped reading takes the full `shutdown_timeout` (probe: 10.01 s) and the association, endpoint and system cells are all cancelled. Swallowing `CancelledError` also contradicts `LinkHandle.close`'s own rule that a caller cancelled while closing "still gets it".

**Proposed fix.** Give the flush a short grace, then abort. Never swallow a cancellation:

```python
_CLOSE_GRACE: Final = 1.0

async def close(self) -> None:
    self._writer.close()
    try:
        async with asyncio.timeout(_CLOSE_GRACE):
            await self._writer.wait_closed()
    except (TimeoutError, OSError):
        self._writer.transport.abort()
    except asyncio.CancelledError:
        self._writer.transport.abort()
        raise
```

Losing unflushed frames is consistent with at-most-once. I rejected a plain `abort()` with no grace: it would drop the final frames of every ordinary close, including a heartbeat or watch reply just written.

**Test to prove it** (`test_wire_shutdown.py::test_close_of_a_link_whose_peer_stopped_reading_finishes`, fails today with `close returned after 1.00s, socket still open: True`; `test_terminate_with_a_peer_that_stopped_reading` fails with `terminate took 10.01s`). The association's own `drain()` in `write_frame` probably adds to the system-level symptom. That part belongs to the association reviewer.

---

### [WIRE-5] IPv6 remoting cannot work: bracketed hosts cannot be dialled, unbracketed ones cannot be written down
**Severity:** High
**Category:** Bug
**Status:** Confirmed (unit probes fail today; this sandbox has no IPv6, so no two-node probe)
**Location:** `src/tapio/remote/transport.py:369`, `src/tapio/remote/address.py:65-79`, `src/tapio/actor/system.py:89`
**Issue:** #191

**What's wrong.** `bind()` and `is_loopback()` both accept `"::1"` and `"[::1]"`. The canonical host defaults to `bind_host` unchanged.

- `bind_host="::1"` gives `Address(host="::1")`, whose string `tapio://s@::1:25520` does not match `_ADDRESS_RE`. Every ref the system writes is unparseable, and every peer refuses its client-hello in `_identify` ("not an address").
- `bind_host="[::1]"` (or `canonical_host="[::1]"`) round-trips as a string, the case `test_an_ipv6_literal_survives_the_round_trip` covers. But `connect()` passes `"[::1]"` to `asyncio.open_connection`, and `getaddrinfo` fails with `gaierror(-2, 'Name or service not known')`. Nothing can dial it, and with TLS `server_hostname="[::1]"` would not match either.

`Address.__post_init__` validates the system and port but never the host, so `""` (the every-interface bind) also produces an address that cannot be parsed back.

**Why it matters.** The regex comment and the IPv6-aware `bind` advertise IPv6 support. A deployment that uses it starts cleanly and then fails every link. The error messages point at the peer ("advertised ..., which is not an address") rather than at the configuration.

**Proposed fix.** Use one spelling everywhere, bracketed in the address and bare at the socket:

```python
# address.py, Address.__post_init__
if self.host is not None and (not self.host or (":" in self.host and not self.host.startswith("["))):
    raise ValueError(f"invalid host {self.host!r}: write an IPv6 literal in brackets")

# transport.py, connect
reader, writer = await asyncio.open_connection(
    host.removeprefix("[").removesuffix("]"), port, ssl=ssl_context
)

# system.py, where the canonical address is derived: bracket a bare IPv6 bind_host.
```

I rejected loosening `_HOST` to accept bare `::1`: the port is then ambiguous.

**Test to prove it** (both fail today):

```python
def test_unbracketed_ipv6_canonical_host_round_trips():
    address = Address(system="s", host="::1", port=25520)
    assert Address.parse(str(address)) == address    # ValueError: not an actor system address

async def test_bracketed_ipv6_host_is_dialable():
    with pytest.raises(OSError) as caught:
        await connect("[::1]", 9, max_frame_bytes=1024, ssl_context=None)
    assert not isinstance(caught.value, socket.gaierror), caught.value   # gaierror today
```

After the fix, the first test should instead expect `Address(host="::1")` to raise.

---

### [LINK-4] Link frames dropped under load break death watch: a watcher can wait forever
**Severity:** High
**Category:** Bug
**Status:** Confirmed (the `Watch` shed path); Suspected by reading (the `WatcheeTerminated` and `Unwatch` drop paths)
**Location:** `src/tapio/remote/association.py:759-782`, `417-448`, `606-631`, `1054-1064`
**Issue:** #192

**What's wrong** `watch()` guards one way a `Watch` frame can be lost: `_write_link` failing because the mailbox is full. It does not guard the other way. While `_link is None` (dialling, or after `adopt`), the actor moves frames from the mailbox into `_pending`, and `_hold` sheds anything past `outbound_capacity`:

```python
772        if isinstance(outbound, LinkOut):
773            _log.warning("dropped a %s frame for %s: %s", outbound.kind, self._peer, waiting)
774            return
```

The local `_watching_there` entry stays, the peer never registers the watch, and no `Terminated` comes while the association lives. The same applies in the other direction. `report_terminated` (line 1062) ignores the result of `_write_link`, so a full outbound buffer drops the `WatcheeTerminated` that a watcher on the peer is waiting for. A dropped `Unwatch` leaves a `_PeerWatcher` on a local cell until the link ends.

**Why it matters** This is the failure the comment at lines 439-443 says death watch exists to prevent. It happens whenever more than `outbound_capacity` frames are queued while the link comes up, or when the buffer is full at the moment a watched actor dies.

**Proposed fix** Treat a watch-protocol frame that cannot be queued as a broken link. Watches that cannot be kept consistent end the association, and `_end_watches` then tells every watcher on both sides:

```python
def _shed(self, outbound):
    if isinstance(outbound, LinkOut):
        self.close(f"a {outbound.kind} frame could not be queued: {waiting}")
        return
    ...
# and in report_terminated / unwatch:
if not self._write_link(frame):
    self.close("a watch frame could not be queued")
```

Alternative: give link frames a lane of their own that is never shed, such as an unbounded deque the writer drains ahead of `_pending`. I rejected it because a watch frame then overtakes the user messages sent before it, and the `LinkOut` docstring says watches must keep their place in that order.

**Test to prove it** Ran as `probes/remote-link/test_watch_shed.py`. With `outbound_capacity=4` and the acceptor's handshake delayed by 300 ms, alpha tells 8 ticks and then starts a watcher. Once the link is up, the watched actor stops. Output on `main`: the log shows `dropped a Watch frame for tapio://beta@...: 4 frames are already waiting`, and `eventually(... "terminated" ...)` fails: `condition never held within 2s`.

---

### [LINK-5] One inbound frame that raises outside the expected list kills the reader and ends in a false quarantine
**Severity:** High
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/remote/association.py:846-878`, `976-1011`; trigger in `src/tapio/remote/codec.py:327-341`
**Issue:** #193
**Related:** the `RecursionError` trigger is WIRE-2.

**What's wrong** `_run` catches `CancelledError`, `FrameTooLargeError`, `MessageDecodingError`, `EOFError`, `OSError`, `TapioError` and `TimeoutError`. Anything else that escapes `_read` ends the reader task silently. That covers a user model whose validator raises `TypeError` or `KeyError` during `receive_frame` (which only catches `ValidationError` and `RefResolutionError`, despite "it never raises"). It also covers a `RecursionError` from `json.loads` on a deeply nested link frame in `_on_link_frame` (which catches `ValidationError`, `ValueError` and `MessageDecodingError`). Once the reader is dead, the association still has a link and keeps writing, but nothing reads. The detector hears silence, and after `unreachable_after` the peer is quarantined and every watcher is told `Terminated`. Every frame the peer sent after the bad one is lost.

**Why it matters** One malformed or unlucky message from an authenticated peer turns into a false quarantine of that whole node. Recovery then needs a manual `reconnect`.

**Proposed fix** Give the reader a last-resort handler that ends the link visibly, and make the frame-level code catch broadly too (the codec half is WIRE-2):

```python
        except (OSError, TapioError, TimeoutError) as error:
            ...
        except Exception as error:  # a reader that dies silently becomes a false verdict
            _log.exception("reading the link to %s failed", self._peer)
            self.close(f"reading the link failed: {type(error).__name__}: {error}")
```

In `_on_link_frame`, add `RecursionError` to the ignored set. In `receive_frame`, dead-letter any `Exception` from `model_validate_json` as `MALFORMED_FRAME`. I rejected catching per frame in `_read` alone, because an exception there that is not about one frame (a bug in `deliver`) should still end the link rather than loop.

**Test to prove it** Ran as `probes/remote-link/test_reader_death.py`. A registered `Fussy` message has a validator that raises `TypeError` for `n == 1` on the receiving side. Alpha tells `n=1` and then `n=2`. Output on `main`: `condition never held within 1s` (tick 2 never arrives), and `beta quarantined: (Address('tapio://alpha@...'),)`.

---

### [LINK-7] `offer` does not wait while a link is coming up: it dead-letters as buffer-full
**Severity:** High
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/remote/association.py:398-415`, `688-701`, `759-782`; contradicts `docs/remoting.md:71-78` and `RemoteRef.offer` (`ref.py:201-206`)
**Issue:** #194

**What's wrong** `offer` waits only for room in the actor's mailbox. While `_link is None`, the actor empties the mailbox into `_pending` straight away, and `_hold` sheds past `outbound_capacity` with `OUTBOUND_BUFFER_FULL`. The mailbox therefore never stays full, `offer` never waits, and the frames it queued are dead-lettered. The same happens in every `adopt` window. A side effect is that the real bound during a dial is twice `outbound_capacity`, split across two buffers.

**Why it matters** A producer that uses `offer` precisely to avoid losing messages loses them on every reconnect. The docs say `offer` "waits for room in this node's outbound buffer".

**Proposed fix** Make the mailbox the only buffer. When there is no link, `_write` waits for one rather than moving the frame into `_pending`. An `asyncio.Event` set in `_open` and cleared in `adopt` does it, with `close()` setting it too so a closing association wakes up and stops. `_pending` and `_shed` disappear, `offer` then waits on the bounded mailbox, and `tell` overflows to `OUTBOUND_BUFFER_FULL` in `send` exactly as it does with a link up. The cost is that `Beat` is not judged while a dial is in flight. That is acceptable, because the dial has its own deadline (`handshake_timeout`). I rejected keeping `_pending` and making `offer` also wait on it, which means two waiter queues with one consumer between them.

**Test to prove it** Ran as `probes/remote-link/test_offer_during_dial.py`. A server accepts TCP and never sends a hello, and `outbound_capacity=4`. Twenty `await target.offer(...)` calls run, and the test asserts that none produced `OUTBOUND_BUFFER_FULL`. Output on `main`: `AssertionError: (20, 16, '4 frames are already waiting for a link to tapio://slow@...')`. All 20 offers returned at once and 16 were dead-lettered.

---

### [LINK-8] A ref from before a peer restarted reaches the new incarnation's actor
**Severity:** High
**Category:** Bug
**Also touches:** Bug (and Docs)
**Status:** Confirmed
**Location:** `src/tapio/actor/cell.py:218-225` (`next_uid` counts from 1), `src/tapio/remote/endpoint.py:549-563`, `docs/unreachable.md:91-95`, `endpoint.py:667-668`
**Issue:** #195

**What's wrong** The docs say: "Refs held across a quarantine are not reusable. Their uid belongs to a session that is over ... a restarted peer [is] a different peer rather than an impostor at the same address: a system mints a new uid per incarnation, and an association is bound to the uid it handshook with." In the code:
- A `RemoteRef` is bound to the peer's address through `PeerOutbox` and carries only the actor's path uid. The system uid is not part of it.
- Actor uids come from a per-system counter that starts at 1. `watch.py`'s own docstring notes that the same deployment hands out the same paths, "uids included".
- A new association accepts whatever uid the dial's handshake returns, and `peer_uid` is compared only when an inbound link meets an existing association.

So after the peer restarts, an old ref's frame is addressed to `/user/worker#2`, which is the new incarnation's `worker`.

**Why it matters** The uid guarantee ("stops a ref to a dead actor from addressing a new actor spawned under the same name") does not hold across a restart. The docs also say the opposite of what happens after an ordinary reconnect to the same incarnation: old refs work there, which is fine but undocumented.

**Proposed fix** Mint actor uids at random (`secrets.randbits(63)` or similar), which is what Pekko does. A ref into a previous incarnation then names a uid that nothing holds, and the frame dead-letters on the receiving side. Then rewrite `unreachable.md:91-95`:

> Refs held across a quarantine keep working after `reconnect` if the peer is the same incarnation, because a ref names a node and an actor, not a link. A peer that restarted mints new actor uids, so a ref from before the restart reaches nothing and its messages dead-letter on the peer. Resolve again to reach the new incarnation.

I rejected adding the system uid to each frame: it changes the wire format to get something random actor uids give for free.

**Test to prove it** Ran as `probes/remote-link/test_restarted_peer.py`. Beta is terminated and restarted on the same port with the same spawn order, and alpha tells through the ref it held from before. Output on `main`: `assert [2] == []`. The new incarnation's `worker` received the message.

---

### [LINK-9] A dialler never checks which system answered, so frames are delivered to the wrong system
**Severity:** High
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/remote/association.py:921-933`
**Issue:** #196

**What's wrong** `_dial` handshakes and then uses only `identity.uid`. `identity.address` is logged and otherwise ignored. A ref to `tapio://gamma@host:p` with beta listening on `host:p` opens an association keyed by gamma, and every frame on it is delivered by beta to beta's actors. The frame's `to` carries no system name, and `receive_frame` resolves it in the receiving system.

**Why it matters** A stale configuration, a reused port or a rescheduled container sends messages to an unrelated system with no error and no dead letter. The handshake establishes the canonical address precisely so that this can be checked.

**Proposed fix** In `_dial`, after `introduce`:

```python
if identity.address != self._peer:
    await link.close()
    msg = f"dialled {self._peer} and {identity.address} answered"
    raise HandshakeError(msg)
```

Comparing the system name alone would be a smaller check. I rejected it because an address mismatch with the same name (two nodes of one deployment behind a swapped port) is the likelier case.

**Test to prove it** Ran as `probes/remote-link/test_wrong_system.py`. It resolves `tapio://gamma@127.0.0.1:<beta's port>/user/worker#<uid>` and tells it a tick. Output on `main`: `a frame addressed to gamma was delivered to beta`, `assert [1] == []`.

---

### [MEMB-1] Downing strategies count members the two sides can disagree about, so both sides of a split can win
**Severity:** High
**Category:** Bug
**Status:** Confirmed (pure functions, by probe)
**Location:** `src/tapio/cluster/downing.py:93-108` (`_sides`), `:203-215` (KeepMajority), `:325-340` (KeepOldest)
**Issue:** #197

**What's wrong.** The safety argument (`downing.py:11-20`, `docs/clustering.md:204-215`) is that the two sides feed the strategy mirror-image views. `_sides` splits `state.alive`, which is every member that is not Down or Removed:

```python
105    gone = state.unreachable
106    reachable = tuple(m for m in state.alive if m.address not in gone)
107    unreachable = tuple(m for m in state.alive if m.address in gone)
```

Membership itself is not guaranteed to match across a split. Three ordinary transitions are made on one node and travel by gossip, so they can be on one side only when the split happens:
1. **A join.** `_admit` (`daemon.py:833-876`) is run by any member, not only the leader. It adds the joiner as `Joining` and bumps the version. If the split comes before that version crosses, only the admitting side counts the joiner.
2. **Exiting to Removed.** The leader makes this move. The other side still has the member as `Exiting`, which is alive and counted. Its process is still running, because "Leaving does not terminate the system" (docs:133).
3. **A down by an operator** on one node.

KeepMajority compares the counts and breaks a tie on the lowest address. KeepOldest picks the oldest from the counted set. When the counted sets differ, each side can name the other as the loser.

**Why it matters.** The result is split brain: two live sides, two singletons, two group-router pools. The probes build the exact views:
- `test_keep_majority_both_sides_win_when_a_joiner_is_known_on_one_side`. Side one is {N2, N3} plus J (Joining). It sees 3 against 2 and downs side two. Side two is {N0, N1}. It sees 2 against 2, its own N0 is the lowest address, and it downs side one. Neither side downs itself.
- `test_keep_majority_both_sides_win_when_a_removal_crossed_only_one_side`. The same outcome comes from a leaving member removed on one side only.
- `test_keep_oldest_both_sides_win_when_the_oldest_was_removed_on_one_side`. The oldest member is leaving. Side one still has it Exiting, so it keeps itself. Side two has it Removed, so its own P becomes the oldest and it keeps itself too.

The join case is the most likely in practice. A node retries `Join` to every seed every `join_retry_interval`, so a joiner admitted at the moment of a split is ordinary. Akka's split brain resolver excludes Joining and WeaklyUp members from every count for exactly this reason.

**Proposed fix.** Count only members whose presence in the count cannot differ between the sides. Exclude `Joining`, because admission is not leader-gated. Exclude `Exiting`, because only the leader moves it on to Removed. Use the same rule for the tie-break address and for the oldest candidate:

```python
_COUNTED = frozenset({MemberStatus.UP, MemberStatus.LEAVING})

def _counted(members, role):
    voting = (m for m in members if m.status in _COUNTED)
    return tuple(voting) if role is None else tuple(m for m in voting if role in m.roles)
```

The whole side is still downed, Joining and Exiting members included. They only stop voting. An operator Down on one side only cannot be fixed inside the strategy. Document it: "an operator down issued during a split can make the views differ".

Rejected alternative: count Joining members on both sides by also placing them on the unreachable side. The other side does not know they exist, so nothing can make the views agree.

**Test to prove it** (`probes/cluster-core/test_downing_views.py`; it fails after the fix):
```python
async def test_a_joiner_known_on_one_side_does_not_let_both_sides_win():
    a, b, c, d, j = N[2], N[3], N[0], N[1], N[4]
    core = [m(c, up=1), m(d, up=2), m(a, up=3), m(b, up=4)]
    side_one = view([*core, m(j, MemberStatus.JOINING, up=0)], [(a, c), (b, d)])
    side_two = view(core, [(c, a), (d, b)])
    v1 = await KeepMajority().decide(side_one)
    v2 = await KeepMajority().decide(side_two)
    assert not (survivors(v1, [a, b, j]) and survivors(v2, [c, d]))
```

---

### [MEMB-2] A restarted node brings back its downed predecessor's unreachability claims
**Severity:** High
**Category:** Bug
**Status:** Confirmed for the state (probe). The daemon consequence (a healthy member downed) is Suspected.
**Location:** `src/tapio/cluster/gossip.py:141-159`, `src/tapio/cluster/reachability.py:55-73`
**Issue:** #198

**What's wrong.** Claims made by a downed observer are meant to "stop counting, because the observer that made it is gone" (`gossip.py:144-148`). The filter works on addresses:

```python
149        return frozenset(m.address for m in self.alive)
```

A `ReachabilityRecord` names its observer by address only, with no uid. Consider a node X that recorded peers unreachable and was then downed. This is the normal case: a node cut off alone records the transport verdict for every peer it had a link to (`daemon.py:616-622`). When X restarts at the same address and is admitted as `Joining`, its address is alive again. Every claim the old incarnation made counts again, cluster-wide.

**Why it matters.** The new incarnation retracts a claim only when it judges that peer itself. For a peer on its ring, that happens at the next probe. For a peer off its ring, it happens only when it opens a link to that peer (`PeerReachable`, `association.py:964`), and gossip targets are chosen at random. Until then:
- every node sees the peer unreachable,
- convergence is blocked, so the joiner itself cannot be promoted,
- if the set stays the same for `down_after` (7 s by default), a strategy downs a healthy member. With KeepMajority, a lone "unreachable" minority is downed.

A stable address across restarts is the usual deployment (a StatefulSet, or a fixed host and port), and a node downed after a partition and then restarted is the usual story. Probe: `test_a_restart_revives_the_old_incarnations_claim` fails with `frozenset({'tapio://n@127.0.0.1:2552'})`.

**Proposed fix.** Minimal fix, with no wire change: a node takes back every claim held under its address that it has not made itself. In the daemon, once the node sees itself in a merged state for the first time:

```python
for record in self._state.reachability.records:
    if (record.observer == self._address
            and record.status is ReachabilityStatus.UNREACHABLE
            and record.observed not in self._monitor.peers):
        self._state = self._state.observing(self._address, record.observed,
                                            ReachabilityStatus.REACHABLE)
self._state = self._state.bumped_by(self._address)
```

The retraction is honest, because the claims belong to an incarnation that no longer exists. Each one is moved to `version + 1`, so it wins every merge.

Rejected alternative: add `observer_uid` to `ReachabilityRecord` and make `_live_observers` return `(address, uid)` keys. It is cleaner, but it changes the frame, so it raises `PROTOCOL_VERSION`, and that is a deployment event.

**Test to prove it** (fails today, passes once the claims are filtered or retracted):
```python
def test_a_restart_does_not_revive_the_old_incarnations_claim():
    x, y = N[0], N[1]
    downed = view([m(x, MemberStatus.DOWN, uid=1), m(y, up=2)], [(x, y)])
    rejoined = downed.with_member(m(x, MemberStatus.JOINING, up=0, uid=2))
    assert rejoined.unreachable == frozenset()
```
The daemon-level version starts 3 nodes plus 4 more with `monitored_peers=1` and cuts X off so it records transport verdicts. It then downs X, restarts it at the same port, and asserts that no other member reaches `Down` within `down_after * 2`.

---

### [MEMB-3] Reachability records are never pruned, so one healed split in a ~300-node cluster makes gossip too large to send
**Severity:** High
**Category:** Bug
**Status:** Confirmed (frame refused, by probe). The assumption is that most pairs across the split had a link, which random gossip produces within minutes.
**Location:** `src/tapio/cluster/reachability.py:26-32`, `src/tapio/cluster/gossip.py:338-344`, `src/tapio/cluster/daemon.py:616-622`
**Issue:** #199

**What's wrong.** Each pair (observer, observed) ever written stays forever. That is deliberate, because deletion is not a join. The transport path writes one record per far member a node had a link to (`_link_changed`, for any alive member, not only ring peers). A half-and-half split therefore leaves about (n/2)^2 records per side, and n^2/2 once the views merge. Members' tombstones and vector-clock entries are not pruned either.

**Why it matters.** `test_scale.py` measures this:

| n | records | gossip JSON |
|---|---|---|
| 500, ring only | 2,500 | 330 KB |
| 200, after a healed split | 20,970 | 2.3 MB |
| 500, after a healed split | 127,470 | 14 MB |

`max_frame_bytes` defaults to 4 MiB. `test_a_healed_split_at_300_nodes_no_longer_fits_in_a_frame` shows that `encode` raises `FrameTooLargeError` for a 300-node state. After that, no `GossipEnvelope` from this node can be sent. Convergence never happens again, and no later change can shrink the state.

With no downing strategy, which is the default, a 10 s blip across the middle of a 300-node cluster is enough. With a strategy, the losing side is downed, but the survivors keep their own (n/2)^2 claims, so the threshold is about n = 390.

**Proposed fix.** Retract a record's content without deleting the key: when the observed member is Removed, write REACHABLE at `version + 1`. Then shrink the encoding by dropping REACHABLE records whose observer or observed member is Removed. That is safe under the join only if every node drops them, which needs the same "everyone has seen the removal" knowledge that tombstone pruning needs (docs:128-131).

Pragmatic first step: stop the transport path from writing a record for every linked far member. Record only that the link is lost, and let the probe ring plus one aggregated per-node flag carry it. Alternatively, cap the transport records at `monitored_peers` per observer, chosen by ring distance.

Rejected alternative: raise `max_frame_bytes`. The merge, validate and leader cost grows with it (MEMB-4), so the node stalls instead of going silent.

**Test to prove it.** `probes/cluster-core/test_scale.py::test_a_healed_split_at_300_nodes_no_longer_fits_in_a_frame` passes today, meaning the frame is refused. Inverted, it is the regression test.

---

### [CLUS-2] A singleton host that restarts at the same address is never replaced: no node runs the singleton again
**Severity:** High
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/cluster/daemon.py:1082-1123` (`_digest`, `_emit`), `src/tapio/cluster/singleton.py:150-152, 194-204`
**Issue:** #200

**What's wrong.** Events are diffed over `Gossip.primaries()`, which keeps one member per address and prefers the live incarnation. When a member restarts at the same address, `_admit` marks the old incarnation `Down` and adds the new one as `Joining`. From then on the primary at that address is the new incarnation. The old one's later move to `Removed` is never in the digest, so `MemberRemoved(old)` is never emitted. The singleton manager keys `_hosts` by `member.key` (address and uid), so the old incarnation stays in `_hosts` for ever. If it was the oldest, every surviving manager computes it as the host. Its address is the restarted node's address, so none of the survivors starts the instance. The restarted node's manager is fresh, and its replay covers only `alive` members, so it computes a different oldest member and does not start either.

**Why it matters.** The usual deployment is the first seed as the oldest member and the singleton host, with a fixed address (a StatefulSet pod, a systemd unit on a fixed port). After one quick restart the singleton is gone for good. Nothing logs it, and every message to it dead-letters. The probe shows the converged view (`old #448 removed up=1`, `new #632 up up=4`, plus two other up members) and `probe.running == set()`.

**Proposed fix.** Fix the event source so every subscriber benefits, not only the singleton. Emit `MemberRemoved` per incarnation:

```python
# in _emit, before the per-address loop
removed_before = {m.key for m in before_members_all if m.status is MemberStatus.REMOVED}
for member in self._state.members:
    if member.status is MemberStatus.REMOVED and member.key not in removed_before:
        if self._digest_primary_differs(member):   # the address now shows a newer incarnation
            self._deliver(ctx, MemberRemoved(member=member))
```

That means the digest also has to hold the set of removed keys. The cheaper and sufficient fix in the singleton is to key `_hosts` by address, so `MemberUp(new)` replaces the old incarnation:

```python
self._hosts: dict[str, Member] = {}
...
case MemberUp(): self._hosts[message.member.address] = message.member
case MemberLeaving() | MemberRemoved():
    held = self._hosts.get(message.member.address)
    if held is not None and held.uid == message.member.uid:
        del self._hosts[message.member.address]
```

Rejected alternative: emitting a `MemberRemoved` for the old incarnation at admit time. It is not removed yet, and a subscriber that reads `Removed` as "gone and agreed" would act early.

**Test to prove it.** Probe `test_a_singleton_survives_its_host_restarting_at_the_same_address`: join 3, place the singleton, terminate node1 (the host), start a replacement on the same port that joins via `[node2, node3]`, wait for convergence, then `eventually(len(probe.running) == 1)`. It fails today with `condition never held within 5.0s`.

---

### [CLUS-3] A manager that subscribes after the cluster formed runs a second instance while it learns who is oldest
**Severity:** High
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/cluster/daemon.py:1288-1294` (`_replay`), `src/tapio/cluster/singleton.py:205, 208-222`
**Issue:** #201

**What's wrong.** The replay sends one `MemberUp` per up member in `self._state.alive` order, which is address order. The manager handles each one as its own turn and calls `_reconcile` after each. If this node's address sorts before the oldest member's, the first `MemberUp` it handles is its own. At that moment it is the only host it knows, so it spawns the keeper. One message later it hears the real oldest and hands off.

**Why it matters.** The instance's `setup` runs on a second node while the real one is running. For "a scheduler that must not fire twice" (the docs' own example) that is the exact failure the singleton exists to prevent. The repo's own tests hide this, because systems are named `node1..3`, so the oldest (node1) always sorts first. It triggers whenever the oldest member's address does not sort first: the first seed is `10.0.0.3`, or (as in CLUS-2) the first seed restarted and now has a higher `up_number` than members whose addresses sort after it. It also triggers on any manager spawned after `join_seed_nodes`, which is the pattern in `examples/tapio_examples/cluster_singleton.py`, and on any manager restarted by supervision.

**Proposed fix.** Replay in seniority order, so a manager always learns the oldest first:

```python
replay.extend(
    MemberUp(member=member)
    for member in sorted(self._state.alive, key=seniority)
    if member.status is MemberStatus.UP
)
```

Rejected alternative: having the manager defer `_reconcile` until "the replay is over". The daemon sends no end-of-replay marker. Adding one (a `CurrentClusterState` snapshot event, as Pekko does) is the more robust long-term shape, but it changes the public event set.

**Test to prove it.** Probe `test_a_late_manager_never_runs_a_second_instance_while_learning_the_oldest`: seed list `[node3, node1, node2]`, so node3 is the oldest and sorts last. Managers are spawned after the join. It fails today:
```
E   AssertionError: ['tapio://node1@127.0.0.1:37429', 'tapio://node3@127.0.0.1:36267']
E   assert 2 == 1   (probe.max_seen)
```

---

### [CLUS-4] A leave asked for on another node starts the successor before the host lets go; the docs promise it cannot
**Severity:** High
**Category:** Bug
**Status:** Confirmed (operator leave). Suspected (slow instance stop).
**Location:** `src/tapio/cluster/singleton.py:197-202, 215-221`, `docs/clustering.md:124-126, 337-343`, `singleton.py:16-28`, `events.py:60-66`
**Issue:** #202

**What's wrong.** Both the successor and the predecessor release on `MemberLeaving`. The design relies on the leaving host hearing `MemberLeaving` first because "the leaving host drives its own transition". That is only true for `Cluster.leave()` called on the host itself. A `Leave` handled anywhere else, either `POST /leave` on the management port of another node (the documented operator path) or a `Leave` frame from a peer (CLUS-10), marks the host `Leaving` on that node first. If that node is the next oldest, its own manager drops the host from `_hosts` and spawns the instance in the same turn. The real host learns one gossip round later.

Separately, the handoff is `self._keeper.tell(_Handoff()); self._keeper = None`. That only asks the keeper to stop. Nothing waits for the instance to finish its current message and its `PostStop`. Even in the self-leave case, an instance busy for longer than a gossip round overlaps with the successor.

**Why it matters.** `docs/clustering.md` says "a graceful leave never runs two instances at once". The singleton module docstring and the `MemberLeaving` docstring say the same. An operator following the `tapio-cluster leave <host>` instructions pointed at any other node gets two instances.

**Proposed fix.** The successor should start only once the leaving host is removed, while the leaving host itself still releases on `MemberLeaving`. Removal requires a converged view of `exiting`, which the host itself must have seen, so by then the host has issued its handoff:

```python
case MemberLeaving():
    self._leaving.add(message.member.key)      # still blocks successors
case MemberRemoved():
    self._leaving.discard(message.member.key)
    self._hosts.pop(message.member.key, None)

def _reconcile(self, ctx):
    host = self._oldest()                       # leaving members still count as oldest
    am_host = host is not None and host.address == self._address and host.key not in self._leaving
```

Rejected alternative: a Pekko-style hand-over handshake (`HandOverToMe` / `HandOverDone`). It is the only design that also covers the slow-stop half, so it is the long-term answer. It needs a wire protocol between managers, which is a larger change than this finding.

Docs: replace "so a graceful leave never runs two instances at once" with "so a leave asked for on the host itself does not normally overlap. A leave asked for on another node, or an instance slow to stop, can overlap for a gossip round."

**Test to prove it.** Probe `test_an_operator_leave_from_another_node_does_not_overlap_two_instances` (`POST /leave` for the host, sent to the next-oldest node). It fails today:
```
E   AssertionError: ['tapio://node1@127.0.0.1:38915', 'tapio://node2@127.0.0.1:33269']
E   assert 2 == 1
```

---

### [CLUS-5] A host downed by a strategy keeps running its instance while the majority starts another
**Severity:** High
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/cluster/singleton.py:59-63, 187-205`, `src/tapio/cluster/cluster.py:66, 200-201`
**Issue:** #203

**What's wrong.** The manager subscribes only to `MemberUp`, `MemberLeaving` and `MemberRemoved`. A node on the losing side of a split moves itself to `Down` and stays there: its own view never converges, so it never removes itself. Its manager never hears an event about itself, so its keeper keeps running. `terminate_on_down` defaults to `False`, so nothing else stops the instance either. On the majority side, the old host is downed and removed, and a successor starts.

**Why it matters.** With `KeepMajority` and default settings, a partition that isolates the host leaves two instances running for as long as the minority process lives. Pekko avoids this by making coordinated shutdown on down the default. Here the singleton docs do not mention that `terminate_on_down` (or `when_downed`) is a precondition for the singleton's guarantee.

**Proposed fix.** Have the manager let go the moment its own node is downed:

```python
_MANAGER_EVENTS = (MemberUp, MemberLeaving, MemberRemoved, SelfDown)
...
case SelfDown():
    self._hosts.clear()   # this node is out; it hosts nothing
```

Also document in `ClusterSingleton` that the old host's instance stops on `SelfDown`, and that the successor may start before the downed side has heard. A split overlap is bounded by `down_after` on the two sides, not removed. Rejected alternative: defaulting `terminate_on_down=True`. That is a reasonable default, but it is a policy change to `Cluster`, and the manager would still depend on it.

**Test to prove it.** Probe `test_a_downed_host_lets_its_instance_go` (3 nodes, `KeepMajority`, partition the host, wait for `when_downed` and a successor start). It fails today:
```
E   AssertionError: {'tapio://node1@127.0.0.1:46255', 'tapio://node2@127.0.0.1:37867'}
```

---

### [CLUS-6] A downing strategy that raises (a lease backend that cannot be reached) stops the daemon on every node
**Severity:** High
**Category:** Bug
**Status:** Confirmed (raises). Suspected (hangs).
**Location:** `src/tapio/cluster/daemon.py:475-477, 1011`
**Issue:** #204

**What's wrong.**

```python
return Behaviors.supervise(...).on_failure(SupervisorStrategy.resume(), on=TapioError)
...
self._apply_downing(await self._strategy.decide(self._state))
```

`LeaseMajority.decide` awaits `Lease.acquire`. The `Lease` protocol is documented as reaching "a row in a database, a Kubernetes lease, a key in etcd". It says nothing about errors. During a partition, that call is the one most likely to raise `OSError` or `TimeoutError`, and neither is a `TapioError`. The exception leaves `_receive`, and the daemon stops (`stopping after a failure in Tick`). With the daemon stopped, the node answers no heartbeats and holds no well-known name. `when_downed` never fires and `Cluster.members` is frozen. The application is not told. A lease call that hangs instead freezes the daemon's turn indefinitely, with the same visible effect.

**Why it matters.** Both sides of the split lose their daemon at the moment the split needs resolving. The node is out of the cluster without being `Down`, so `terminate_on_down` does nothing.

**Proposed fix.**

```python
try:
    async with asyncio.timeout(self._settings.heartbeat_interval.total_seconds()):
        verdict = await self._strategy.decide(self._state)
except Exception:   # CancelledError is BaseException and still propagates
    _log.exception("%r could not decide about %s; trying again next turn", self._strategy, sorted(unreachable))
    return
self._apply_downing(verdict)
```

Rejected alternative: widening the supervision to `resume` on every `Exception`. That would hide real daemon bugs, and it would still abort the rest of the turn (`_emit`, `_changed.set()`).

**Test to prove it.** Probe `test_a_lease_that_fails_does_not_take_the_daemon_down` (2 nodes, `LeaseMajority` over a lease whose `acquire` raises `ConnectionRefusedError`, then partition). It fails today:
```
E   assert [False, False] == [True, True]     # daemon_running on each node
ERROR tapio://node1/system/cluster#2: stopping after a failure in Tick
```

---

### [CLUS-7] A node that learns of its own downing as `removed` is never told it was downed
**Severity:** High
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/cluster/daemon.py:1049-1073` (`_announce_if_downed`), `daemon.py:1140-1146`, `daemon.py:529-539`
**Issue:** #205

**What's wrong.** `_announce_if_downed` fires only on `me.status is MemberStatus.DOWN`, and `SelfDown` is emitted only on a move into `DOWN`. The other members stop gossiping to a downed member, since it is not `alive`. It learns its fate only when one of them answers its own gossip, and by then the leader may already have removed it. The node then goes straight from `up` to `removed`. It publishes no `ClusterDowned`, `when_downed` never returns, `terminate_on_down` never fires, and `SelfDown` is not delivered. The daemon then stops itself on `REMOVED` (line 539), so the node is a zombie: the process runs, but it has no daemon and no signal.

Measured with an operator down, 3 repetitions each: in a 2-node cluster the downed node always saw `['up', 'removed']`, and `downed.is_set()` was `False`. In a 3-node cluster it saw `['up', 'down', 'removed']`, because the third node relayed `down` first. With a strategy, the same happens when the two sides' `down_after` windows start at different times and the majority removes the minority before the minority decides.

**Why it matters.** `when_downed` and `terminate_on_down` are the documented way to shut down a downed node. A downed node that keeps running keeps its singleton instance (see CLUS-5) and keeps serving its other work under an identity the cluster has written off.

**Proposed fix.** Treat "removed without having been seen leaving" as downed:

```python
def _announce_if_downed(self) -> None:
    me = self.self_member
    if me is None or self._downed_announced:
        return
    if me.status is MemberStatus.DOWN or (
        me.status is MemberStatus.REMOVED and not self._left_gracefully
    ):
        ...
```

Set `_left_gracefully` when the node's own status is first seen in `_LEAVING_STATUSES`. Make the same change to the `SelfDown` condition in `_emit`. Rejected alternative: having the leader delay removal of a downed member until that member has seen `down`. A downed member is usually unreachable, so the delay would be unbounded.

**Test to prove it.** Probe `test_a_node_downed_by_an_operator_is_told_so` (2 nodes, `KeepMajority`, `terminate_on_down=True`, `POST /down` for node2 on node1). It reaches `REMOVED`, then fails with `TimeoutError` on `when_downed()`.

---

### [PERI-1] The shutdown deadline is not a bound: a cancelled handler's async cleanup runs as long as it likes
**Severity:** High
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/dispatch/tasks.py:14-42`, called from `src/tapio/actor/cell.py:884-891`
**Issue:** #206
**Related:** fix together with CORE-1, which replaces the shield in the same function.

**What's wrong**
At the deadline, `ActorCell.stop` cancels the actor's task and then waits for it with no bound at all:

```python
# cell.py:884-891
        try:
            async with asyncio.timeout_at(deadline):
                await asyncio.shield(self._task)
        except TimeoutError:
            ...
            await cancel_and_wait(self._task)
```

```python
# tasks.py:32-34
    task.cancel()
    try:
        await task
```

A handler whose `finally` (or `__aexit__`) awaits anything, for example closing an HTTP client or a DB pool, keeps running that cleanup after the cancel, and `terminate()` waits for all of it. A handler that catches `CancelledError` and loops never returns, and `terminate()` hangs forever.

**Why it matters**
AGENTS.md: "Shutdown races one deadline for the whole tree". `docs/lifecycle.md`: "the worst case tracks `shutdown_timeout`". With ordinary async cleanup code the worst case is unbounded. The probe sets `shutdown_timeout=0.1s`; a handler with `finally: await asyncio.sleep(1.0)` makes `terminate()` take 1.10 s. A Kubernetes preStop budget sized from `shutdown_timeout` gets SIGKILLed.

**Proposed fix**
Give `cancel_and_wait` an optional bound and use a short grace at the call site, logging what did not finish:

```python
async def cancel_and_wait(task: "asyncio.Task[None]", *, timeout: float | None = None) -> bool:
    """...Returns whether the task finished inside `timeout`."""
    task.cancel()
    current = asyncio.current_task()
    before = current.cancelling() if current is not None else 0
    try:
        done, _ = await asyncio.wait({task}, timeout=timeout)
    finally:
        if current is not None and current.cancelling() > before:
            raise asyncio.CancelledError
    if task in done and not task.cancelled():
        task.exception()  # retrieve it, so asyncio does not log it
    return task in done
```

and in `ActorCell.stop`: `finished = await cancel_and_wait(self._task, timeout=_CANCEL_GRACE)`, warning "did not finish its cancellation within the grace; abandoned" when `finished` is False. The abandoned task still belongs to the cell and is cancelled; it is only no longer awaited. Rejected alternative: keep waiting but log a warning at the deadline. It keeps the hang, which is the bug.

**Test to prove it** (ran as a probe, fails today)

```python
async def test_shutdown_deadline_is_not_bounded_after_cancellation():
    settings = IsolatedTapioSettings(shutdown_timeout=timedelta(milliseconds=100))
    system = ActorSystem("deadline", settings)
    entered = asyncio.Event()

    async def on_job(message: Job) -> Behavior[Job]:
        entered.set()
        try:
            await asyncio.sleep(30)
        finally:
            await asyncio.sleep(1.0)
        return Behaviors.same()

    ref = system.spawn(Behaviors.receive_message(on_job, msg_type=Job), "slow")
    ref.tell(Job(item=1))
    await entered.wait()
    started = time.perf_counter()
    await system.terminate()
    assert time.perf_counter() - started < 0.5
```
```
E       AssertionError: shutdown_timeout=0.1s, terminate took 1.10s
```

---

### [PERI-2] `remote.reconnect` returns success on a link the peer refuses, so the documented repair silently does nothing
**Severity:** High
**Category:** Bug
**Also touches:** Bug (cross-subsystem: remote) and Docs (testkit)
**Status:** Confirmed
**Location:** `src/tapio/remote/endpoint.py:655-690`, `src/tapio/remote/association.py:589-604`; docs at `src/tapio/testkit/remote.py:14-22`, `:82-90`, `:297-305`, `docs/testing.md:266-268`, `docs/unreachable.md:73-75`
**Issue:** #207
**Related:** the link pass reached the same conclusion from the dial-race side (LINK-15 item 6, LINK-2).

**What's wrong**
After a symmetric partition both sides quarantine. `clear_quarantine`'s docstring says the far side "refuses the dial until it has relented too", and `reconnect` documents `HandshakeError: If the peer ... refused this system`. In practice alpha's handshake completes, `wait_connected` resolves, `reconnect` returns normally, and only then does beta log `refused a link from tapio://alpha...` and close it. The next `tell` is lost.

The testkit module docstring teaches exactly this sequence as the repair:

```python
    nodes.heal()           # the packets flow again, and nothing re-associates
    await nodes.alpha.remote.reconnect(nodes.beta.address)
```

and `TwoNodes.heal`, `LinkFaults.heal`, `docs/testing.md` and `docs/unreachable.md` ("`await system.remote.reconnect(peer_address)` clears the quarantine and re-associates") all say `reconnect` alone is the repair, with no mention of `clear_quarantine` on the other side. The examples (`partition.py:212`, `remote_ask.py:135`) do it correctly.

**Why it matters**
A caller follows the docstring, sees `reconnect` succeed, and then loses every message. The docstring's own `Raises: HandshakeError` contract is broken, so a supervisor that retries on `HandshakeError` never retries.

**Proposed fix**
Remote side: make the refusal visible to the dialler before `_ready` resolves, either by having the refusing side answer the handshake with an explicit refusal frame, or by having `reconnect` wait for the first post-handshake read or heartbeat before returning. Docs side: the testkit example and the two doc pages should read

```python
nodes.heal()
nodes.beta.remote.clear_quarantine(nodes.alpha.address)   # each side relents for itself
await nodes.alpha.remote.reconnect(nodes.beta.address)
```

with the sentence "A symmetric partition quarantines both sides, so the side being dialled must `clear_quarantine` first."

**Test to prove it** (ran as a probe, fails today)

```python
async def test_reconnect_after_symmetric_partition_without_clearing_the_other_side():
    async with two_nodes() as nodes:
        seen: list[int] = []
        worker = nodes.beta.spawn(counting(seen), "w")
        here = await nodes.alpha.resolve(uri(nodes.beta, worker), expect=Tick)
        here.tell(Tick(n=1))
        await eventually(lambda: seen == [1])
        nodes.partition()
        await eventually(lambda: bool(nodes.alpha.remote.quarantined)
                         and bool(nodes.beta.remote.quarantined), within=5.0)
        nodes.heal()
        with pytest.raises(HandshakeError):      # what the docstring promises
            await nodes.alpha.remote.reconnect(nodes.beta.address)
```
The probe version showed `reconnect` returning, `beta quarantined after alpha reconnect: (Address('tapio://alpha@...'),)`, beta logging `refused a link from tapio://alpha...`, and `seen == [1]` after a second send.

---

### [PERI-3] `terminate_on_down=True` is ignored unless a downing strategy is set, so an operator-downed node never shuts down
**Severity:** High
**Category:** Bug
**Also touches:** Bug and Docs (cross-subsystem: cluster)
**Status:** Confirmed
**Location:** `src/tapio/cluster/cluster.py:131-132`, docstring `:83-90`; `src/tapio/cluster/messages.py:186-193`; `docs/clustering.md:412-419`
**Issue:** #208

**What's wrong**

```python
        if downing is not None and terminate_on_down:
            self._down_watch = self._terminate_when_downed()
```

The docstring justifies this with "It has no effect without a `downing` strategy, since nothing then downs this node." That is false: the management port (`Down` message) downs a node with no strategy configured, and `docs/clustering.md:416-419` says that case is exactly what the port is for, ending "the downed member hears the decision as gossip and shuts itself down". The daemon does publish `ClusterDowned` (`daemon.py:_announce_if_downed` runs regardless of strategy) and `when_downed` resolves, but nobody is subscribed to terminate.

**Why it matters**
An operator downs a stuck member, the docs say it shuts itself down, it does not. It keeps running as a `Down` member that may not rejoin as itself, holding its singleton manager, its ports and its work.

**Proposed fix**

```python
        if terminate_on_down:
            self._down_watch = self._terminate_when_downed()
```

and drop the "It has no effect without a `downing` strategy" sentence, replacing it with "It applies however this node was downed, by a strategy or by an operator." Rejected: changing the docs to say an operator down never shuts the node down. That leaves `terminate_on_down=True` silently meaningless, which is worse.

**Test to prove it** (ran as a probe, `probes/periphery/test_periphery_cluster.py`)

```python
async def test_an_operator_downed_node_with_terminate_on_down_shuts_down():
    async with cluster_of(3, terminate_on_down=True) as nodes:
        seeds = seeds_of(nodes)
        await asyncio.gather(*(n.cluster.join_seed_nodes(seeds) for n in nodes))
        first, _, third = nodes
        first.cluster._ref.tell(Down(address=third.address))  # what the port sends
        await eventually(lambda: third.status is MemberStatus.DOWN, within=5.0)
        await asyncio.wait_for(third.cluster.when_downed(), 2.0)       # passes
        await asyncio.wait_for(third.system.when_terminated(), 3.0)    # times out today
```
```
E                   TimeoutError
```

---

### [PERI-4] `docs/lifecycle.md` says `PostStop` runs on a restart's teardown; `PreRestart` says it does not
**Severity:** High
**Category:** Docs
**Status:** Confirmed
**Location:** `docs/lifecycle.md:61-70`; `src/tapio/actor/signals.py:41-49`
**Issue:** #209

**What's wrong**
`lifecycle.md:68-70`: "`PostStop` is where a resource an actor opened is closed. It runs for a stop, a restart's teardown and a shutdown alike, so there is one place to write it rather than three." `PreRestart`'s docstring: "`PostStop` does not follow, because a restart is not a stop." The tests agree with the docstring (`tests/actor/test_supervision.py:229` asserts `"PostStop" not in seen` after restarts). The code bug where PostStop follows PreRestart during a backoff is CORE-2; this finding is about the page.

The same table lists `ChildFailed` as something "an actor is told about", while `signals.py:11-14` and `:75` say it is "Handled by the runtime, never by a behavior's signal handler".

**Why it matters**
A reader who closes a connection only in `PostStop`, as the page tells them to, leaks one connection per restart. Under `restart()` with no limit that is an unbounded leak.

**Proposed fix**
Replace lines 68-70 with: "`PostStop` is where a resource an actor opened is closed when the actor stops, by its own choice, by a supervisor's `stop`, or by shutdown. A restart is not a stop: the failing incarnation gets `PreRestart` instead, and `PostStop` does not follow. Close the resource in both handlers, or hold it outside the part a restart re-runs." Drop `ChildFailed` from the table, or mark it "handled by supervision, never seen by `on_signal`".

**Test to prove it**
Docs-only. The existing `tests/actor/test_supervision.py:229` already pins the code side; add a doc test that restarts once with an `on_signal` recorder and asserts `seen == ["PreRestart"]`.

---

### [PERI-5] The `Routers.group` snippet calls `ctx.spawn` without the required name
**Severity:** High
**Category:** Docs
**Status:** Confirmed
**Location:** `src/tapio/actor/router.py:219-221`; `docs/clustering.md:357-359`
**Issue:** #210

**What's wrong**

```python
proxy = ctx.spawn(Routers.group(Job, role="worker", path="/user/worker"))
```

`ActorContext.spawn(behavior, name, mailbox=None)` has no default for `name`, so this is a `TypeError`. It appears in the docstring rendered on the reference page and in `docs/clustering.md`, which is not a snippet include and so is never executed.

**Why it matters**
The only usage example for the group router fails on first copy-paste.

**Proposed fix**

```python
proxy = ctx.spawn(Routers.group(Job, role="worker", path="/user/worker"), "workers")
```

Better: move the clustering-page block into an included test file (see PERI-20) so it runs.

**Test to prove it** (probe)

```python
def test_routers_group_docstring_example_calls_spawn_without_a_name():
    params = inspect.signature(ActorContext.spawn).parameters
    assert params["name"].default is not inspect.Parameter.empty
```
```
E       assert <class 'inspect._empty'> is not <class 'inspect._empty'>
```

---

### [PERI-6] The `BehaviorTestKit` module example builds `Spawned("worker")`, which raises
**Severity:** High
**Category:** Docs
**Status:** Confirmed
**Location:** `src/tapio/testkit/behavior.py:9-16`
**Issue:** #211

**What's wrong**

```python
assert kit.effects == (Spawned("worker"),)
```

`Spawned.__init__(self, name, behavior, mailbox, ref)` takes four positional arguments. This module docstring is rendered on the reference page.

**Why it matters**
The first example of the kit a reader sees fails with `TypeError` before it gets to the assertion.

**Proposed fix**
Use the comparison the class was designed for: `assert kit.effects == ("worker",)` (as `docs/testing.md` does via `tests/docs/test_testing_page.py:201`).

**Test to prove it** (probe)

```python
def test_behavior_testkit_docstring_example_constructs_spawned():
    from tapio.testkit import Spawned
    Spawned("worker")
```
```
E       TypeError: Spawned.__init__() missing 3 required positional arguments: 'behavior', 'mailbox', and 'ref'
```

---

### [PERI-7] The frame shown in `docs/remoting.md` is rejected by the decoder
**Severity:** High
**Category:** Docs
**Status:** Confirmed
**Location:** `docs/remoting.md:116-123`; `src/tapio/remote/codec.py:400-439`
**Issue:** #212

**What's wrong**
The documented frame addresses `"/user/checkout/session-7#f3a1c8"`. The uid is a decimal int: `format_target` writes `#{path.uid}` and `parse_target` does `int(fragment or 0)`, so `f3a1c8` raises and the whole frame becomes `MessageDecodingError: malformed frame`. The other fields (`v`, `to`, `from`, `t`, `p`) match `encode`.

**Why it matters**
This page is the wire spec a non-Python peer or a packet capture tool would be written from. A frame built from it is dead-lettered as malformed.

**Proposed fix**
Change the example to `"to": "/user/checkout/session-7#42"` and add one sentence: "The fragment is the decimal incarnation uid."

**Test to prove it** (probe)

```python
def test_remoting_page_frame_example_decodes():
    body = (b'{"v": 1, "to": "/user/checkout/session-7#f3a1c8",'
            b' "from": "tapio://web@10.0.0.9:25520",'
            b' "t": "orders.protocol.Reserve", "p": {"sku": "X-1", "qty": 2}}')
    decode(len(body).to_bytes(4, "big") + body, system="orders")
```
```
E   tapio.errors.MessageDecodingError: malformed frame: invalid literal for int() with base 10: 'f3a1c8'
```

---

### [PERI-8] One `Routers.pool(...)` value spawned twice shares one rotation, and half of each pool never gets work
**Severity:** High
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/actor/router.py:194-207`; same shape in `src/tapio/cluster/router.py:97-98` (Suspected there, see below)
**Issue:** #213

**What's wrong**

```python
        chosen = strategy if strategy is not None else RoundRobin()

        def build(ctx: ActorContext[T]) -> Behavior[T]:
            ...
            return _PoolBehavior(routees, chosen, _pool_msg_type(routees))
```

The strategy is created once, when the behavior value is built, and every actor spawned from that value shares it. Behaviors are values and are routinely spawned more than once (`pool = Routers.pool(4, worker()); for region in regions: ctx.spawn(pool, region)`). With two routers fed alternately, the shared counter gives router A only even ticks and router B only odd ticks. The same docstring warns the reader about exactly this for a stateful routee behavior ("Every routee starts from the same object, so an already-built behavior holding state would be shared by the whole pool"), then does it itself. A restarted router also keeps the old rotation, which is harmless; a user-supplied stateful strategy shared across pools is worse.

`cluster/router.py` builds one `_GroupRouter` object per `group_router(...)` call, and its `behavior()` closures mutate `self._routees`, `self._daemon` and `self._here`, so two spawns of one group-router value share a routee table. That half was read but not run, so it is Suspected.

**Why it matters**
With the default round-robin and two pools of 2 fed in alternation, `a/routee-2` and `b/routee-1` get nothing. Throughput halves, and so does the backpressure the user sized.

**Proposed fix**
Build the strategy per actor. Accept a factory, keeping the instance form for compatibility:

```python
        def build(ctx: ActorContext[T]) -> Behavior[T]:
            chosen = strategy if strategy is not None else RoundRobin()
            ...
```

and document on `strategy=` that a stateful strategy instance passed in is shared by every actor spawned from this behavior, so pass a fresh one per spawn or a `Callable[[], RoutingStrategy]`. Rejected: copying the strategy with `copy.copy`, which is a guess about user classes.

**Test to prove it** (probe)

```python
async def test_one_pool_behavior_spawned_twice_starves_half_of_each_pool():
    ...  # routees record f"{parent}/{name}"
    pool = Routers.pool(2, tagged())
    a = system.spawn(pool, "a"); b = system.spawn(pool, "b")
    for n in range(4):
        a.tell(Job(item=n)); await eventually(lambda: total() == 2 * n + 1)
        b.tell(Job(item=n)); await eventually(lambda: total() == 2 * n + 2)
    assert sorted(got) == ["a/routee-1", "a/routee-2", "b/routee-1", "b/routee-2"]
```
```
E           AssertionError: {'a/routee-1': [0, 1, 2, 3], 'b/routee-2': [0, 1, 2, 3]}
```

---


## Medium

### [CORE-7] `ActorRef[T]` is invariant, so the type checker rejects a safe substitution and pushes users to `cast`
**Severity:** Medium
**Category:** Quality
**Status:** Confirmed (mypy)
**Location:** `src/tapio/actor/ref.py:20-24`
**Issue:** #214

**What's wrong.** `T = TypeVar("T", bound=Message)` is invariant. A ref that accepts `A | B` can be used wherever a ref that accepts `A` is wanted, because everything you can tell the second you can tell the first. That is contravariance, and it is Pekko's `ActorRef[-T]`. mypy `--strict` on a scratch module:

```
variance.py:20: error: Argument 1 to "wants_a_sink" has incompatible type "ActorRef[A | B]"; expected "ActorRef[A]"  [arg-type]
```

**Why it matters.** The most common case is putting `ctx.self_ref` (typed with the actor's whole protocol, a union) into a `reply_to: ActorRef[Reply]` field. Under strict mypy, which this project advertises, that fails, and the user's way out is a `cast` that would also silence a real mistake. `T` only appears in contravariant positions (`tell`, `offer`, the return of `ask`'s `make`), so the change is sound.

**Proposed fix.**

```python
T_contra = TypeVar("T_contra", bound=Message, contravariant=True)

class ActorRef(Generic[T_contra]):
    def tell(self, message: T_contra) -> None: ...
```

Apply the same change to the subclasses (`LocalActorRef`, `RemoteRef`, `AdapterRef`, `PromiseRef`). `Behavior[T]` is a separate question: it is consumed by a cell typed with the same `T`, so leave it invariant.

**Test to prove it.** A `tests/typing/` module checked by `make type`:

```python
def wants(ref: ActorRef[A]) -> None: ...
def ok(both: ActorRef[A | B]) -> None:
    wants(both)            # must type-check
def bad(only_a: ActorRef[A]) -> None:
    x: ActorRef[A | B] = only_a  # type: ignore[assignment]  (must stay an error)
```

---

### [WIRE-6] The dialler signs any nonce before it checks the listener, and the proof binds nothing else
**Severity:** Medium
**Category:** Bug
**Status:** Confirmed (probe fails today)
**Location:** `src/tapio/remote/handshake.py:340-358`, `366-372`
**Issue:** #215

**What's wrong.** Both proofs are `HMAC(secret, nonce)`. They do not bind the role, the other nonce, or the identity that travels next to them. The dialler answers the listener's challenge before it has verified anything about the listener:

```python
340    hello = _read(_ServerHello, await link.read_link(timeout), "server-hello")
...
354            proof=_proof(secret, hello.nonce),
```

So anyone a node can be made to dial becomes a signing oracle. The probe uses a relay with no secret. It opens a connection to the guarded node S and takes S's nonce. It hands that nonce to an honest dialler C as its own server-hello. It takes C's proof, rewrites `system`/`address` to `tapio://evil@10.6.6.6:6666`, and forwards the hello to S. S sends a `welcome`, so `accept` returned an identity of the attacker's choosing.

**Why it matters.** `handshake.py` says "a peer holding no secret cannot pass for one that does", and `security.md` presents the secret as "the whole authorization model". With plaintext links an on-path attacker already wins, as the docs say. This attack needs less than that: only a way to make one node dial an address, such as a ref in a message, a seed or DNS entry, or a member address in gossip. It also lets the attacker pick the identity, which keys the association.

**Proposed fix.** Bind role, both nonces and both identities into each MAC:

```python
def _transcript(role: str, server_nonce: str, client_nonce: str, client: str, server: str) -> bytes:
    return "\0".join(("tapio-v1", role, server_nonce, client_nonce, client, server)).encode()
```

The client proves over the transcript with its own address. The server proves over it with both addresses. A rewritten identity then fails at S. A pure relay can still connect C's real identity through, which only TLS (or a TLS exporter bound into the MAC) prevents. Say that in `security.md`. This changes the wire contract, so it raises `PROTOCOL_VERSION` and says so in the pull request. I rejected only reordering so the server proves first: that turns the listener into the oracle instead.

**Test to prove it** (`test_wire_probes.py::test_relay_without_the_secret_is_refused`, fails today with `a party with no secret was welcomed as tapio://evil: ['welcome']`).

---

### [WIRE-7] Unauthenticated peers can make the listener buffer `max_frame_bytes` each, with no limit on how many
**Severity:** Medium
**Category:** Bug
**Status:** Confirmed (probe fails today)
**Location:** `src/tapio/remote/transport.py:301-321` (`read_link` uses the message cap), `endpoint.py:275-282` (no limit on pending handshakes)
**Issue:** #216

**What's wrong.** The client-hello is read with `read_frame`, so its cap is `max_frame_bytes` (4 MiB by default). A legitimate hello is under 512 bytes. `StreamReader.readexactly` resumes reading while it waits, so the buffer grows to the declared length. Eight connections that proved nothing hold 33,554,424 bytes in the probe. Nothing caps concurrent handshakes.

**Why it matters.** `security.md` sells the cap as "the cheapest way to stop a hostile or buggy peer from exhausting memory". Before authentication the cost per connection is 4 MiB multiplied by however many connections the attacker opens, within each `handshake_timeout`.

**Proposed fix.** Give `read_link` its own small limit, for example `_HANDSHAKE_FRAME_BYTES: Final = 4096`, checked with `frame_length(prefix, max_frame_bytes=min(limit, self._max_frame_bytes))`. Cap pending handshakes in the endpoint, and close beyond the cap (the management port already caps at 32).

**Test to prove it** (`test_wire_shutdown.py::test_an_unauthenticated_hello_is_capped_well_below_max_frame_bytes`, fails today with `AssertionError: 33554424 bytes buffered for 8 peers that proved nothing`).

---

### [WIRE-8] A TLS listener ignores `handshake_timeout`: the TLS handshake gets asyncio's 60 seconds
**Severity:** Medium
**Category:** Bug
**Status:** Confirmed (probe fails today)
**Location:** `src/tapio/remote/transport.py:419-435`
**Issue:** #216

**What's wrong.** `listen()` calls `asyncio.start_server(handler, sock=listener, ssl=ssl_context)` with no `ssl_handshake_timeout`, so the default of 60 s applies. The endpoint's handler, and with it `handshake_timeout`, only starts once TLS has completed. The dialler side is fine, since `_dial` wraps `connect` in `asyncio.timeout`.

**Why it matters.** `RemoteSettings.handshake_timeout` says "One deadline for the whole opening, so a peer that accepts a connection and then says nothing costs this and not a parked task". On a TLS listener a silent TCP peer costs 60 s, invisible to the endpoint, and it holds `terminate` (WIRE-3).

**Proposed fix.** Add a `handshake_timeout: float` parameter to `listen` and pass `ssl_handshake_timeout=handshake_timeout if ssl_context else None`. The endpoint already has the value.

**Test to prove it** (`test_wire_probes.py::test_tls_listener_closes_a_silent_tcp_peer_at_the_handshake_deadline`, fails today with `Failed: a TCP peer that never starts TLS was held past the deadline`, with `handshake_timeout=0.3s` and a 2 s wait).

---

### [WIRE-9] "Strict validation" off the wire is lax validation
**Severity:** Medium
**Category:** Docs
**Status:** Confirmed (probe fails today)
**Location:** `src/tapio/remote/codec.py:30-33`, `docs/security.md` ("Strict validation as the decode")
**Issue:** #236

**What's wrong.** `codec.py` says "a message off the wire is validated by construction, strictly, with no way to skip it". `security.md` says "a field that is the wrong type or missing is a decoding failure". `Message.model_config` has no `strict=True`, and `receive_frame` calls `model_validate_json(frame.payload)` without `strict=True`. A frame with `"p": {"n": "5"}` for `n: int` is delivered as `Tick(n=5)`.

**Why it matters.** A reader who relies on "wrong type is refused at the boundary", for example to skip checks in a handler, is wrong. Lax coercion is a reasonable choice, but the security page should not claim the opposite.

**Proposed fix.** Fix the docs, since flipping to strict would refuse JSON that a non-tapio sender writes for `datetime`, `UUID` and similar fields. Replacement for `security.md`: "**Validation as the decode.** A frame becomes a message by being validated against the registered model, in Pydantic's default (lax) mode. A missing field, or a value that cannot be converted to the field's type, is a decoding failure and not a surprise inside a handler. A value that can be converted, such as `"5"` for an `int`, is converted. Set `strict=True` in a message's `model_config` to refuse those too." In `codec.py`, replace "strictly" with "against the registered model".

**Test to prove it** (`test_wire_probes.py::test_wire_validation_is_strict`, fails today with `a string '5' was accepted for an int field: [5]`). After the docs fix, keep it inverted as a test that documents the lax behaviour.

---

### [WIRE-10] No test ever opens a TLS link
**Severity:** Medium
**Category:** Quality
**Status:** Confirmed
**Location:** `tests/remote/test_transport.py:200-206` (the only remoting TLS test)
**Issue:** #239

**What's wrong.** The suite's only remoting TLS test checks that a missing certificate file raises. Nothing establishes a TLS link, checks that a server with `cafile` refuses a client without a certificate, or checks that the dialler refuses a hostname mismatch. `make check` would not notice a regression in any of those.

**Why it matters.** `security.md` tells every multi-machine deployment to use TLS. A security feature with no positive test can break silently.

**Proposed fix.** Generate a CA and two node certificates in a session fixture (the probe uses `openssl`; `trustme` as a dev dependency is cleaner). Add three tests: a round trip, a server with `cafile` refusing a client with no certificate, and a dialler with `check_hostname=True` refusing a certificate for another name.

**Test to prove it.** `test_wire_probes.py::test_tls_round_trip_between_two_systems` and `test_tls_contexts` pass today. They confirm that the server requires a client certificate when `cafile` is set, that client verification is `CERT_REQUIRED` even with `check_hostname=False`, and that both contexts have a minimum of TLS 1.2.

---

### [LINK-10] A spawner stops, with every worker it started, when an arguments validator raises anything but `ValueError`
**Severity:** Medium
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/remote/spawner.py:448-473`
**Issue:** #217

**What's wrong** `factory.arguments` is guarded by `except ValidationError` and `except RefResolutionError`. Pydantic wraps only `ValueError` and `AssertionError` from a validator into `ValidationError`. A `TypeError` or `KeyError` from an arguments model's validator escapes `on_spawn`, so the spawner fails. The default strategy is stop, and its children (every worker it started) stop with it. The input that triggers it comes from the peer.

**Why it matters** The module says "every failure is a reply rather than an exception ... one malformed request must not stop the rest". A peer that can reach the spawner can stop all of its workers with one request.

**Proposed fix** Mirror the guard already used around `factory.build`:

```python
    except RefResolutionError: ...
    except Exception as error:  # a reply beats stopping the spawner
        ctx.log.exception("validating the arguments for %r raised", key)
        return SpawnFailed(factory=key, reason=SpawnFailure.INVALID_ARGS,
                           detail=f"{key!r} arguments raised {type(error).__name__}: {error}")
```

**Test to prove it** Ran as `probes/remote-link/test_spawner_args.py`. The probe spawns one worker, then sends `Spawn(args={"size": -1})` to a factory whose validator raises `TypeError` on a negative size. Output on `main`: `AskTargetTerminated: tapio://spawning/user/spawner#1 stopped before replying to an ask expecting SpawnReply`.

---

### [LINK-11] A frame being flushed by `_open` vanishes when the reader is cancelled
**Severity:** Medium
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/remote/association.py:944-950`
**Issue:** #218

**What's wrong**

```python
944        while self._pending:
945            outbound = self._pending.popleft()
946            try:
947                await link.write_frame(outbound.frame)
948            except OSError:
949                self._pending.appendleft(outbound)
950                raise
```

The frame is taken off `_pending` before the await. A `CancelledError` there leaves it in neither place. `adopt` cancels this reader through `previous.close()`, and so does `_close_sockets` on every stop and shutdown. `_write` handles the same situation on the actor side with `_lost_with_old_link`, but `_open` has no equivalent.

**Why it matters** A frame lost with no dead letter, on paths (dial race, close during flush) where the failure is visible to this side.

**Proposed fix** Dead-letter the frame in hand on any exit, matching `_write`. It is not put back, because its bytes may already be in the old transport's buffer and at-most-once forbids resending it:

```python
            except BaseException as error:
                if isinstance(outbound, Outbound):
                    self._dead_letter(outbound.payload, outbound.recipient,
                                      DeadLetterReason.LINK_FAILED,
                                      detail=f"the link ended while it was being written: {error!r}")
                raise
```

`appendleft` on `OSError` is still wrong for the same reason: `_release` then publishes "stopped before it left" for a frame that may have left.

**Test to prove it** Ran as `probes/remote-link/test_open_cancelled.py`. Two frames sit in `_pending`. `_open` parks writing frame 1 on a link that never drains, `adopt` retires that link, and the winner is opened. Output on `main`: `AssertionError: ([], [b'\x00\x00\x00\x012'])`, `assert 1 == 2`. Frame 2 went out on the new link, and frame 1 was neither written nor dead-lettered.

---

### [LINK-12] Releasing a stale `_PeerWatcher` removes the live one that replaced it
**Severity:** Medium
**Category:** Bug
**Status:** Confirmed (mechanism); Suspected (end-to-end interleaving)
**Location:** `src/tapio/actor/watch.py:152-169`, `src/tapio/remote/association.py:1131-1133`
**Issue:** #219

**What's wrong** A cell's watcher map is keyed by `(address, path)`, and `remove_watcher` pops by key without checking that the stored watcher is the one being removed. Two associations for one peer exist at once while the old one finishes stopping. Suppose the peer's watcher re-watches the same actor through the new association, which replaces the old proxy under the same key. When the old association's `_end_watches` then calls `proxy.release()`, it removes the new proxy. The new association still believes the watch is registered, but the cell will never tell it, so the watcher on the peer waits forever. `_release` yields in `_close_sockets` before `_end_watches`, and under LINK-3 it can wait there indefinitely, which makes the window wide.

**Proposed fix**

```python
def remove_watcher(self, watcher: Watcher) -> None:
    key = (watcher.address, watcher.path)
    if self._watchers.get(key) is watcher:
        del self._watchers[key]
```

The same identity check applies to `stop_watching`'s caller in `cell.unwatch`.

**Test to prove it** Ran as `probes/remote-link/test_peer_watch_key.py`. It adds two proxies with the same key to a live cell, removes the first, and stops the actor. Output on `main`: `the live proxy was removed by the stale one's release`. To confirm the end-to-end case, the test would need an old association parked in `_close_sockets` (the stuck peer from LINK-3) while the peer re-dials and sends `Watch` for the same watcher path.

---

### [LINK-13] Dead letters from the association's own mailbox: internal messages, wrong recipient, no peer
**Severity:** Medium
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/actor/cell.py:1445-1476` as reached from `association.py:133-140`; `src/tapio/actor/timers.py:296-305`
**Issue:** #220

**What's wrong** When the association actor stops, the cell drains its mailbox through `ActorCell._dead_letter`. That unwraps `Outbound` (a `Carrier`), but it names the association actor as the recipient (`tapio://alpha/system/remote/beta-127.0.0.1-37077-1#2`), gives the reason `recipient-terminated`, and sets no `peer`. It also publishes `Close`, `Beat` and `LinkOut`, which are internal messages, as user dead letters. While the outbound buffer is full, each heartbeat tick also publishes a `Beat` as `mailbox-full` through `deliver_offloop`.

**Why it matters** `Association.send` and `docs/unreachable.md` promise dead letters "naming the peer". A subscriber that filters on `peer` or `recipient` misses these frames, and one that alarms on dead letters gets noise from runtime internals. Offer waiters woken by `mailbox.close()` land here as well.

**Proposed fix** Let the association account for its own mailbox in `_release`, before the cell drains it. That needs a hook to take what is left. Alternatively, have `Outbound` carry its own recipient and peer and let the cell's `_dead_letter` prefer them, and drop non-`Carrier` messages whose type is internal to remoting. I prefer the first, because it keeps "what never left" in one place.

**Test to prove it** Ran as `probes/remote-link/test_link_close_drop.py::test_internal_association_messages_never_reach_the_dead_letter_stream`. Output on `main`: `AssertionError: {'Close', 'Tick'}`. The log in the LINK-1 probe shows `Tick to tapio://alpha/system/remote/beta-...#2 (recipient-terminated)`.

---

### [LINK-14] Docs: "only silence quarantines", and the timings that make a dial look like silence
**Severity:** Medium
**Category:** Docs
**Status:** Confirmed by reading
**Location:** `docs/unreachable.md:30-34`, `src/tapio/remote/association.py:688-733, 784-822`, `src/tapio/settings.py:97-121`
**Issue:** #236

**What's wrong** `unreachable.md` says "Only silence quarantines". Two other paths reach `_declare_unreachable` and quarantine:
- A write that does not drain within `unreachable_after` (`_write` and `_beat` `TimeoutError`). The peer may be heartbeating at that moment.
- `_judge` during a dial, since the detector starts at construction. With `handshake_timeout` (default 5 s) longer than `unreachable_after`, a slow dial quarantines the peer instead of failing and redialling. Nothing validates the relation between the two, and `two_nodes()` ships exactly that combination (300 ms against 5 s).

Replacement text:

> A peer is quarantined when nothing has arrived from it for `unreachable_after`, and also when this node cannot get a frame into it for that long. Keep `handshake_timeout` below `unreachable_after`, or a dial that is merely slow is judged as silence.

Also add a model validator to `RemoteSettings` that rejects `handshake_timeout >= unreachable_after`.

---

### [MEMB-4] `leader` and `converged` are O(members x records) and run several times per daemon message
**Severity:** Medium
**Category:** Quality
**Also touches:** Quality (performance with a correctness consequence)
**Status:** Confirmed (timings by probe)
**Location:** `src/tapio/cluster/gossip.py:161-203`, `src/tapio/cluster/reachability.py:95-120`
**Issue:** #221

**What's wrong.** Both properties call `reachability.is_reachable(address, observers)` once per member, and each call scans every record. `_live_observers()` is rebuilt per call too. The daemon evaluates `leader` in `_lead`, then `converged`, then `leader` and `unreachable` twice more in `_digest` before and after any turn with subscribers (`daemon.py:488, 944-946, 1087, 1138`). It does this for every message, heartbeats included.

**Why it matters.** Measured at 500 nodes (`test_scale.py`, `test_scale_fix.py`):

| case | current leader + converged | one pass |
|---|---|---|
| ring only | 77 ms | 0.5 ms |
| after a healed split | 4.3 s | 10 ms |

A node at 500 nodes handles about 13 cluster messages a second (ticks, 5 heartbeats in, 5 replies, gossip). That is more than a second of CPU per second, so heartbeats are answered late and nodes start calling each other unreachable. The current code fails at the scale the ring was built for.

**Proposed fix.** Compute the unreachable set once and test membership in it. Same answer, proven equal by `test_same_answer_faster`:

```python
@property
def leader(self) -> str | None:
    gone = self.unreachable
    candidates = [m for m in sorted(self.members, key=sort_key)
                  if m.status not in _GONE and m.address not in gone]
    ...

@property
def converged(self) -> bool:
    gone = self.unreachable
    return all(m.address not in gone and m.address in self.seen
               for m in self.members if m.status not in _GONE)
```

Rejected alternative: a `functools.cached_property` on the frozen model. It works in Pydantic v2, but it hides the cost rather than removing it, and every `model_copy` throws the cache away.

**Test to prove it:**
```python
def test_leader_and_convergence_are_linear_in_the_records():
    g = build(500, cross_claims=True)
    start = time.perf_counter(); g.leader; g.converged
    assert time.perf_counter() - start < 0.1
```

---

### [MEMB-5] LeaseMajority names a side by an address the other side can also use
**Severity:** Medium
**Category:** Bug
**Status:** Suspected. The function is confirmed by probe. Whether a stale view lasts `down_after` depends on MEMB-7.
**Location:** `src/tapio/cluster/downing.py:465-481`
**Issue:** #222

**What's wrong.**
```python
478        owner = min(_addresses(reachable))
479        if await self.lease.acquire(owner):
```
The owner is "the lowest address this node still counts as reachable". Suppose a node on side two has seen only part of the split, for example only B unreachable but not A. It names the lease with A, which is the winning side's name. The lease is re-entrant per owner, so it succeeds, and that node downs B, a member of the *winning* side. `test_lease_owner_is_shared_by_a_stale_view_on_the_other_side` shows D on side two getting the verdict `{B}`. Once the split heals, that Down reaches side one and cannot be taken back.

**Proposed fix.** Make the owner something only one side can produce, and make every node of a side agree on it. One option: only the side's leader (`state.leader`, computed on the reachable side) acquires, using its own address. Every other node does nothing until the leader's Down decisions arrive by gossip, which is Akka's shape. If the "every node decides" design has to stay, at least refuse to decide while this node's view of its own side disagrees with the reachable set seen in gossip from its side-mates.

Rejected alternative: hash the reachable set into the owner. A stale node still produces a set of its own, so this only replaces "steals the winner's name" with "takes a third name", and the lease grants that name if it is free.

**Test to prove it:**
```python
async def test_a_stale_view_cannot_take_the_lease_under_the_other_sides_name():
    ...  # as in the probe
    assert b not in await strategy.decide(d_stale)
```

---

### [MEMB-6] `Lease.acquire` is unbounded, and the daemon awaits it inside its turn
**Severity:** Medium
**Category:** Bug
**Status:** Confirmed that there is no bound (probe). The stall follows from the one-consumer mailbox.
**Location:** `src/tapio/cluster/downing.py:381-392, 479`; `src/tapio/cluster/daemon.py:1011`
**Issue:** #223

**What's wrong.** The `Lease` protocol says nothing about timeouts or exceptions. `LeaseMajority.decide` awaits `acquire` with no bound, and `_down` awaits `decide` inside the message handler. The daemon docstring says "a peer's heartbeat is answered on the turns either side of it" (`daemon.py:985-988`). That is true, but no heartbeat is answered *during* the wait.

**Why it matters.** A lease backend that hangs longer than `unreachable_after` makes this node look dead to its own side. That changes the split set and resets the `down_after` timer on its side-mates. A backend that raises propagates into the daemon's supervision. `test_lease_majority_has_no_bound_on_acquire` shows `decide` never returns.

**Proposed fix.** Bound the wait, and treat a timeout or an error as "lost", which means down this side. That is the safe direction.
```python
try:
    held = await asyncio.wait_for(self.lease.acquire(owner), self.acquire_timeout)
except (TimeoutError, OSError):
    held = False
```
Document both `acquire_timeout` and the rule "a failure to reach the lease downs this side" in `Lease`.

Rejected alternative: run the decision in a child task. That keeps heartbeats flowing but adds a task the cell has to own and cancel, and the decision must still be bounded.

**Test to prove it:**
```python
async def test_a_hanging_lease_downs_this_side_in_bounded_time():
    verdict = await asyncio.wait_for(LeaseMajority(lease=Hangs(), acquire_timeout=0.1).decide(state), 1)
    assert a in verdict
```

---

### [MEMB-7] "A partition drops every link across it" only covers links that exist
**Severity:** Medium
**Category:** Docs
**Also touches:** Docs / Bug risk
**Status:** Suspected. The ring-only asymmetry is confirmed by probe.
**Location:** `docs/clustering.md:204-215`, `src/tapio/cluster/daemon.py:592-599`
**Issue:** #237

**What's wrong.** The docs correctly say that the probe ring alone does not give mirror images. `test_ring_alone_does_not_give_mirror_images` shows 9 nodes with `monitored_peers=2` where both sides keep themselves. The docs then rely on the transport: "a partition drops every link across it". A pair with no association produces no `PeerUnreachable`. A member is invisible to the far side if it is off every far-side ring and has no link to any far-side node. A freshly joined node is the typical case, since it has links only to the seeds and its ring.

**Proposed fix.** Make the ring skip members already unreachable when it chooses whom to watch, as Akka's `HeartbeatNodeRing` does, so watching moves on into the far side. Keep the transport as a faster signal. Do not drop the watch on an unreachable peer, otherwise the existing `follow` retraction would withdraw the claim. Docs replacement: "A strategy's safety depends on every far member being observed unreachable by someone on this side, through the ring or through a link that existed when the split happened. A member that has neither, such as one that joined moments before, can be counted by both sides."

**Test to prove it.** The probe above is the pure half. The daemon half needs 9 nodes with `monitored_peers=1`, one node joined just before a cut, and an assertion that both sides do not both survive.

---

### [MEMB-9] The leader rule as documented leaves out the reachability filter, and its fallback is wider than documented
**Severity:** Medium
**Category:** Docs
**Status:** Confirmed (probe)
**Location:** `src/tapio/cluster/gossip.py:161-184`, `docs/clustering.md:103-107`
**Issue:** #237

**What's wrong.** Both texts say "the first member in address order whose status is up or leaving", with a fallback "before anybody is up". The code also drops unreachable members. It falls back whenever no *reachable* Up or Leaving member exists, so a `Joining` or `Exiting` member can be leader while Up members exist (`test_leader_falls_back_to_a_joining_member_while_up_members_exist`, `test_leader_may_be_an_exiting_member`). A `LeaderChanged` subscriber then sees a Joining node named leader.

**Proposed fix (docs):** "The leader is the first member in address order, among those no live member reports unreachable, whose status is `up` or `leaving`. If there is none, for example before anybody is `up` or while every `up` member is unreachable, it is the first such member of any live status, `joining` and `exiting` included. A leader acts only on a converged view, so a fallback leader during a split does nothing until the split is resolved."

---

### [CLUS-8] `join_seed_nodes` reports success for a node that is removed or downed
**Severity:** Medium
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/cluster/cluster.py:498, 589-601`
**Issue:** #224

**What's wrong.** `_until(MemberStatus.UP, ...)` returns once `member.rank >= rank_of(UP)`. `DOWN` and `REMOVED` rank above `UP`. After `leave()`, or after the node was downed, calling `join_seed_nodes` again returns the `REMOVED` member at once. The daemon has stopped, so the `Seeds` message dead-letters.

**Why it matters.** The docstring says it returns "once the leader has accepted it" and raises `ClusterError` if the node is not `Up` in time. A supervisor loop that rejoins after a leave believes it is a member.

**Proposed fix.**

```python
member = await self._until(MemberStatus.UP, patience)
if member.status in (MemberStatus.DOWN, MemberStatus.REMOVED):
    raise ClusterError(f"{self.address} is {member.status} and cannot rejoin as itself; restart the system to join as a new incarnation")
```

Also check `self_member` before sending `Seeds`.

**Test to prove it.** Probe `test_join_after_leave_does_not_report_up`. It prints `join returned ... status=<MemberStatus.REMOVED: 'removed'>` and fails with `DID NOT RAISE ClusterError`.

---

### [CLUS-9] A subscriber that is a remote ref breaks every later turn of the daemon
**Severity:** Medium
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/cluster/daemon.py:1182-1203` (`_tell`), `daemon.py:1205-1233` (`_subscribe`), `cluster.py:411-441`
**Issue:** #225

**What's wrong.** `Cluster.subscribe` accepts any `ActorRef`, and watching a remote ref succeeds. Events are not registered on the wire (by design, events.py:16-18), so `RemoteActorRef.tell(MemberUp(...))` raises `MessageEncodingError`. `_tell` catches only `MessageTypeError` and `MailboxFullError`. The error leaves `_receive`, and supervision resumes the daemon. The subscriber is kept, though, so every later turn that has an event raises at the same point. The rest of each such turn is skipped: subscribers after it in the dict, `_changed.set()` (which `join_seed_nodes` and `leave` wait on), and the stop on `REMOVED`.

**Why it matters.** One misplaced subscription silently starves every local subscriber that came after it, for the rest of the process's life. Supervision then logs `resumed after a failure in GossipEnvelope` every round.

**Proposed fix.** Refuse a non-local subscriber in `_subscribe`, and drop a subscriber on any error about the message:

```python
if not isinstance(ref, (LocalActorRef, AdapterRef)):
    _log.warning("refused a cluster subscription from %s: events do not cross a link", ref.path)
    return
...
except (MessageTypeError, MessageEncodingError) as error:
    ... self._forget(ctx, subscriber.ref.path); return False
```

Also say "local actors only" in the `Cluster.subscribe` docstring.

**Test to prove it.** Probe `test_a_remote_subscriber_does_not_starve_the_local_ones`: a remote ref on node1 is subscribed first, then a local recorder, then node3 joins. The local recorder never hears `MemberUp(node3)`, and the log shows `MessageEncodingError: tapio.cluster.events.MemberUp has no wire key`.

---

### [CLUS-10] `Leave` is a registered wire message: any peer past the handshake can walk any member out
**Severity:** Medium
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/cluster/messages.py:111-122`, `daemon.py:512-513, 897-910`
**Issue:** #226

**What's wrong.** `Leave` is a `@register_message()` `WireMessage`. The daemon is a well-known name, so any system that completes a handshake can resolve `/system/cluster` on any member and send `Leave(address=<anyone>)`. The daemon applies it without checking who sent it. No code in the repository sends `Leave` across a link: `Cluster.leave` and management both `tell` the local daemon. `Down`, the equivalent operator action, is deliberately local (messages.py:180-184).

**Why it matters.** The management port puts a token or mTLS in front of leave and down, yet the wire offers leave to any holder of the remoting secret, member or not. That includes a plain remoting client that never joined the cluster. A leave of the singleton host also triggers the CLUS-4 overlap.

**Proposed fix.** Make `Leave` a local `Message` like `Down`, and remove its registration. Removing a registry key changes the wire contract, so the pull request should say whether `PROTOCOL_VERSION` moves. If a remote leave is wanted later, it should be an explicit, authenticated operator path. Rejected alternative: having the daemon accept a wire `Leave` only for the sender's own address. The envelope does not tell the daemon which peer sent a frame, so it would trust a field the sender writes.

**Test to prove it.** Probe `test_a_peer_cannot_make_another_member_leave`: node2 resolves node1's daemon and tells `Leave(node3)`. It fails today with `AssertionError: removed`.

---

### [CLUS-11] Over TLS the 32-connection cap and the 30 s budget do not cover the handshake
**Severity:** Medium
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/cluster/management.py:232-238, 245-278`, `docs/security.md` ("The cap keeps the cost of a flood fixed"), `settings.py:259-262`
**Issue:** #227

**What's wrong.** With `ssl=context`, `asyncio.start_server` performs the TLS handshake before it calls `_on_connection`. A connection that opens TCP and never sends a ClientHello therefore never reaches the cap check and never enters `_connections`. It is held by asyncio until `ssl_handshake_timeout` (60 s by default), and nothing in this module owns it. A client can open as many as the file-descriptor limit allows. The remoting listener shares that limit, because it runs in the same process.

**Why it matters.** The cap exists to protect the event loop that answers heartbeats (management.py:80-84). Over TLS the cap does not apply. The docs say the cost of a flood is fixed, and over TLS it is not.

**Proposed fix.** Accept plaintext, and upgrade inside the owned, counted, deadline-bound task:

```python
self._server = await asyncio.start_server(self._on_connection, sock=self._listener, limit=_MAX_HEADER_BYTES)
...
async def _handle(self, reader, writer):
    try:
        async with asyncio.timeout(_REQUEST_TIMEOUT):
            if self._ssl is not None:
                await writer.start_tls(self._ssl)   # Python 3.11+
            status, body = await self._answer(reader)
```

Rejected alternative: lowering `ssl_handshake_timeout` only. That still leaves the connections uncounted and unowned.

**Test to prove it.** Probe `test_the_connection_cap_holds_over_tls`: 40 idle TCP connections, then a 41st real TLS request. It should get a `503`, and today it gets `200 OK`.

---

### [CLUS-12] A management certificate that cannot be loaded leaves a bound port that never answers, silently
**Severity:** Medium
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/cluster/management.py:224-238, 433-438`
**Issue:** #228

**What's wrong.** `server_ssl_context(self._tls)` runs inside the `_serve` task, not at construction. A missing or bad `certfile` raises `OSError` in that task. Nobody awaits the task until `_close`, which suppresses `OSError` (`ssl.SSLError` is a subclass). The socket stays bound and listening, so the kernel accepts connections that nobody ever reads. No log line is produced.

**Why it matters.** `Cluster(...)` succeeds. An operator's `tapio-cluster status` hangs until its 10 s timeout, with nothing in the node's log to explain why. The module's own reasoning ("fail the whole construction rather than leaving a daemon running beside a bind that never happened", cluster.py:113-115) applies equally here. Remoting's `endpoint._serve` has the same shape (endpoint.py:226-233), for its reviewer.

**Proposed fix.** Build the context in `open_management_listener` (or in `ClusterManagement.__init__`) and pass it in. The existing `except BaseException` in `Cluster.__init__` already closes the listener on failure.

**Test to prove it.** Probe `test_a_bad_management_certificate_fails_construction`. It fails today with `construction succeeded; the port answered None`.

---

### [CLUS-13] `when_downed`, `terminate_on_down` and `ClusterDowned` say only a strategy downs a node; an operator does too
**Severity:** Medium
**Category:** Docs
**Status:** Confirmed
**Location:** `src/tapio/cluster/cluster.py:83-90, 508-527`, `messages.py:220-230`, `daemon.py:1063-1071`
**Issue:** #237
**Related:** PERI-3 is the code half of this (the flag is ignored without a strategy). Fix the two together.

**What's wrong.** `when_downed`: "It never returns when downing is switched off, since nothing then downs this node". `terminate_on_down`: "It has no effect without a `downing` strategy, since nothing then downs this node". Both are false. `POST /down` on any node (or `Down` from any node's management port) downs this node with no strategy configured. In a 3-node run with `downing=None`, the target's `downed` event was set. With no strategy, `terminate_on_down=True` is silently ignored (cluster.py:200), so an operator down leaves the process running. The `ClusterDowned.detail` text always says "was on the losing side of a split", including for an operator down.

**Why it matters.** An application that sets `terminate_on_down=True` without a strategy, and relies on the operator escape hatch the docs recommend for exactly that case (clustering.md:412-419), expects the downed node to stop. It does not.

**Proposed fix.** Wire `terminate_on_down` regardless of `downing`. Then replace the docs with: "Returns once this node is downed, by a strategy on either side of a split or by an operator through any node's management port." Make the detail text neutral: f"{address} was downed. A downed member may not rejoin as itself, so the system should be shut down."

**Test to prove it.** In the CLUS-7 probe, use 3 nodes, `downing=None` and `terminate_on_down=True`, then assert `system.is_terminating` after `POST /down`.

---

### [PERI-9] `examples/README.md` omits six of the 28 examples and says every tier has landed
**Severity:** Medium
**Category:** Docs
**Status:** Confirmed
**Location:** `examples/README.md:21-73`
**Issue:** #238

**What's wrong**
Not listed: `blocking_offload`, `cluster_join`, `cluster_management`, `cluster_singleton`, `rolling_restart`, `split_brain`. Each exists in `examples/tapio_examples/` and is asserted in `tests/examples/test_suite.py`. The page also claims every example "finishes in under 2 seconds"; `split_brain` takes 3.54 s and `rolling_restart` 1.08 s in the examples suite (`--durations`).

**Why it matters**
The README is the suggested reading order, and the whole clustering surface plus the blocking-call footgun are missing from it.

**Proposed fix**
Add a row for `blocking_offload` under Tier 3 ("`ctx.run_blocking`, and the damage a blocking call does to every other actor, counted"), and a "Tier 6: Clustering" table with `cluster_join`, `cluster_management`, `cluster_singleton`, `rolling_restart` and `split_brain`. Change "finishes in under 2 seconds" to "finishes in a few seconds".

**Test to prove it**
Add to `tests/examples/test_suite.py`:

```python
def test_every_example_is_in_the_readme():
    readme = (Path(tapio_examples.__file__).parents[1] / "README.md").read_text()
    missing = sorted(m for m in ASSERTED if f"`{m}`" not in readme)
    assert missing == []
```
Fails today with the six names.

---

### [PERI-10] `RoundRobin` claims a shrink never hands the last routee more work, and its own test asserts the opposite
**Severity:** Medium
**Category:** Docs
**Also touches:** Docs and Quality
**Status:** Confirmed
**Location:** `src/tapio/actor/router.py:67-85`; `tests/actor/test_router.py:211-226`
**Issue:** #238

**What's wrong**
Docstring: "removing a dead routee shifts the rotation instead of restarting it. An actor that has just received work does not receive more straight away because the pool shrank." With `[r1, r2, r3]` after three sends (`r3` got the last), removing `r2` gives `routees[3 % 2] = r3`, which is the actor that just received work. `test_the_rotation_survives_the_pool_shrinking` asserts `strategy.select(two, ...) is two[1]` (that is `r3`) under a docstring repeating the false claim.

**Why it matters**
The test enshrines behaviour that contradicts the stated invariant, so a fix to the code would be "caught" as a regression. It is a fairness wobble, not a correctness bug.

**Proposed fix**
Either drop the sentence ("A counter modulo the current size: after a shrink the rotation continues from an arbitrary point.") and rename the test to what it checks, or keep the promise by remembering the last routee's path and advancing from its successor. The first is honest and cheap.

**Test to prove it** (probe)

```python
def test_round_robin_gives_the_same_routee_twice_after_a_shrink():
    strategy = RoundRobin()
    refs = [ActorRef[Job](ActorPath.root("t").child("user").child(f"r{n}")) for n in (1, 2, 3)]
    last = [strategy.select(refs, Job(item=0)) for _ in range(3)][-1]
    assert strategy.select([refs[0], refs[2]], Job(item=0)) is not last
```
```
E       AssertionError: assert ActorRef('tapio://t/user/r3') is not ActorRef('tapio://t/user/r3')
```

---

### [PERI-11] A released `AdapterRef` publishes dead letters on the sender's thread
**Severity:** Medium
**Category:** Bug
**Status:** Confirmed (dead letters), Suspected (`release` itself)
**Location:** `src/tapio/actor/adapter.py:164-188`, `:224-230`, `:258-261`
**Issue:** #229

**What's wrong**
`AdapterRef`'s docstring: "it is safe to use from any thread". The live path hops to the loop with `call_soon_threadsafe`. The released path does not:

```python
        self._validate(message)
        if self._released:
            self._released_letter(message)   # dead_letters.publish(...) right here
            return
```

`DeadLetterOffice.publish` increments `_total`, runs every subscriber, and updates the throttle counters, all on the foreign thread and concurrently with the loop. `release()` has the same shape: `self._cell.release_adapter(self.path)` mutates the cell's `AdapterRegistry._paths` set and the system `RefRegistry` dict off the loop, which can race `release_all()` iterating `_paths` during the owner's termination.

**Why it matters**
Subscribers are written as loop code (the docs subscribe a `TestProbe.tell`, a list `append`, or an `EventStream` handler). Running them on an arbitrary thread breaks their assumptions, and the dead-letter counters can lose updates. The probe saw the subscriber run on `foreign-sender`.

**Proposed fix**

```python
    def _released_letter(self, message: Message) -> None:
        runtime = self._cell.runtime
        publish = functools.partial(
            runtime.dead_letters.publish, message, self.path, DeadLetterReason.ADAPTER_RELEASED
        )
        if runtime.dispatcher.is_current():
            publish()
            return
        with contextlib.suppress(RuntimeError):  # loop closed: nobody left to tell
            runtime.dispatcher.call_soon_threadsafe(publish)
```

and the same hop in `release()` for the registry mutation. Rejected: a lock in `DeadLetterOffice`. It would not make subscribers loop-safe.

**Test to prove it** (probe)

```python
async def test_a_released_adapter_publishes_dead_letters_on_the_callers_thread():
    ...
    adapter.release()
    threads: list[str] = []
    system.dead_letters.subscribe(lambda letter: threads.append(threading.current_thread().name))
    sender = threading.Thread(target=lambda: adapter.tell(Reply(n=1)), name="foreign-sender")
    sender.start(); sender.join()
    await eventually(lambda: bool(threads))
    assert threads == ["MainThread"]
```
```
E           AssertionError: ['foreign-sender']
```

---

### [PERI-12] An adapter made in `setup` leaves one registry entry per restart, not one per actor
**Severity:** Medium
**Category:** Bug
**Also touches:** Bug and Docs
**Status:** Confirmed
**Location:** `src/tapio/actor/adapter.py:117-122`, `:276-290`; `src/tapio/actor/cell.py:693-697`
**Issue:** #229

**What's wrong**
`message_adapter`'s docstring: "An adapter per protocol, made in `setup`, costs one registry entry for the life of the actor and is what most actors want." Adapters are bound to the actor, not the incarnation, and `setup` re-runs on every restart, so every restart registers a new `$adapter-N` and keeps the old one. The old adapters also keep translating with the closure of the incarnation that made them.

**Why it matters**
Under `SupervisorStrategy.restart()` with no `max_restarts`, a flapping actor grows the system ref registry without bound. A peer holding an old adapter ref keeps reaching the actor through the old incarnation's `adapt`, which may close over state the restart was meant to discard.

**Proposed fix**
Pick one model and document it. The smaller change is to say the truth: "Each call registers a new adapter, and `setup` runs again on every restart, so an adapter made in `setup` costs one entry per incarnation. Release the previous one in `PreRestart`, or make the adapter outside the part a restart re-runs." The fuller fix is to release adapters created by an incarnation when that incarnation is torn down, and keep only those made outside `setup`. That changes the "a restart does not turn replies into dead letters" guarantee, so it is a decision for the owner.

**Test to prove it** (probe)

```python
async def test_an_adapter_made_in_setup_accumulates_one_entry_per_restart():
    ...  # setup makes one adapter; supervise with restart(); tell Fail() five times
    await eventually(lambda: len(setups) == 6)
    assert len(ref.cell._adapters.paths) == 1
```
```
E           assert 6 == 1
```

---

### [PERI-13] The blocking pool finds its threads by name, so a second system with the same name waits on the first one's threads
**Severity:** Medium
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/dispatch/blocking.py:70-75`, `:102-113`, `:170-179`
**Issue:** #230

**What's wrong**

```python
        self._prefix = f"tapio-blocking-{system}"
        self._match = f"{self._prefix}_"
    ...
            for thread in threading.enumerate()
            if thread.name.startswith(self._match) and thread.is_alive()
```

System names are not unique in a process. Two `ActorSystem("orders")` (two app instances, two test fixtures, or the cell default `BlockingPool(system="test")` beside the `"test"` system fixture) share a prefix. The prefix case (`orders` vs `orders-eu`) is handled and tested; equal names are not. `shutdown` then polls the other system's idle worker threads until the deadline and logs a false warning.

**Why it matters**
`terminate()` of an idle system takes the full `shutdown_timeout` and logs "1 blocking call(s) were still running at the shutdown deadline", naming a thread that belongs to someone else. `threads` (documented as "a test has to be able to assert that shutdown left nothing running") reports threads it does not own.

**Proposed fix**
Track the executor's own threads instead of matching names: `executor._threads` is private, so keep a reference to the workers by wrapping the initializer:

```python
    def _register(self) -> None:
        self._owned.add(threading.current_thread())   # initializer, runs in each worker

    ThreadPoolExecutor(max_workers=..., thread_name_prefix=..., initializer=self._register)

    @property
    def threads(self):
        return tuple(t for t in self._owned if t.is_alive())
```

(`_owned` is a `weakref.WeakSet`, written from worker threads; a `set` guarded by a lock is fine too.) Rejected: adding the system uid to the prefix. It works but keeps identity in a string.

**Test to prove it** (probe)

```python
async def test_pool_shutdown_waits_on_another_same_named_systems_threads(caplog):
    settings = IsolatedTapioSettings(shutdown_timeout=timedelta(milliseconds=500))
    first = ActorSystem("same", settings); second = ActorSystem("same", settings)
    await first.blocking.submit(asyncio.get_running_loop(), lambda: None)
    await second.blocking.submit(asyncio.get_running_loop(), lambda: None)
    started = time.perf_counter()
    await second.terminate()
    took = time.perf_counter() - started
    await first.terminate()
    assert "still running" not in caplog.text
    assert took < 0.25
```
```
E       AssertionError: WARNING  tapio.blocking:blocking.py:164 1 blocking call(s) were still running at the shutdown deadline and cannot be interrupted: tapio-blocking-same_0
```

---

### [PERI-14] Settings accept values that fail later, inside an actor
**Severity:** Medium
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/settings.py:323`, `:336-361`
**Issue:** #231

**What's wrong**
`blocking_pool_size: int = 16` has no bound. `0` is accepted, and the first `ctx.run_blocking` raises `ValueError("max_workers must be greater than 0")` from `ThreadPoolExecutor` inside whichever actor blocked first, where supervision treats it as that actor's failure. `default_mailbox_capacity=0` is accepted and every later spawn raises from `MailboxConfig.__post_init__`. Negative `shutdown_timeout`, `ask_timeout`, `dead_letter_log_first` and `dead_letter_summary_interval` are accepted too. `ClusterSettings` already uses `Annotated[int, Field(ge=1)]`, so the pattern exists.

**Why it matters**
`TAPIO_BLOCKING_POOL_SIZE=0` in a deployment surfaces as an actor failure on the first database call, far from the configuration.

**Proposed fix**

```python
    default_mailbox_capacity: Annotated[int, Field(ge=1)] | None = None
    blocking_pool_size: Annotated[int, Field(ge=1)] = 16
    dead_letter_log_first: Annotated[int, Field(ge=0)] = 10
```

and a `field_validator` rejecting non-positive timedeltas for the four durations.

**Test to prove it** (probes)

```python
def test_blocking_pool_size_zero_is_refused():
    with pytest.raises(ValidationError):
        IsolatedTapioSettings(blocking_pool_size=0)

def test_default_mailbox_capacity_zero_is_refused():
    with pytest.raises(ValidationError):
        IsolatedTapioSettings(default_mailbox_capacity=0)
```
Both fail today with `Failed: DID NOT RAISE ValidationError`.

---

### [PERI-15] "Every error tapio raises derives from `TapioError`" is false; an invalid actor name raises `ValueError`
**Severity:** Medium
**Category:** Docs
**Also touches:** Docs and Bug
**Status:** Confirmed
**Location:** `src/tapio/errors.py:1-11`, `:61-66`; `src/tapio/actor/path.py:33-48`
**Issue:** #238

**What's wrong**
The module docstring promises one `except TapioError` catches the library. `system.spawn(b, "bad name")` raises a bare `ValueError` from `ActorPath.__post_init__`, not `ActorNameError` ("A child could not be given the name it asked for"). So do `ActorSystem("bad name")`, `Routers.pool(0, ...)`, `MailboxConfig(capacity=0)`, `AdapterRef.offer` off-loop (`RuntimeError`) and `BlockingPool.submit` after shutdown (`RuntimeError`).

**Why it matters**
A caller spawning a name built from user input catches `ActorNameError` or `TapioError`, as the docs tell them to, and the `ValueError` escapes.

**Proposed fix**
Raise `ActorNameError` for an invalid name in `cell.spawn`/`system.spawn` (validate before building the path), and add the other cases to a `ConfigurationError(TapioError)` or say plainly in the module docstring: "Programming errors in arguments raise the builtin `ValueError` or `RuntimeError`; everything about the runtime's state derives from `TapioError`."

**Test to prove it** (probe)

```python
async def test_every_error_tapio_raises_derives_from_tapio_error():
    system = ActorSystem("errs", IsolatedTapioSettings())
    try:
        with pytest.raises(ActorNameError):
            system.spawn(Behaviors.receive_message(_same, msg_type=Job), "bad name")
    finally:
        await system.terminate()
```
```
E               ValueError: invalid actor name 'bad name': names must start with a letter or digit ...
```

---

### [PERI-16] Runtime internals are exported from `tapio.actor` and `tapio.cluster`, and the top level is inconsistent
**Severity:** Medium
**Category:** Quality
**Status:** Confirmed
**Location:** `src/tapio/actor/__init__.py:44-90`; `src/tapio/cluster/__init__.py:151-188`; `src/tapio/__init__.py:196-260`
**Issue:** #232

**What's wrong**
- `tapio.actor.__all__` exports `ActorCell`, `LocalActorRef`, `Mailbox`, `Envelope`, `DeadLetterOffice`, `DeadLetterRef`, `PeerResolver`, `Watcher`, `Directive`, `ReceivingBehavior`, `SetupBehavior`, `Supervise`, `SuperviseBehavior`, `WithStashBehavior`, `WithTimersBehavior`, `UnstashBehavior`. No example imports any of them. Only tests use `LocalActorRef`, to reach `ref.cell`.
- `tapio.cluster.__all__` exports the wire protocol (`GossipEnvelope`, `Heartbeat`, `HeartbeatReply`, `Join`, `Leave`, `WireMessage`), the merge state (`Gossip`, `Reachability*`, `VectorClock`, `Ordering`) and the ring (`RingMonitor`, `monitored_by`). It does not export `ClusterSettings`, `ManagementSettings` or `ClusterError`, which every cluster example needs (they import from `tapio.settings`).
- The top level exports `RemoteSettings` and `TLSSettings` but not `ClusterSettings`, `ManagementSettings` or `ClusterError`.
- `tapio.Spawned` (a remote spawn reply) and `tapio.testkit.Spawned` (a kit effect) are different classes with one name.

**Why it matters**
Every exported name is a compatibility promise under semantic-release. Exporting `ActorCell` invites `ref.cell.abort()`, which AGENTS.md tells people not to do. Exporting wire messages lets an application `tell` a `Join` to the daemon.

**Proposed fix**
Trim `tapio.actor.__all__` to the user surface (refs, paths, behaviors, context, signals, supervision, mailbox config, dead letters, timers, stash, routers, system, `ask`) and keep internals importable from their modules. Move the cluster wire messages and merge types out of `tapio.cluster.__all__` (keep them importable from their modules for tests and the reference page). Add `ClusterSettings`, `ManagementSettings` and `ClusterError` to `tapio.cluster` and `ClusterError` to `tapio`. Rename the kit effect to `SpawnedChild` with a deprecated alias.

**Test to prove it**
`tests/test_package.py` could pin the surface: `assert set(tapio.actor.__all__) == EXPECTED_ACTOR_SURFACE`. Fails today on the extra names.

---

### [PERI-17] `BlockingPool` and `Dispatcher` are public but do not say what a caller must not do
**Severity:** Medium
**Category:** Docs
**Status:** Confirmed
**Location:** `src/tapio/dispatch/__init__.py:10`; `src/tapio/dispatch/blocking.py:57-58`, `:115-147`; `src/tapio/dispatch/dispatcher.py:20-50`
**Issue:** #238

**What's wrong**
Both are in `tapio.dispatch.__all__` and reachable as `system.blocking` and `system.dispatcher` (the cluster uses `system.dispatcher.spawn_task`). `BlockingPool` is documented as "The threads one system runs blocking calls on." `submit` does not say it must be called on the loop thread (it lazily creates the executor and calls `loop.run_in_executor`, neither thread-safe), nor that `shutdown` belongs to the system and calling it early refuses every later `run_blocking`. `Dispatcher.spawn_task` is "Start a named task on this system's loop" with no word that the task is owned by nobody: no cell cancels it in a termination sequence, which is the AGENTS.md invariant ("If you add a task, say which cell owns it").

**Why it matters**
These are the two objects whose misuse breaks the leak invariants the testkit asserts.

**Proposed fix**
`BlockingPool` class docstring: "Owned and shut down by its system. Call `submit` only from the system's loop; use `ctx.run_blocking` from an actor. Do not call `shutdown` yourself: it refuses every later blocking call in the system." `Dispatcher.spawn_task`: "The task is not owned by any cell, so nothing cancels it at shutdown. The caller must hold it and cancel or await it before the system terminates; actor code should not use this."

**Test to prove it**
Docs-only.

---

### [PERI-18] `cancel_and_wait` treats a cancellation from before the call as aimed at this wait
**Severity:** Medium
**Category:** Bug
**Status:** Confirmed (helper), Suspected (reachable from today's call sites)
**Location:** `src/tapio/dispatch/tasks.py:35-38`
**Issue:** #233

**What's wrong**

```python
    except asyncio.CancelledError:
        current = asyncio.current_task()
        if current is not None and current.cancelling():
            raise
```

`cancelling()` is a running count of requests not yet `uncancel()`ed. A caller that caught an earlier cancellation and went on into cleanup (the common `except CancelledError: await handle.close(); raise` shape) still has `cancelling() >= 1`, so the retired task's own `CancelledError` is re-raised and the caller's cleanup stops at the first `cancel_and_wait`. The docstring promises it "re-raises a cancellation aimed at the caller".

**Why it matters**
In `RemoteHandle.close` the `finally` still closes the link, so the damage today is limited to the exception changing. A future call site that resumes work after the wait (the case the helper exists for) stops early. I did not find a current call site whose caller is already cancelling, so the impact is Suspected.

**Proposed fix**
Compare against the count on entry, as in PERI-1's sketch:

```python
    current = asyncio.current_task()
    before = current.cancelling() if current is not None else 0
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        if current is not None and current.cancelling() > before:
            raise
    except Exception:
        pass
```

**Test to prove it** (probe)

```python
async def test_cancel_and_wait_reraises_a_cancellation_from_before_the_call():
    outcome: list[str] = []
    async def caller() -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            pass
        try:
            await cancel_and_wait(asyncio.ensure_future(asyncio.sleep(10)))
        except asyncio.CancelledError:
            outcome.append("re-raised"); return
        outcome.append("returned")
    task = asyncio.ensure_future(caller()); await asyncio.sleep(0); task.cancel(); await task
    assert outcome == ["returned"]
```
```
E       AssertionError: assert ['re-raised'] == ['returned']
```

(A second probe, a task that swallows the caller's cancellation, passed: on CPython 3.13 the outer cancellation is not lost. Not a finding.)

---

### [PERI-19] `BehaviorTestKit` reads `TAPIO_*` from the environment by default
**Severity:** Medium
**Category:** Quality
**Status:** Confirmed
**Location:** `src/tapio/testkit/behavior.py:261-282`
**Issue:** #234

**What's wrong**
`self._settings = settings if settings is not None else TapioSettings()`. `tapio.testkit.settings` exists, by its own module docstring, because "a developer with `TAPIO_VALIDATE_ON_TELL=0` exported runs a different suite from everyone else and nothing says so". The `actor_system` fixture uses `IsolatedTapioSettings()`; the kit, in the same package, does not. tapio's own suite hides this with a session-wide autouse fixture that deletes `TAPIO_*` (`tests/conftest.py:99`), which a user's suite does not have.

**Why it matters**
In a user's project, `TAPIO_VALIDATE_ON_TELL=0` silently switches off the content validation that `kit.run` documents as "validated against the declared message type first, exactly as delivery would".

**Proposed fix**
Default to `IsolatedTapioSettings()` and change the docstring to "Isolated from the environment when omitted, like the fixtures."

**Test to prove it**

```python
def test_the_kit_ignores_the_environment(monkeypatch):
    monkeypatch.setenv("TAPIO_VALIDATE_ON_TELL", "0")
    kit = BehaviorTestKit(counter())
    assert kit._settings.validate_on_tell is True
```

---

### [PERI-20] "Every code block is a snippet include from `examples/`" is false, and the broken blocks are the ones that are not
**Severity:** Medium
**Category:** Docs
**Status:** Confirmed
**Location:** `docs/index.md:31-32`; `mkdocs.yml:25`; `docs/getting-started.md:4-5`
**Issue:** #238

**What's wrong**
`index.md`: "Every code block on this site is a snippet include from `examples/`, so nothing documented here is unexecuted." `mkdocs.yml`: "Docs never copy-paste code". `docs/clustering.md` has seven inline Python blocks (`join_seed_nodes`, `Cluster(system, ClusterSettings(...))`, `cluster.subscribe`, the group router, two `ManagementSettings` blocks), `docs/remoting.md` an inline frame, and `docs/testing.md` includes from `tests/docs/`, not `examples/`. PERI-5 and PERI-7 are both in blocks this claim says cannot exist.

**Why it matters**
The claim tells a reader not to doubt the snippets, and two of the uncovered ones are wrong.

**Proposed fix**
Move the clustering inline blocks into `tests/docs/test_clustering_page.py` with `--8<--` sections, as `docs/testing.md` already does, and change the sentence to "Every code block on this site is included from a file CI runs: `examples/` or `tests/docs/`." Until then, say "Most code blocks...".

**Test to prove it**
A docs lint in `tests/docs/`: parse every `docs/*.md` and fail on a fenced `python` block that is not a single `--8<--` line. Fails today on `clustering.md`.

---

### [PERI-21] README contradicts itself on validation cost, and names overflow strategies by values the setting rejects
**Severity:** Medium
**Category:** Docs
**Status:** Confirmed
**Location:** `README.md:173-186`, `:213-218`
**Issue:** #238

**What's wrong**
Design notes: "about 30% more per message for a one-field message, and about 3x for a ten-field one". The table below: 1.4x and 2.4x. And: "Bounded mailboxes take an overflow strategy (`fail`, `drop_new`, `drop_oldest`)". The enum values are `"fail"`, `"drop-new"`, `"drop-oldest"`, so `TAPIO_DEFAULT_MAILBOX_OVERFLOW=drop_new` fails validation at startup.

**Why it matters**
The cost numbers are the README's headline argument. The overflow names are the spelling a reader would put in an environment variable.

**Proposed fix**
"about 40% more per message for a one-field message, and about 2.4x for a ten-field one with nested models", and "(`FAIL`, `DROP_NEW`, `DROP_OLDEST`; in an environment variable `fail`, `drop-new`, `drop-oldest`)".

**Test to prove it** (probe)

```python
def test_readme_overflow_names_are_the_setting_values():
    IsolatedTapioSettings(default_mailbox_overflow="drop_new")
```
```
E       Input should be 'fail', 'drop-new' or 'drop-oldest' [type=enum, input_value='drop_new', input_type=str]
```

---

### [PERI-22] The suite's `system` fixture does not assert the leak invariant that AGENTS.md says every system-starting test asserts
**Severity:** Medium
**Category:** Quality
**Also touches:** Quality (tests)
**Status:** Confirmed (gap), no current leak
**Location:** `tests/conftest.py:155-162`
**Issue:** #239

**What's wrong**
AGENTS.md: "Anything that starts a system wraps itself in `tapio.testkit.assert_no_leaked_tasks()`." The plugin's `actor_system` does (`testkit/plugin.py:213`). The suite's own `system` fixture only terminates. About 161 test functions take `system: ActorSystem`, so none of them checks for leaked tasks or threads.

I ran the whole suite (894 tests) with an autouse leak-checking plugin (`probes/periphery/leakplugin/leakcheck.py`). It found no leaked task. The only new threads were `asyncio_0` default-executor threads that tests create on purpose with `asyncio.to_thread`. So nothing leaks today; the guard is just missing.

**Why it matters**
A runtime change that orphans a task passes the 161 tests that would most likely see it.

**Proposed fix**

```python
@pytest.fixture
async def system(settings: TapioSettings) -> AsyncIterator[ActorSystem]:
    """A running system, terminated however the test ends, and checked for leaks."""
    with assert_no_leaked_tasks():
        running = ActorSystem("test", settings)
        try:
            yield running
        finally:
            await running.terminate()
```

(Leave threads out, or the tests that use `asyncio.to_thread` need their own exemption.)

**Test to prove it**
A test in `tests/test_conftest.py` that uses `system`, creates an orphan `asyncio.create_task(asyncio.sleep(30))` and expects the fixture teardown to fail (`pytester`).

---

### [PERI-23] Tests use `asyncio.sleep` as synchronisation where an `eventually` or a probe is available
**Severity:** Medium
**Category:** Quality
**Also touches:** Quality (tests)
**Status:** Confirmed
**Location:** `tests/actor/test_system.py:90`, `:134-137`; `tests/actor/test_delivery.py:110`, `:133`; `tests/actor/test_dead_letters.py:212`, `:237`, `:288`, `:387`
**Issue:** #239

**What's wrong**
Each waits a fixed 10 to 50 ms and then asserts something the runtime does asynchronously. Examples:
- `test_system.py:90`: `first.tell(Increment()); await asyncio.sleep(0.01); second = system.spawn(idle(), name="worker")`. On a slow runner the first actor has not stopped yet and the spawn raises `ActorNameError`.
- `test_delivery.py:110`: sleeps 10 ms for a stop, then asserts a dead-letter log line.
- `test_dead_letters.py:212`: `released.set(); await asyncio.sleep(0.05)` then asserts the dead letters.

AGENTS.md and `docs/testing.md:272-278` both say to wait for the effect rather than guess a duration.

**Why it matters**
These are the flaky-on-CI shape, and each one costs its sleep on every run.

**Proposed fix**
`await eventually(lambda: not _alive(first))` (or watch with a probe and `expect_terminated`) before the respawn; `await eventually(lambda: len(seen) == 3)` before the dead-letter assertions; `await eventually(lambda: "scripted failure" in caplog.text)`.
The sleeps that measure real time (`test_supervision.py:224`, `:620`, the restart-window tests) and the ones proving a negative (`test_death_watch.py:88`) are legitimate and should stay.

**Test to prove it**
Run the listed tests under CPU contention (`stress-ng --cpu 0` or `pytest -n 16` on a small runner). They are the ones that fail.

---


## Low

### [CORE-8] The shutdown-deadline warning always says "handling no message"
**Severity:** Low
**Category:** Bug
**Status:** Confirmed (log line in the CORE-1 probe)
**Location:** `src/tapio/actor/cell.py:891-898`
**Issue:** none: Low findings get no issue

**What's wrong.** The warning reads `self._current` after `cancel_and_wait` has returned. By then the cancellation has run `_on_message`'s `finally`, which resets `_current` to `None`. The probe logs `kid#2: did not stop within the shutdown deadline while handling no message; cancelled` for an actor stuck in `Wedge`.

**Why it matters.** `docs/lifecycle.md` presents this warning as how the slow actor is identified. It names the path, but never the message, which is the half that says what it was stuck on.

**Proposed fix.** Read `self._current` before cancelling (included in the CORE-1 diff).

**Test to prove it.** With `caplog`, wedge a handler on message `Wedge`, terminate with a short deadline, and assert `"while handling Wedge"` is in the warning.

---

### [CORE-9] `ActorSystem.resolve` skips the `expect` check for a local address, contrary to its `Raises` section
**Severity:** Low
**Category:** Docs
**Status:** Confirmed (by reading)
**Location:** `src/tapio/actor/system.py:477-482`
**Issue:** none: Low findings get no issue

**What's wrong.** The local branch returns before `normalize_msg_type(expect, ...)` runs:

```python
477        if address.system == self.name and (
478            not address.is_addressable or address == self._address
479        ):
480            return cast("ActorRef[T]", self.resolve_path(address, path))
481
482        msg_type = normalize_msg_type(expect, origin=f"resolving {uri}")
```

The docstring says it raises `MessageTypeError` "If `expect` is not a `Message` subclass or a union of them". For a local address it never does, and it never compares `expect` with the actor's real type either.

**Why it matters.** It is caught later by the `tell` validation, so nothing is unsound at runtime. But a test that resolves its own address with a wrong `expect` passes where the same code against a peer fails.

**Proposed fix.** Normalize `expect` before the branch, and for a live local ref check that every member of `expect` is accepted by `ref.cell.msg_type`, raising `MessageTypeError` otherwise.

**Test to prove it.** `with pytest.raises(MessageTypeError): await system.resolve(str_of(local_ref), expect=int)`.

---

### [WIRE-11] The version check accepts `true` and `1.0`
**Severity:** Low
**Category:** Bug
**Status:** Confirmed (probe fails today)
**Location:** `src/tapio/remote/codec.py:235-240`
**Issue:** none: Low findings get no issue

**What's wrong.** `version != PROTOCOL_VERSION` uses Python equality, so `{"v": true}` and `{"v": 1.0}` pass. `Frame.version` is typed `int` but holds `True`.

**Proposed fix.** `if type(version) is not int or version != PROTOCOL_VERSION:`.

**Test to prove it** (`test_version_true_is_refused`, fails today with `DID NOT RAISE MessageDecodingError`).

---

### [WIRE-12] Uid and port parsing accept non-ASCII digits and signs
**Severity:** Low
**Category:** Bug
**Status:** Confirmed (probe fails today)
**Location:** `src/tapio/remote/address.py:37-42`, `src/tapio/remote/codec.py:437-439`
**Issue:** none: Low findings get no issue

**What's wrong.** `\d` in a `str` regex matches any Unicode decimal digit, and `int()` converts them, so `tapio://s@h:1/user/x#٣` parses as uid 3. `parse_target` uses `int(fragment or 0)`, which also accepts `+3`, ` 3` and `1_0`. One ref therefore has many string forms.

**Proposed fix.** Compile both regexes with `re.ASCII`. In `parse_target`, require `fragment.isascii() and fragment.isdigit()` before `int()`.

**Test to prove it** (`test_uid_digits_are_ascii_only`, fails today with `DID NOT RAISE ValueError`).

---

### [WIRE-13] Registering one class under a second key silently changes the key it is sent under
**Severity:** Low
**Category:** Quality
**Status:** Confirmed (by reading)
**Location:** `src/tapio/remote/registry.py:203-213`
**Issue:** none: Low findings get no issue

**What's wrong.** The duplicate check is per key only. `register_message("new")` applied to a class already registered as `"old"` overwrites `_BY_TYPE[cls]`, so every later `encode` writes `"new"`, and a peer on the previous release dead-letters it. Decorators apply bottom-up, so which key wins depends on the order they are stacked in. The module docstring promises that "a duplicate key raises at import rather than winning", but a duplicate *type* does win.

**Proposed fix.** Raise when `cls in _BY_TYPE` with a different key. To accept an old key on the way in, add an explicit `aliases=` parameter that writes only `_BY_KEY`.

**Test to prove it.**

```python
def test_a_second_key_for_one_type_is_refused():
    @register_message("probe.a")
    class A(Message): ...
    with pytest.raises(MessageRegistrationError):
        register_message("probe.b")(A)
```

---

### [WIRE-14] `decode` parses every payload three times and rounds numbers through `float`
**Severity:** Low
**Category:** Quality
**Status:** Confirmed (by reading)
**Location:** `src/tapio/remote/codec.py:229`, `256`, `328`
**Issue:** none: Low findings get no issue

**What's wrong.** `json.loads` parses the whole body, `json.dumps(payload)` re-serializes the payload, and `model_validate_json` parses it again. The splice in `encode` exists ("parsing it just to serialize it again would double the cost of every send") to avoid exactly this on the way out. The detour also means a model validates text that the peer did not send. Numbers pass through Python `float`, so `1.10` arrives as `1.1`, and a `Decimal` field written as a JSON number loses precision.

**Proposed fix.** Validate the header with a small Pydantic model, keep `p` as `Any`, and validate the payload with `msg_type.model_validate(payload)` inside `use_context`. Python-mode lax validation of JSON-native values matches JSON mode for the usual types. Alternative: slice the payload bytes out, since `encode` always writes `"p"` last, but that only holds for frames tapio wrote.

**Test to prove it.** A `Decimal` round-trip test with a custom number serializer, or a benchmark in `tests/benchmarks/`.

---

### [WIRE-15] Smaller docstring and docs inaccuracies
**Severity:** Low
**Category:** Docs
**Status:** Confirmed
**Location:** as listed
**Issue:** none: Low findings get no issue

- `docs/remoting.md`, frame example: see PERI-7.
- `docs/security.md`, "an HMAC of a server-supplied nonce with the shared secret": both sides prove, each over the other side's nonce. Replace with "an HMAC of each side's nonce, computed by the other side with the shared secret".
- `handshake.accept`/`introduce`, `timeout: Seconds allowed for the whole exchange`: it is applied per read, so `introduce` can take twice as long (the outer `asyncio.timeout` in `_dial` saves it). Replace with "Seconds allowed for each frame the peer must send".
- `protocol.PROTOCOL_VERSION`, "It appears in every frame as `v`": link frames (heartbeat, watch) carry no `v`. Replace with "It appears in every message frame as `v`".
- `TLSSettings.cafile`, "a client with it set verifies the server's": a client verifies the server either way, against the system trust store when `cafile` is unset. Replace with "a client checks the server's certificate against it instead of the system trust store".
- `RemoteSettings.max_frame_bytes` is unvalidated. `0` or a negative value refuses every handshake with a "frame too large" message, and a value of `2**32` or more lets `framed`/`encode` raise `OverflowError` instead of `FrameTooLargeError`. Add `Field(gt=0, lt=2**32)`.

---

### [LINK-15] Low findings
**Severity:** Low
**Category:** Quality
**Also touches:** Quality / Docs
**Status:** Confirmed by reading
**Issue:** none: Low findings get no issue

1. `cell.watch` (`cell.py:796-797`) calls `target.add_watcher(self)` before `self._watch.watching(target)`. `PeerOutbox.watch` and `Association.watch` can answer synchronously (no association, or `_write_link` failed), and `notify_terminated` then calls `stop_watching` before the entry exists, which leaves a stale `_watching` entry. Swap the two lines. `Association.watch`'s `_closing` branch (lines 425-430) is unreachable through `PeerOutbox`, since `outbound()` never returns a closing association.
2. `_open` with `_closing` set closes the link and returns, and `_run` then calls `_read` on the closed link. That logs a spurious "link to ... ended" warning (`association.py:951-953`, `859-860`). Return a flag, or raise, so `_run` stops.
3. The spawner reveals what it does not offer. A key that is registered but not offered answers `not-allowed` with the full allowlist in `detail`, while an unregistered key answers `unknown-factory` (`spawner.py:424-446`). A peer can enumerate both. Answer `not-allowed` for any key outside the allowlist, and keep the list out of the reply.
4. `RemoteSettings.heartbeat_interval` says "How often an idle association writes a heartbeat. A link that carries traffic needs none of these". `_beat` writes one every tick regardless of traffic (`association.py:786-790`). Either skip the write when a frame went out within the interval, or reword the docstring to "How often an association writes a heartbeat".
5. The frame example in `docs/remoting.md` uses a hex uid: see PERI-7.
6. `reconnect` can return successfully on a link that the peer closes in the next turn, when the peer's own association wins a dial race (`endpoint.py:385-388`). Its docstring promises a `HandshakeError` when the peer "dropped the link before it carried anything". This is resolved by the fix to LINK-2.
7. `PhiAccrualDetector` keeps running `_sum` and `_sum_sq` by subtracting evicted samples forever, so float error accumulates over a long-lived link (`failure.py:202-208`). Recompute both from the deque every `max_samples` records.

---

### [MEMB-8] The merge laws hold only for canonical values, and nothing enforces that a value is canonical
**Severity:** Low
**Category:** Quality
**Status:** Confirmed (probe)
**Location:** `src/tapio/cluster/gossip.py:50, 299-309`; `src/tapio/cluster/reachability.py:87, 95-151`
**Issue:** none: Low findings get no issue

**What's wrong.** `Gossip.members` and `Reachability.records` accept duplicate keys in any order, from the constructor or from a frame. `merge` deduplicates and sorts, so `x.merge(x) != x` for such a value. `test_idempotent_on_wire_input` fails on two copies of the same member. The suite's strategies deduplicate before building, so they never test this.

Separately, `Reachability.unreachable` and `is_reachable(observers=None)` are public and count dead observers. That is the opposite of `Gossip.unreachable`, and outside `__repr__` they are used only by tests.

**Proposed fix.** Add an `AfterValidator` on both fields that merges duplicates and sorts. The constructor is then the merge's identity, and decoding a frame canonicalises it. Make `observers` required in `is_reachable`, and make `Reachability.unreachable` private, or document that it ignores who is alive.

**Test to prove it:** `test_idempotent_on_wire_input` in `probes/cluster-core/test_merge_laws.py`.

---

### [MEMB-10] The docs say only the leader downs a member, but every node does
**Severity:** Low
**Category:** Docs
**Status:** Confirmed
**Location:** `docs/clustering.md:43`, `src/tapio/settings.py:228-229`
**Issue:** none: Low findings get no issue

**What's wrong.** The status table says `down` is set by "the leader", and `down_after` says "before the leader downs anybody". `_down` runs on every node (`daemon.py:971-981`), and an operator downs through any node (`_down_member`).
**Proposed fix (docs):** table cell "a downing strategy on every node, or an operator through any node". `down_after`: "before any node applies its strategy".

---

### [MEMB-11] "Every member is watched by exactly that many others" is false for small clusters
**Severity:** Low
**Category:** Docs
**Status:** Confirmed
**Location:** `docs/clustering.md:168`, `src/tapio/settings.py:174-176`, `src/tapio/cluster/monitor.py:9-10`
**Issue:** none: Low findings get no issue

**What's wrong.** `monitored_by` returns at most n-1 peers, so with n <= `monitored_peers` each member is watched by n-1 others.
**Proposed fix:** "watched by that many others, or by every other member when the cluster is smaller than that".

---

### [MEMB-12] `ClusterSettings` accepts timings that cannot work
**Severity:** Low
**Category:** Quality
**Status:** Confirmed (probe `test_settings.py`)
**Location:** `src/tapio/settings.py:149-240`
**Issue:** none: Low findings get no issue

**What's wrong.** A zero or negative `gossip_interval`, `heartbeat_interval`, `unreachable_after` or `down_after` is accepted. A negative `down_after` downs on the first unreachable observation. The documented relation "set `unreachable_after` well above `heartbeat_interval`" is not checked either.
**Proposed fix:** `Annotated[timedelta, Field(gt=timedelta(0))]` on every interval, plus a `model_validator` that rejects `unreachable_after <= heartbeat_interval`.

---

### [CLUS-14] The first subscriber misses events produced later in the turn that subscribed it
**Severity:** Low
**Category:** Bug
**Status:** Suspected
**Location:** `src/tapio/cluster/daemon.py:488, 492-493, 519-524`
**Issue:** none: Low findings get no issue

**What's wrong.** `before = self._digest() if self._subscribers else None` is taken before the `Subscribe` is handled. For the first subscriber it is `None`, so `_emit` is skipped. If `_lead` or the time-based `_down` changes the state later in that same turn, the subscriber's replay was taken before that change, and no diff is sent. The obvious case is `down_after` expiring on the subscribe turn, which loses `SelfDown`. Another is a single-node cluster whose leader takes the next leave step on that turn.

**Proposed fix.** `before = self._digest() if self._subscribers or isinstance(message, Subscribe) else None`.

**Test to prove it.** This needs a daemon built with an injected `now` and a fake strategy, so that `down_after` expires exactly on the `Subscribe` turn. Then assert that `SelfDown` reaches the subscriber.

---

### [CLUS-15] The replay leaves out members that are leaving, contradicting "no window in which a late subscriber has missed something"
**Severity:** Low
**Category:** Docs
**Status:** Confirmed (by reading)
**Location:** `src/tapio/cluster/daemon.py:1288-1305`, `docs/clustering.md:286-290`
**Issue:** none: Low findings get no issue

**What's wrong.** `_replay` sends `MemberUp` only for `UP` members. A member in `leaving` or `exiting` is in no replayed event, and `MemberLeaving` is never replayed. A late subscriber cannot tell "leaving" from "never existed" until the removal.

**Proposed fix.** Replay `MemberLeaving` for alive members in `_LEAVING_STATUSES`, after the `MemberUp`s. Alternatively, qualify the docs sentence.

---

### [CLUS-16] The group router adds joining and leaving members when they become reachable again
**Severity:** Low
**Category:** Bug
**Status:** Confirmed (by reading)
**Location:** `src/tapio/cluster/router.py:160-161`, `daemon.py:1128-1137`
**Issue:** none: Low findings get no issue

**What's wrong.** `ReachableMember` is emitted for any member that is not `down` or `removed`, which includes `joining`, `leaving` and `exiting`. The router `_offer`s on it regardless of status, so a `joining` member that flapped gets work before `MemberUp`. That contradicts "A member that joins is added" (on up). A member that is leaving gets added back after it went unreachable.

**Proposed fix.** Make the router's `_offer` on `ReachableMember` require `member.status is MemberStatus.UP`.

---

### [CLUS-17] `timeout=timedelta(0)` silently means "the default"
**Severity:** Low
**Category:** Bug
**Status:** Confirmed (by reading)
**Location:** `src/tapio/cluster/cluster.py:496, 558`
**Issue:** none: Low findings get no issue

**What's wrong.** `(timeout or self._settings.join_timeout)` treats the falsy `timedelta(0)` as omitted. **Proposed fix.** `timeout if timeout is not None else ...`.

---

### [CLUS-18] The first seed is recognised by comparing raw address strings
**Severity:** Low
**Category:** Quality
**Status:** Confirmed (by reading)
**Location:** `src/tapio/cluster/daemon.py:789, 812`, `member.py:27-56`
**Issue:** none: Low findings get no issue

**What's wrong.** `AddressStr` validates but does not normalise. A first seed listed as `localhost` while its canonical host is `127.0.0.1`, or written with different IPv6 spelling, never forms the cluster. It also dials itself as if it were another seed. The `join_seed_nodes` timeout message does not mention this cause.

**Proposed fix.** Normalise in `AddressStr` (`str(Address.parse(text))`), or compare `Address` values, and log a warning when no seed equals this node's canonical address.

---

### [CLUS-19] Smaller docs inaccuracies
**Severity:** Low
**Category:** Docs
**Status:** Confirmed
**Location:** as listed
**Issue:** none: Low findings get no issue

- `docs/clustering.md:43`: the statuses table says `down` is "set by the leader". `_down` runs on every node (daemon.py:972-983), and an operator can down a member from any node.
- `settings.py:261`, `docs/security.md`: "gives each request thirty seconds to arrive and be answered". Reading has 30 s and writing has another 30 s (management.py:296, 314), and `wait_closed()` is unbounded (line 321). Suggested wording: "thirty seconds to arrive, and thirty more to be written".
- `cluster.py:100-101` and `management.py:471`: `Raises: InsecureRemoteConfig: If ... with no token`. Mutual TLS also satisfies the rule. Suggested wording: "with neither a token nor a client certificate requirement".

---

### [CLUS-20] CLI: `--client-key` without `--client-cert` is ignored, and the token only travels on the command line
**Severity:** Low
**Category:** Quality
**Status:** Confirmed (by reading)
**Location:** `src/tapio/cluster/cli.py:87-90, 113-127, 244-245`
**Issue:** none: Low findings get no issue

**What's wrong.** `--client-key` alone neither enables TLS nor errors. `--token` has no `envvar`, so the operator's secret is visible in `ps` output and in shell history. The CLI's routes and status codes do match the server (`GET /status` gives 200, `POST /leave|/down` gives 202, errors carry `{"error": ...}`).

**Proposed fix.** `typer.Option(envvar="TAPIO_MANAGEMENT_TOKEN")`, and fail with a usage error when `--client-key` is given without `--client-cert`.

---

### [PERI-24] The examples completeness check compares against a hand-kept set, not the tests
**Severity:** Low
**Category:** Quality
**Also touches:** Quality (tests)
**Status:** Confirmed
**Location:** `tests/examples/test_suite.py:42-71`, `:538-541`
**Issue:** none: Low findings get no issue

**What's wrong**
`test_every_example_is_asserted` checks `modules == ASSERTED`, where `ASSERTED` is a literal set. Adding a module name to the set with no `test_<name>` function passes. `test_blocking_offload` starts a thread pool but only checks for leaked tasks, not threads.

**Proposed fix**

```python
def test_every_example_is_asserted():
    modules = {m.name for m in pkgutil.iter_modules(tapio_examples.__path__)}
    tested = {name.removeprefix("test_") for name in globals() if name.startswith("test_")}
    assert modules <= tested, sorted(modules - tested)
```

and wrap `test_blocking_offload` in `assert_no_leaked_threads()` too.

**Test to prove it**
Add `"ghost"` to `ASSERTED` and a `ghost.py` example with no test. Today the suite passes; after the fix it fails.

---

### [PERI-25] `describe_callable` says a lambda falls back to `repr`; it does not
**Severity:** Low
**Category:** Docs
**Status:** Confirmed
**Location:** `src/tapio/logging.py:65-83`
**Issue:** none: Low findings get no issue

**What's wrong**
"Anything without one, a lambda included, falls back to `repr`." A lambda has `__qualname__` (`f.<locals>.<lambda>`), so it is named by that.

**Proposed fix**
"A function, a lambda included, gives its `__qualname__`, such as `build.<locals>.<lambda>`. Anything without one, a `functools.partial` for instance, falls back to `repr`."

**Test to prove it** (probe)
`assert describe_callable(f) == repr(f)` fails with `'test_...<locals>.<lambda>' == '<function ...>'`.

---

### [PERI-26] `docs/blocking.md` misdescribes the thread check and omits that a wedged call blocks interpreter exit
**Severity:** Low
**Category:** Docs
**Status:** Confirmed
**Location:** `docs/blocking.md:74-78`, `:45-50`; `src/tapio/dispatch/blocking.py:29-34`
**Issue:** none: Low findings get no issue

**What's wrong**
"`assert_no_leaked_threads()` is the companion, and `system.blocking.threads` is what it reads". It reads `threading.enumerate()` (`testkit/leaks.py:129-133`). And "past the deadline it logs what is still running and gives up on it": `ThreadPoolExecutor` workers are joined at interpreter exit, so a wedged call with no timeout also stops the process from exiting.

**Proposed fix**
"`assert_no_leaked_threads()` is the companion: it fails if any thread started inside the block is still alive, and `system.blocking.threads` lists the pool's own." Add: "Python joins pool threads at interpreter exit, so a call still running then also holds the process open until it returns."

---

### [PERI-27] `TestProbe.expect_terminated` takes whichever signal is next, and loses it on a mismatch
**Severity:** Low
**Category:** Quality
**Status:** Confirmed by reading
**Location:** `src/tapio/testkit/probe.py:234-257`, `:323-325`
**Issue:** none: Low findings get no issue

**What's wrong**
All signals go into one FIFO. With two watched actors stopping in either order, `expect_terminated(a)` fails when `b`'s signal is first, and that signal is consumed, so a following `expect_terminated(b)` times out.

**Proposed fix**
Search the queue for a matching `Terminated` within the timeout and keep the rest, or document "in the order they arrive".

---

### [PERI-28] `LinkFaults.delay` throttles rather than delays, `drop` replaces, and a second `link_faults` orphans the first
**Severity:** Low
**Category:** Docs
**Status:** Confirmed by reading
**Location:** `src/tapio/testkit/remote.py:96-139`, `:209-233`
**Issue:** none: Low findings get no issue

**What's wrong**
`allow_write` sleeps inside the association's writer, so with `delay(0.05)` ten frames take 0.5 s: it is a bandwidth cap, not a latency. `drop(n)` sets the count rather than adding. `link_faults(system)` called twice installs a new filter, and the first returned `LinkFaults` silently controls nothing.

**Proposed fix**
Document the three: "Every frame waits its turn, so this also limits throughput to one frame per delay." "Replaces any drops still pending." "Calling this again replaces the controls." Or raise in `link_faults` if a filter is already installed.

---

### [PERI-29] `assert_no_leaked_tasks` reports the leak instead of the block's own failure
**Severity:** Low
**Category:** Quality
**Status:** Confirmed by reading
**Location:** `src/tapio/testkit/leaks.py:30-43`
**Issue:** none: Low findings get no issue

**What's wrong**
The check runs in `finally` and raises `AssertionError` even when the block itself raised. A failing test that also leaves its actors' tasks running (because it never reached its cleanup) is reported as a leak, with the real failure demoted to `__context__`.

**Proposed fix**
Run the check only on a clean exit, or raise with the original as the primary error:

```python
    try:
        yield
    except BaseException:
        raise                      # the block's failure is the one to report
    leaked = ...
```

---

### [PERI-30] `CONTRIBUTING.md` does not carry the commit and PR rules it is said to summarise
**Severity:** Low
**Category:** Docs
**Status:** Confirmed
**Location:** `CONTRIBUTING.md:48-80`; `AGENTS.md` "Commit messages and pull requests"
**Issue:** none: Low findings get no issue

**What's wrong**
AGENTS.md says CONTRIBUTING "summarises the commit and pull request rules below, so a change to those needs the same change there". Missing from CONTRIBUTING: end the PR body with the `Co-Authored-By` trailer; do not pass `--body-file` to `gh pr merge`; `BREAKING CHANGE:` moves the minor while 0.x.

**Proposed fix**
Add three bullets under "Commits and versions" and "Pull requests" with those rules.

---

### [PERI-31] Smaller doc inaccuracies
**Severity:** Low
**Category:** Docs
**Status:** Confirmed
**Location:** various
**Issue:** none: Low findings get no issue

- `docs/supervision.md:65`: "`SupervisorStrategy.backoff` waits, and waits longer each time" reads as a constructor. It is a field; the call is `SupervisorStrategy.restart(backoff=Backoff(...))`.
- `docs/getting-started.md:11-13`: "the `tapio-cluster` operator command is a separate extra ... does not install it". The script is always installed (`pyproject.toml:50-51`); only `typer` is the extra, and without it the command exits with an install hint.
- `docs/clustering.md:423`, `:429`: "Binding it anywhere another host can reach requires a token" and the comment "required beyond loopback". A client CA (`tls.cafile`) satisfies the rule too, as the same page says thirty lines later.
- `README.md:298`: the trademark note names Apache Kafka, which the README never mentions.

---

### [PERI-32] Property tests cover the merge laws; the leader and downing decisions have none
**Severity:** Low
**Category:** Quality
**Also touches:** Quality (tests), gap note only
**Status:** Confirmed
**Location:** `tests/cluster/test_{member,reachability,clock,gossip}.py`, `tests/cluster/strategies.py`
**Issue:** none: Low findings get no issue

**What's wrong**
Commutativity, associativity and idempotence are property-tested for `Member`, `Reachability`, `VectorClock` and `Gossip`, plus an order-independence test. Not property-tested: that `leader_actions` is deterministic and idempotent for a converged view, and that a downing strategy gives the same verdict when fed the two mirror-image views of a partition (`docs/clustering.md:204-215` says safety depends on this). The cluster reviewer is writing merge-law properties; these two are the gaps I would add.

---

## 5. Design disagreements

These disagree with a documented decision. They are not bugs, and they get an issue only on request. The core pass has none: every core decision it questioned (the default `stop`, the kept mailbox on restart, `tell` never raising about the recipient) holds up.

### From the remote wire layer pass

#### Client certificates authenticate nothing about the address a peer claims
**Location:** `src/tapio/remote/transport.py:527-544`, `handshake._identify`

With `cafile` set, the server accepts any certificate the CA signed and then trusts whatever `address` the client-hello claims. A node holding a valid certificate can claim to be any other node and take over the association keyed by that address. That is consistent with the documented "one trust domain" model, so it is not a bug. With mutual TLS configured, though, checking the claimed host against the certificate's SANs costs little and would turn "the same CA" into "the same node". If that stays out of scope, `security.md` should say so next to the mutual TLS advice.

#### `localhost` is trusted by name, while every other name is refused
**Location:** `src/tapio/remote/transport.py:508-524`

The comment refuses names because "it might resolve to loopback, it might not", then accepts `localhost` by name. `bind()` resolves it with `AF_INET`, which on any sane host is `127.0.0.1`, so this is safe in practice. The documented reason does not cover the exception, though. One sentence saying that `localhost` is accepted because `bind()` resolves it with `AF_INET` would close the gap.

### From the remote link layer pass

#### One bidirectional link per pair, resolved by address order
**Location:** `src/tapio/remote/endpoint.py:345-395`, `src/tapio/remote/association.py:484-538`

Every bug in the dial-race family (LINK-2, LINK-11, the `_resume` hang in LINK-3, eaa3dfe, and the four-field ownership that `handle.py` was written to replace) comes from swapping a socket under a running writer. Pekko Artery avoids the whole class: each node writes only on the connection it dialled and only reads on the ones it accepted, so there is never a second link to retire. FIFO per association still holds, because each direction has exactly one writer and one socket. It costs one more socket per pair. Worth weighing before adding a "superseded" frame for LINK-2.

#### Every remote ask costs a watch round trip
**Location:** `src/tapio/actor/ask.py:339-367`, `src/tapio/remote/ref.py:264-277`

A remote ask sends `Watch` before the request and `Unwatch` after the reply, and registers a `_PeerWatcher` on the target cell on the peer for the duration of the ask. That is three frames per request/response and a cell map write per ask on the responder. The "unreachable" half of the benefit is available without a frame, since the association's `_end_watches` already knows the peer went away. Only the "target stopped" half needs the remote registration. A promise registered with the association locally, notified on association end, plus a target-stopped reply sent back by the peer's `receive_frame` when it dead-letters a frame addressed to a stopped actor that carries a `reply_to`, would get both halves with no extra frames. Not a bug. It is the largest fixed cost on the remote hot path.

### From the cluster membership core pass

#### One flaky link inside a side flips the whole decision
`_sides` puts every member that any live observer reports unreachable on the far side. Take a split {A, B, C} | {D, E} where B also cannot hear C. Side one counts {A, B} against {C, D, E} and downs itself. C sees itself as "unreachable", so its verdict `{A, B}` does not name it, and it survives alone for one more round before it downs itself too. Side two downs itself. The whole cluster goes down for a single bad link. Akka treats "indirectly connected" members (reported unreachable by a member of the same side) separately: it downs them on their own and decides the split on the rest. Worth adopting before KeepMajority is recommended for large clusters.

#### Vector clock entries are keyed by address and never pruned
This is acknowledged for tombstones (docs:128-131), but not for clock entries or reachability records. With addresses that change on every restart, as with pod IPs, all three grow without bound. That is the same pruning problem as MEMB-3, and it is worth naming in the docs next to the tombstone note.

### From the cluster daemon and surface pass

#### Loopback is treated as an authenticated boundary
`token` is optional on loopback "since reaching the port at all already means being on the machine" (settings.py:286-287). In a Kubernetes pod every sidecar shares loopback, and on a multi-user host every local user does. For a port that can down members, a default that asks for a token whenever one is configured anywhere in the deployment would be safer. This is a documented decision, so it is listed here and not as a finding.

#### The singleton has no hand-over protocol
The module docstring calls placement by "oldest member, no election, no lock" the smallest correct design. CLUS-3, CLUS-4 and CLUS-5 show that correctness depends on event ordering that the daemon does not guarantee across nodes. A two-message handshake between the old and new manager (as in Pekko) would make "never two at once" hold by construction rather than by timing.

### From the periphery, testkit and docs pass

#### The pytest plugin is registered in every project that installs tapio
**Location:** `pyproject.toml:55-56`, `src/tapio/testkit/plugin.py`

`[project.entry-points.pytest11] tapio = "tapio.testkit.plugin"` loads three fixtures (`actor_system`, `make_probe`, `tapio_settings`) into any pytest run in any environment that has tapio installed, including a service's own unrelated test suite, and requires an asyncio runner there. A name clash with an existing `actor_system` fixture is resolved silently by pytest's precedence rules. The documented choice is "nothing to import". The alternative is to ship the plugin and ask users to opt in with `pytest_plugins = ["tapio.testkit.plugin"]` or `-p tapio.testkit.plugin`, which costs one line and removes the global side effect.

#### `ActorContext` has a `spawn` but no way to stop a child
**Location:** `src/tapio/actor/context.py`

The only way to stop a child is to send it a message its behavior answers with `stopped()`. That is consistent with "stop through the behavior", but it means a parent cannot stop a child whose behavior has no stop message (a third-party behavior, a router's routees, an adapter-heavy child) without the child's cooperation. Pekko has `ctx.stop(child)`. Worth an explicit sentence in `lifecycle.md` if the omission is deliberate.

## 6. Coverage of this review

This review is not exhaustive, and this section says where it stops.

**Read in full by the coordinator:** `message.py`, `validation.py`, `actor/path.py`, `actor/signals.py`, `actor/ref.py`, `actor/mailbox.py`, `actor/dead_letters.py`, `actor/events.py`, `actor/cell.py`, `actor/watch.py`, `actor/supervision.py`, `actor/restarts.py`, `actor/ask.py`, `actor/timers.py`, `actor/stash.py`, `actor/behavior.py`, `actor/construction.py`, `actor/system.py`, `dispatch/tasks.py`. Skimmed: `actor/context.py` (the abstract surface only).

**Remote wire pass.** Read in full: `remote/address.py`, `codec.py`, `context.py`, `registry.py`, `protocol.py`, `transport.py`, `handle.py`, `handshake.py`, the remote and TLS parts of `settings.py`, `docs/remoting.md`, `docs/security.md`. Skimmed for call sites: `remote/endpoint.py`, `remote/association.py`, `actor/system.py`.

**Remote link pass.** Read in full: `remote/association.py`, `endpoint.py`, `failure.py`, `peers.py`, `ref.py`, `spawner.py`, `handle.py`, `actor/watch.py`, `actor/ask.py`, `testkit/remote.py`, `docs/remoting.md`, `docs/unreachable.md`. Not reached by this pass: the cluster daemon's own use of `PhiAccrualDetector`.

**Cluster membership core pass.** Read in full: `cluster/clock.py`, `member.py`, `gossip.py`, `reachability.py`, `monitor.py`, `downing.py`, `ClusterSettings`, `docs/clustering.md`, and the existing property tests. Hypothesis tried to break commutativity, associativity and idempotence on every field and tie-break, with wider pools than the suite's own strategies (about 1,500 examples per property), and could not. The merge is a real join on canonical values. `_merge_seen` is associative, `VectorClock.compare` is antisymmetric, and the monitor uses the dispatcher's monotonic clock.

**Cluster daemon and surface pass.** Read in full: `cluster/daemon.py`, `cluster.py`, `singleton.py`, `router.py`, `events.py`, `messages.py`, `management.py`, `cli.py`, `ManagementSettings`, `TLSSettings`, `docs/clustering.md`, the management section of `docs/security.md`. Checked and found sound: every timing read goes through the dispatcher's clock; the daemon and the management port own and cancel their tasks; the CLI matches the server's routes and status codes; join retry and `join_seed_nodes` timeout semantics match the docs.

**Periphery, docs and tests pass.** Read in full: `dispatch/`, `testkit/`, `logging.py`, `errors.py`, `version.py`, `settings.py`, the three package `__init__.py` files, `actor/adapter.py`, `actor/router.py`, `README.md`, every page under `docs/`, `mkdocs.yml`, `examples/README.md`, `CONTRIBUTING.md`. An autouse leak checker was run over all 894 tests and found no leaked task today, so PERI-22 is a gap rather than a live leak.

**Every module was read in full by at least one pass, except `actor/context.py`, an abstract interface that was read for its surface only.** These parts, and are where a second look would most likely find more:

- `cluster/daemon.py` was read in full by one pass. It is the largest cluster module (1,375 lines) and carries most of the cluster findings.
- `remote/association.py` (1,254 lines) was read in full by one pass. It holds three of the five Critical findings.
- The examples were executed and their docs claims checked, but their prose was not read line by line.
- `docs/playground/` and `scripts/stage_playground.py` were not reviewed.
- `tests/benchmarks/` was not reviewed. The README's benchmark figures were not re-measured, only checked for internal consistency (PERI-21).

**Not tested here:** IPv6 end to end (this sandbox has no IPv6 interface, so WIRE-5 is shown with unit probes), and Python versions other than 3.13. WIRE-3 depends on the Python 3.12+ behaviour of `Server.wait_closed`, so it does not reproduce on 3.11, which is still in the CI matrix.

## Appendix: module map

Generated from each module's own docstring and its `tapio.*` imports at `eb4c227`. One layering note it makes visible: `actor.ref`, `actor.cell` and `actor.system` import from `remote.*` (`address`, `context`, `registry`, `codec`, `endpoint`), so the local runtime cannot be imported without the remoting package. That is a design choice, ref serialisation needs it, but it means a change to the wire layer can break a purely local user.

| Module | Lines | Responsibility (its own docstring) | Depends on (tapio.*) |
|---|---|---|---|
| `tapio` | 123 | tapio: a Pekko-inspired actor toolkit for Python. | actor.behavior, actor.context, actor.dead_letters, actor.events, actor.mailbox, actor.path, actor.ref, actor.router, actor.signals, actor.stash, actor.supervision, actor.system, actor.timers, errors, message, remote.address, remote.failure, remote.registry, remote.spawner, settings, version |
| `actor` | 90 | Actors: refs, paths, behaviors, and the runtime they run in. | actor.adapter, actor.ask, actor.behavior, actor.cell, actor.context, actor.dead_letters, actor.mailbox, actor.path, actor.ref, actor.router, actor.signals, actor.stash, actor.supervision, actor.system, actor.timers, actor.watch |
| `actor.adapter` | 344 | Message adapters: taking delivery of a protocol you do not own. | actor.cell, actor.dead_letters, actor.path, actor.ref, logging, message, remote.address, remote.registry, validation |
| `actor.ask` | 378 | Ask: one request, one reply, and every way that can fail. | actor.cell, actor.dead_letters, actor.path, actor.ref, actor.watch, errors, logging, message, remote.address, validation |
| `actor.behavior` | 727 | Behaviors: what an actor does with the next message, and what it becomes. | actor.context, actor.signals, actor.stash, actor.supervision, actor.timers, errors, logging, message, validation |
| `actor.cell` | 1586 | `ActorCell`: one actor, one task, one mailbox. | actor.adapter, actor.ask, actor.behavior, actor.construction, actor.context, actor.dead_letters, actor.events, actor.mailbox, actor.path, actor.ref, actor.restarts, actor.signals, actor.stash, actor.supervision, actor.timers, actor.watch, dispatch.blocking, dispatch.dispatcher, dispatch.tasks, errors, logging, message, remote.address, remote.registry, settings, validation |
| `actor.construction` | 120 | Deferred construction: turning a wrapped behavior into one that can run. | actor.behavior, actor.context, actor.stash, actor.timers, errors, message |
| `actor.context` | 295 | `ActorContext`: what an actor is handed to act on its surroundings. | actor.behavior, actor.mailbox, actor.path, actor.ref, logging, message, validation |
| `actor.dead_letters` | 380 | Dead letters: the one place a message goes when it cannot be delivered. | actor.events, actor.path, actor.ref, logging, message, remote.address |
| `actor.events` | 114 | The event stream: things the runtime noticed, for whoever wants them. | logging, message |
| `actor.mailbox` | 360 | The two-lane mailbox: signals outrank user messages. | actor.signals, errors, message |
| `actor.path` | 92 | Actor paths: the stable, printable identity of a place in the tree. | - |
| `actor.ref` | 199 | `ActorRef`: a local handle to an actor, usable as a Pydantic field. | actor.path, actor.watch, message, remote.address, remote.context |
| `actor.restarts` | 86 | Counting an actor's restarts, one budget per supervisor. | actor.supervision |
| `actor.router` | 271 | Routers: one address in front of several identical actors. | actor.behavior, actor.cell, actor.context, actor.dead_letters, actor.mailbox, actor.ref, actor.signals, cluster.router, errors, message, validation |
| `actor.signals` | 82 | Signals: what the runtime tells an actor about its own lifecycle. | actor.ref |
| `actor.stash` | 148 | Stash: holding messages an actor is not ready for yet. | actor.behavior, errors, message |
| `actor.supervision` | 231 | Supervision: what happens to an actor whose handler raised. | - |
| `actor.system` | 734 | `ActorSystem`: the tree, its guardians, and its shutdown. | actor.behavior, actor.cell, actor.dead_letters, actor.events, actor.mailbox, actor.path, actor.ref, dispatch.blocking, dispatch.dispatcher, errors, logging, message, remote.address, remote.codec, remote.context, remote.endpoint, remote.registry, settings, validation |
| `actor.timers` | 360 | Timers: an actor sending itself a message later, or repeatedly. | actor.cell, logging, message |
| `actor.watch` | 205 | The two ends of a death watch: the protocols, and the book an actor keeps. | actor.path, actor.ref, remote.address |
| `cluster` | 98 | Clustering: membership by gossip, and a leader computed rather than elected. | cluster.clock, cluster.cluster, cluster.downing, cluster.events, cluster.gossip, cluster.member, cluster.messages, cluster.monitor, cluster.reachability, cluster.router, cluster.singleton |
| `cluster.cli` | 290 | The `tapio-cluster` command: read a cluster, and move a member. | - |
| `cluster.clock` | 152 | Vector clocks: which of two gossip states happened first, if either did. | message |
| `cluster.cluster` | 606 | Clustering, as an application sees it. | actor.events, actor.ref, actor.system, cluster.daemon, cluster.downing, cluster.events, cluster.gossip, cluster.management, cluster.member, cluster.messages, errors, remote.address, remote.endpoint, settings |
| `cluster.daemon` | 1375 | The actor that gossips: one per node, at `/system/cluster`. | actor.adapter, actor.behavior, actor.cell, actor.context, actor.events, actor.path, actor.ref, actor.signals, actor.supervision, actor.timers, cluster.clock, cluster.downing, cluster.events, cluster.gossip, cluster.member, cluster.messages, cluster.monitor, cluster.reachability, errors, logging, message, remote.failure, remote.registry, settings |
| `cluster.downing` | 485 | Deciding to stop waiting for an unreachable member, safely on both sides. | cluster.gossip, cluster.member |
| `cluster.events` | 136 | Cluster events: what a node tells an application about membership changes. | cluster.member, message |
| `cluster.gossip` | 392 | The gossip state: what a node believes about the cluster, and how two beliefs merge. | cluster.clock, cluster.member, cluster.reachability, message |
| `cluster.management` | 574 | A small HTTP surface an operator reaches a cluster through. | actor.behavior, actor.context, actor.ref, actor.signals, cluster.messages, dispatch.dispatcher, errors, logging, message, remote.transport, settings |
| `cluster.member` | 263 | Members and their statuses, and the lattice the statuses are merged by. | message, remote.address |
| `cluster.messages` | 275 | What cluster nodes say to each other, and what a node says to itself. | actor.ref, cluster.events, cluster.gossip, cluster.member, message, remote.registry |
| `cluster.monitor` | 316 | Who watches whom, so that a member is judged whether or not it is talked to. | cluster.member, cluster.reachability, remote.failure |
| `cluster.reachability` | 241 | Who can hear whom, which is a different question from who is a member. | cluster.member, message |
| `cluster.router` | 242 | A group router: one address in front of an actor on every member of a role. | actor.behavior, actor.context, actor.dead_letters, actor.ref, actor.router, actor.timers, cluster.daemon, cluster.events, cluster.member, errors, logging, message, validation |
| `cluster.singleton` | 243 | ClusterSingleton: one instance of an actor, on the oldest member of a role. | actor.behavior, actor.context, actor.ref, actor.timers, cluster.daemon, cluster.events, cluster.member, logging, message |
| `dispatch` | 10 | Dispatch: where actor work runs. | dispatch.blocking, dispatch.dispatcher |
| `dispatch.blocking` | 174 | The pool that blocking calls run on, so they do not run on the loop. | logging |
| `dispatch.dispatcher` | 76 | The dispatcher: the one place that knows which loop a system runs on. | - |
| `dispatch.tasks` | 42 | Cancelling a task and waiting for it, without losing the caller's own stop. | - |
| `errors` | 209 | The tapio error hierarchy. | - |
| `logging` | 83 | Logging that always says which actor spoke. | actor.path |
| `message` | 30 | The base class every tapio message must subclass. | - |
| `remote` | 10 | Remoting: addressing, the wire format, and the registries behind them. | - |
| `remote.address` | 179 | Addresses: where a system is, and how a ref writes itself down. | actor.path |
| `remote.association` | 1254 | An association: one link to one peer, and the actor that owns it. | actor.behavior, actor.context, actor.dead_letters, actor.events, actor.path, actor.ref, actor.signals, actor.timers, actor.watch, dispatch.dispatcher, errors, logging, message, remote.address, remote.codec, remote.failure, remote.handle, remote.handshake, remote.transport, settings |
| `remote.codec` | 439 | The wire format: a length prefix, a JSON object, and no imports. | actor.dead_letters, actor.path, errors, message, remote.address, remote.context, remote.protocol, remote.registry |
| `remote.context` | 108 | The ambient system a ref deserializes against. | actor.path, actor.ref, errors, remote.address |
| `remote.endpoint` | 966 | The endpoint: one listening port, and every association behind it. | actor.behavior, actor.cell, actor.context, actor.dead_letters, actor.events, actor.mailbox, actor.path, actor.ref, actor.signals, actor.watch, dispatch.dispatcher, errors, logging, message, remote.address, remote.association, remote.handle, remote.handshake, remote.peers, remote.ref, remote.transport, settings, validation |
| `remote.failure` | 377 | Deciding that a peer is gone, and admitting what that decision is worth. | message, remote.address |
| `remote.handle` | 128 | One socket, its reader task, and the single close that ends both. | dispatch.tasks, remote.transport |
| `remote.handshake` | 343 | The handshake: who is on the other end, and may they speak at all. | errors, remote.address, remote.protocol, remote.transport, version |
| `remote.peers` | 107 | Which peers this system will talk to, and which it has given up on. | remote.address |
| `remote.protocol` | 67 | The version of the wire contract, which is not the version of the library. | - |
| `remote.ref` | 308 | `RemoteRef`: a handle to an actor on another system. | actor.ask, actor.cell, actor.path, actor.ref, actor.watch, errors, message, remote.address, remote.codec, validation |
| `remote.registry` | 231 | Two registries: message types by key, and live refs by path. | actor.path, actor.ref, errors, message |
| `remote.spawner` | 595 | Starting an actor on another node, without supervising it from here. | actor.behavior, actor.context, actor.ref, errors, logging, message, remote.registry |
| `remote.transport` | 571 | The link: length-framed JSON over a TCP stream, with optional TLS. | errors, remote.codec, settings |
| `settings` | 378 | System-wide settings, read from the environment with a `TAPIO_` prefix. | actor.mailbox |
| `testkit` | 68 | Test support: helpers for asserting things about a running system. | testkit.behavior, testkit.leaks, testkit.probe, testkit.remote, testkit.settings |
| `testkit.behavior` | 623 | `BehaviorTestKit`: one behavior, no system, no mailbox, no scheduling. | actor.behavior, actor.construction, actor.context, actor.mailbox, actor.path, actor.ref, actor.signals, actor.stash, actor.supervision, actor.timers, errors, logging, message, settings, validation |
| `testkit.leaks` | 69 | Assertions that a block of work left nothing running behind it. | - |
| `testkit.plugin` | 107 | The pytest plugin: fixtures for tests that need a running system. | actor.system, message, settings, testkit.leaks, testkit.probe, testkit.settings, validation |
| `testkit.probe` | 332 | `TestProbe`: an actor whose only job is to be asserted about. | actor.behavior, actor.context, actor.mailbox, actor.path, actor.ref, actor.signals, actor.system, message, validation |
| `testkit.remote` | 356 | Two nodes in one process, and a way to break the link between them. | actor.system, errors, remote.transport, testkit.settings |
| `testkit.settings` | 118 | Settings a test builds from its own arguments, and from nothing else. | settings |
| `validation` | 193 | Delivery-time message validation. | actor.path, errors, message, settings |
| `version` | 54 | The package version, in one place both the package and the wire can read. | - |
