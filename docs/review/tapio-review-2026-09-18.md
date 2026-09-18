# Code review of tapio, 2026-09-18

Reviewed at commit `bc6bbaa` on `main`. Every finding cites a file and line range as of that commit.

## 1. Summary

The runtime is in good shape for a pre-alpha: lint, `mypy --strict`, 843 tests, 29 examples and a strict docs build all pass, coverage is 95%, and the merge laws the cluster rests on are property-tested. The documentation is unusually honest about what the library does not promise. Most of what is wrong sits at the seams between layers, where a rule that holds inside one module is assumed by another.

Three things matter most:

1. **Death watch keys watchers by actor path, and paths do not include the node.** Two nodes that share a system name, which is the deployment shape the clustering docs recommend, collide in a remote target's watcher map, and one of them silently never hears `Terminated` (T-01). The same identity gap makes two `ActorRef`s to different nodes compare equal.
2. **Deferred construction is run from four places with three different failure policies.** A `setup` that raises when returned from a handler kills the actor's task outside supervision (T-02), a `setup` that spawns and then raises orphans its children past system termination (T-03), and a remote factory whose `setup` raises stops the spawner and every worker it started (T-05). One shared "evaluate and account for failure" path would close all three (T-13).
3. **One line of ordinary user code takes a node out of its cluster.** Subscribing to cluster events through a message adapter, the documented way to accept a foreign protocol, raises `WatchError` inside the daemon's receive loop, and the daemon stops. So does a subscriber whose declared type does not cover an event it is sent: the repository's own `test_a_subscriber_that_asked_for_nothing_hears_everything` passes while the daemon dies underneath it (T-04, T-23).

Everything above was reproduced with the tests quoted under each finding. No Critical security hole was found; the trust model the security page states is implemented as described.

**The open issue.** Issue #140 (a connected link socket collected with its transport still open, root cause unknown, `bc6bbaa` landed as a candidate fix) was checked against every remoting path read here. Nothing in this review explains it, and nothing here contradicts the theory in its comment that the socket-ownership rewrite removed it. The full suite was run eight times on `bc6bbaa` as part of this review; the result is recorded in section 5. Issue #64 (closed) is the graceful-leave half of design point D-1. Issues #60 and #95 (closed) fixed the two earlier off-loop remote `tell` bugs; T-07 is the third in that line and sits one call earlier in the same path.

## 2. Findings table

| ID | Severity | Category | Status | File | Title | Issue |
|---|---|---|---|---|---|---|
| T-01 | Critical | Bug | Confirmed | `src/tapio/actor/watch.py` | Watchers are keyed by path only, so peers sharing a system name lose `Terminated` | |
| T-02 | High | Bug | Confirmed | `src/tapio/actor/cell.py` | A `setup` returned from a handler that raises escapes supervision and kills the actor | |
| T-03 | High | Bug | Confirmed | `src/tapio/actor/cell.py` | A `setup` that spawns children and then raises orphans them past termination | |
| T-04 | High | Bug | Confirmed | `src/tapio/cluster/daemon.py` | A subscriber the daemon cannot watch, or cannot deliver to, stops the cluster daemon | |
| T-05 | High | Bug | Confirmed | `src/tapio/remote/spawner.py` | A remote factory whose `setup` raises stops the spawner and its children | |
| T-06 | Medium | Bug | Confirmed | `src/tapio/actor/cell.py` | Self-sends and timers scheduled during the first construction skip validation | |
| T-07 | Medium | Bug | Confirmed | `src/tapio/remote/endpoint.py` | An off-loop remote `tell` with no association spawns an actor from the wrong thread | |
| T-08 | Medium | Bug | Confirmed | `src/tapio/cluster/router.py` | A group router freezes its own node's routee at the moment it heard `MemberUp` | |
| T-09 | Medium | Bug | Confirmed | `src/tapio/actor/system.py` | `terminate()` awaited inside a handler waits out the whole deadline and cancels the caller | |
| T-10 | Medium | Docs | Confirmed | `docs/unreachable.md` | The page says `Terminated` always comes with a quarantine; a failed link tells watchers and re-dials | |
| T-11 | Medium | Docs | Confirmed | `docs/getting-started.md` | Two pages still say clustering does not exist | |
| T-12 | Medium | Docs | Confirmed | `src/tapio/remote/failure.py` | Docstrings say clustering replaces the decider and the peer provider; nothing does | |
| T-13 | Medium | Quality | Confirmed | `src/tapio/actor/cell.py` | Deferred construction has four call sites and three failure policies | |
| T-14 | Medium | Bug | Suspected | `src/tapio/remote/association.py` | A simultaneous dial resolved during a stalled write closes the surviving association | |
| T-15 | Low | Bug | Confirmed | `src/tapio/actor/ask.py` | `ask` with a union reply type raises `AttributeError` | |
| T-16 | Low | Bug | Suspected | `src/tapio/actor/system.py` | A failure inside the drain leaves `when_terminated` waiting for ever | |
| T-17 | Low | Quality | Confirmed | `src/tapio/remote/codec.py` | `decode` re-serializes the payload the encoder was careful not to | |
| T-18 | Low | Quality | Confirmed | `src/tapio/actor/cell.py` | A restart racing a parent's stop logs a traceback for a benign race | |
| T-19 | Low | Dead code | Confirmed | several | Unused public members: `describe_blocking`, `last_heard`, `logged`, `use_peers` | |
| T-20 | Low | Quality | Confirmed | `tests/actor/test_dead_letters.py` | Tests that sleep a guessed duration before asserting | |
| T-21 | Low | Docs | Confirmed | `src/tapio/actor/system.py` | `resolve` skips the `expect` check for a local address though its docstring says it raises | |
| T-22 | Low | Quality | Confirmed | `src/tapio/remote/association.py` | Every remote send re-validates the `Outbound` wrapper when `validate_on_tell` is on | |
| T-23 | Low | Quality | Confirmed | `tests/cluster/test_events.py` | A cluster events test passes while the daemon it exercises has died | |

## 3. Findings, in full

### [T-01] Watchers are keyed by path only, so peers sharing a system name lose `Terminated`

**Severity:** Critical
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/actor/watch.py:113-121`, `src/tapio/remote/association.py:1006-1017`, `src/tapio/remote/association.py:1206-1209`, `src/tapio/actor/ref.py:145-153`

**What's wrong.** A cell's watchers live in `DeathWatch._watchers`, a dict keyed by `watcher.path`:

```python
def add_watcher(self, watcher: Watcher) -> None:
    self._watchers[watcher.path] = watcher
```

A watch that arrives over a link is registered as a `_PeerWatcher` whose `path` is the peer's watcher path, parsed into the peer's system name and nothing else:

```python
proxy = _PeerWatcher(
    ...,
    watcher=parse_target(self._peer.system, request.watcher),
    ...
)
target.add_watcher(proxy)
```

An `ActorPath` is `(system, elements, uid)`. It carries no address. The clustering page tells every node to use the same system name (`tapio://orders@10.0.0.1:2551`, `tapio://orders@10.0.0.2:2551`), and identical deployments spawn the same actors in the same order, so the incarnation counter hands out the same uids on every node. Two nodes named `orders` that both spawn `/user/coordinator` first both produce `tapio://orders/user/coordinator#1`. When both watch the same actor on a third node, the second `add_watcher` overwrites the first, and the first watcher never hears anything. An `unwatch` from one peer removes the other's entry for the same reason.

The same identity gap sits under `ActorRef.__eq__` and `__hash__`, which compare paths only: a `RemoteRef` to `/user/x#1` on node A equals one to `/user/x#1` on node B, so they collapse in any set or dict a user keys by ref, and in `DeathWatch._watching` on the watching side.

**Why it matters.** A watcher waits for ever for a signal the library promised. The lifecycle page sells death watch as the way to keep a map of live actors true; with the collision, the map silently keeps a dead entry. An `ask` promise is a watcher too, so two nodes asking the same remote actor can collide, and the loser waits out its full timeout instead of failing fast when the target stops. Nothing logs, because from every participant's point of view the registration succeeded. The existing test suite never sees it because `tests/cluster/conftest.py` names its nodes `node1`, `node2` and so on, while the documentation recommends the colliding shape.

**Proposed fix.** Key remote watchers by the peer's address as well as the path. The smallest change is on the association side, since the watcher path is opaque to the target anyway:

```python
# association.py, in _on_watch
proxy = _PeerWatcher(
    association=self,
    target=target,
    watchee=watchee,
    watcher=_peer_watcher_path(self._peer, request.watcher),
    reply_to=request.watcher,
)
```

where `_peer_watcher_path` builds a path that cannot collide across peers, for example by parsing the watcher path in a synthetic system name derived from the peer's canonical address (`f"{peer.system}@{peer.host}:{peer.port}"` sanitised to the path alphabet). A cleaner but wider fix is to give `Watcher` a `key` property separate from `path` and key `DeathWatch._watchers` by it: a cell's key is its path, a `PromiseRef`'s its path, a `_PeerWatcher`'s the pair `(peer address, watcher path)`.

Separately, `ActorRef.__eq__` and `__hash__` should include `address` for refs that carry one, so that refs to different nodes never compare equal. `LocalActorRef`, `RemoteRef`, `AdapterRef` and `PromiseRef` all expose `address` already.

Alternative considered: telling users to give every node a distinct system name. Rejected because the docs, the CLI examples and the `Cluster` docstring all use one name per cluster, and because uid collisions would still make two refs to different nodes compare equal.

**Test to prove it.** Fails before the fix at `results["a"]`, which reports `MISSED`; passes after.

```python
async def test_two_peers_with_the_same_system_name_collide_as_watchers():
    target = ActorSystem("target", remoting())
    node_a = ActorSystem("worker", remoting())
    node_b = ActorSystem("worker", remoting())
    try:
        actor = target.spawn(
            Behaviors.receive_message(_stoppable, msg_type=Ping | Stop), "actor"
        )
        probe_a: TestProbe[Ping] = TestProbe(node_a, Ping)
        probe_b: TestProbe[Ping] = TestProbe(node_b, Ping)
        assert probe_a.path == probe_b.path  # identical deployments do this

        ref_a = await node_a.resolve(uri(target, actor), expect=Ping | Stop)
        ref_b = await node_b.resolve(uri(target, actor), expect=Ping | Stop)
        probe_a.watch(ref_a)
        probe_b.watch(ref_b)
        await eventually(lambda: len(cell_of(actor).watchers) >= 1)
        ref_a.tell(Stop())

        await probe_b.expect_terminated(ref_b)
        await probe_a.expect_terminated(ref_a)  # fails: got nothing within 3s
    finally:
        await node_a.terminate()
        await node_b.terminate()
        await target.terminate()
```

Observed: `watchers on the target cell: (ActorPath('tapio://worker/user/$1#2'),)`, one entry for two watchers.

### [T-02] A `setup` returned from a handler that raises escapes supervision and kills the actor

**Severity:** High
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/actor/cell.py:896-906`, `src/tapio/actor/cell.py:1127-1143`, `src/tapio/actor/cell.py:1197-1205`

**What's wrong.** `_on_message` guards the handler with `except Exception`, then applies the returned behavior outside that guard:

```python
self._current = message
try:
    nxt = await behavior.receive(self._ctx, cast("T", message))
except Exception as error:
    await self._on_failure(error)
    return
finally:
    self._current = None
await self._become(nxt, message)
```

`_become` calls `_evaluate`, which runs user code: `SetupBehavior.setup`, `WithTimersBehavior.with_timers`, `WithStashBehavior.with_stash`, and the `_MAX_SETUP_DEPTH` check. An exception from any of them propagates out of `_on_message`, out of `_run`, and ends the actor's task with an unretrieved exception. The `finally` in `_run` calls `_finish`, so watchers are told and the mailbox is drained, but no supervision decision is taken, `PreRestart` and `PostStop` never run, and a `restart` strategy is ignored. `_on_signal` has the same shape at line 875. The restart path at line 1042 wraps the same call in `try/except`, so the author knew this could raise; the handler path did not get the same care.

Returning `Behaviors.setup(...)` from a handler is the documented way to switch into a state that needs construction, and construction is exactly where a resource is opened and can fail.

**Why it matters.** The supervision page promises that a failing handler "never leaves the actor's own receive loop" and "becomes a decision". Here it leaves the loop. The actor dies under a `restart()` strategy, `PostStop` does not run so a held resource is not released, and asyncio reports "Task exception was never retrieved" at garbage collection, attributed to nothing.

**Proposed fix.** Move `_become` inside the guarded region, so a failure while becoming is the same failure as one while receiving:

```diff
     self._current = message
     try:
         nxt = await behavior.receive(self._ctx, cast("T", message))
+        await self._become(nxt, message)
     except Exception as error:
         await self._on_failure(error)
-        return
     finally:
         self._current = None
-    await self._become(nxt, message)
```

and the same in `_on_signal`. `_become` only awaits `_stop_self`, which is already safe to run inside the guard.

**Test to prove it.** Before the fix the actor terminates and its task holds the `RuntimeError`; after it, the actor restarts and handles `Ping(n=2)`.

```python
async def test_a_setup_returned_from_a_handler_is_supervised():
    restarts = 0
    seen: list[int] = []

    def build(ctx):
        nonlocal restarts
        restarts += 1

        async def on_message(message: Ping) -> Behavior[Ping]:
            seen.append(message.n)
            if message.n == 1:
                return Behaviors.setup(lambda ctx: (_ for _ in ()).throw(RuntimeError("no")))
            return Behaviors.same()

        return Behaviors.receive_message(on_message, msg_type=Ping)

    async with ActorSystem("t", IsolatedTapioSettings()) as system:
        ref = system.spawn(
            Behaviors.supervise(Behaviors.setup(build)).on_failure(
                SupervisorStrategy.restart()
            ),
            name="actor",
        )
        ref.tell(Ping(n=1))
        ref.tell(Ping(n=2))
        await eventually(lambda: 2 in seen)
        assert restarts == 2
```

Observed before the fix: `restarts: 1`, the probe watching the actor received `Terminated`, and `task.exception()` was the `RuntimeError`.

### [T-03] A `setup` that spawns children and then raises orphans them past termination

**Severity:** High
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/actor/cell.py:766-786`, `src/tapio/actor/cell.py:484-508`

**What's wrong.** `_spawn_child` registers the child cell, starts it, and on any exception removes it from the parent's map:

```python
self._children[name] = child
try:
    child.start()
except BaseException:
    self._children.pop(name, None)
    raise
```

`child.start()` runs the child's deferred construction synchronously. If that construction spawned grandchildren before raising, or returned a behavior with no message type so that `start` raises `BehaviorTypeError` at line 508, those grandchildren have already started their tasks, registered themselves in the ref registry, and recorded the failed cell as their parent. The failed cell is dropped from the tree and never started or stopped, so nothing in the shutdown sweep reaches them. The system's `terminate()` returns with their tasks still running.

**Why it matters.** This breaks the invariant the repository lists first among the runtime's design rules: every task belongs to a cell and is cancelled in its termination sequence. The `actor_system` fixture would catch it, but only in a test that happens to hit the path. A production service that opens a connection in `setup` after spawning a helper and gets an exception from the connection leaves the helper running, addressable and holding whatever it holds, for the life of the process.

**Proposed fix.** Make `start` own the failure: if evaluation raises after children exist, stop them before re-raising. Since `start` is synchronous, the honest option is to abort them the way `_finish` already does:

```diff
 def start(self) -> None:
-    behavior = self._evaluate(self._initial)
+    try:
+        behavior = self._evaluate(self._initial)
+    except BaseException:
+        # Construction failed after it may have spawned children. Nothing
+        # will ever stop this cell, so it stops what it started.
+        self._finish()
+        raise
     self._install_behavior(behavior)
     if directive_of(behavior) is Directive.STOPPED:
         self._finish()
         return

     msg_type = behavior.msg_type
     if msg_type is None:
+        self._finish()
         msg = (...)
         raise BehaviorTypeError(msg)
```

`_finish` aborts the children, drains the mailbox, releases watches and deregisters refs, and is idempotent. `_spawn_child` can then drop its own `pop`, since `_finish` calls `_parent._remove_child`.

**Test to prove it.** Fails before the fix with `1 task(s) still running: tapio-cell:tapio://t/user/parent/kid#2`.

```python
async def test_a_setup_that_spawns_then_raises_leaves_no_orphan():
    kid = None

    def build(ctx):
        nonlocal kid
        kid = ctx.spawn(Behaviors.receive_message(_echo, msg_type=Ping), "kid")
        raise RuntimeError("setup failed after spawning")

    with assert_no_leaked_tasks():
        async with ActorSystem("t", IsolatedTapioSettings()) as system:
            with pytest.raises(RuntimeError):
                system.spawn(Behaviors.setup(build), name="parent")
            assert system.refs.lookup(kid.path) is None
        assert not cell_of(kid).is_alive
```

### [T-04] A subscriber the daemon cannot watch, or cannot deliver to, stops the cluster daemon

**Severity:** High
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/cluster/daemon.py:1110-1127`, `src/tapio/cluster/daemon.py:1100-1108`, `src/tapio/cluster/daemon.py:1141-1168`, `src/tapio/cluster/daemon.py:432-471`, `src/tapio/actor/cell.py:1363-1379`

**What's wrong.** Two things about a subscriber can raise inside the daemon's turn, and the daemon has no supervision layer, so either one stops it with the default decision. The daemon's `_receive` catches nothing.

First, `_subscribe` watches every new subscriber:

```python
ref = message.subscriber
wanted = frozenset(message.events)
if ref.path not in self._subscribers:
    ctx.watch(ref)
```

`ctx.watch` raises `WatchError` for a ref with no watch target: an `AdapterRef`, a `RecordingRef`, a `DeadLetterRef`, or a `PromiseRef`. `_receive` does not catch it, and the daemon's behavior has no supervision layer, so the default decision applies and the daemon stops. `Cluster.subscribe` is fire-and-forget, so the caller hears nothing.

An adapter is the natural subscriber. The clustering page says the subscriber "has to accept the events it asks for as part of its declared message type", and the getting-started page says the way to accept a foreign protocol without widening your own type is `ctx.message_adapter`. Following both pages together produces this crash.

Second, `_deliver` and `_replay` send each event with a plain `ref.tell(event)`. A local `tell` validates against the recipient's declared type on the caller's side and raises `MessageTypeError` to the caller, which here is the daemon. A subscriber whose declared type does not cover an event it asked for, or that asked for everything and declared less, therefore stops the daemon the first time such an event is produced. `_replay` sends `LeaderChanged` to every subscriber that did not filter, immediately on subscribing, so "subscribe to everything" with a type that omits `LeaderChanged` kills the daemon at once.

That second case is already in the repository. `tests/cluster/test_events.py:17-34` declares its recorder as `MemberUp | MemberRemoved` and `test_a_subscriber_that_asked_for_nothing_hears_everything` subscribes it with no filter. Running that test with error logging on shows:

```
ERROR tapio.actor:cell.py:976 tapio://node1/system/cluster#2: stopping after a failure in Subscribe
tapio.errors.MessageTypeError: LeaderChanged sent to tapio://node1/user/watcher#4 does not match the declared message type MemberUp | MemberRemoved
```

The test passes because the `MemberUp` replays land before the `LeaderChanged` that raises. The comment on issue #140 noticed the intermittent `MessageTypeError` and called it a latent test defect; the defect it exposes is in the daemon (see also T-23).

**Why it matters.** The node silently leaves the cluster. The daemon's well-known name is deregistered, so gossip addressed to it dead-letters on every other node. Its heartbeats stop, and its watchers report it unreachable; with a downing strategy configured, the cluster downs a healthy node because one actor subscribed to events. `Cluster.subscribe` is fire-and-forget, so the caller hears nothing, and the only trace is one error log line from the actor logger. Observed for the adapter case: `stopping after a failure in Subscribe`, and `system.refs.lookup(/system/cluster)` returned `None` afterwards.

**Proposed fix.** Three parts. Refuse an unwatchable subscriber in the daemon without failing:

```diff
 if ref.path not in self._subscribers:
-    ctx.watch(ref)
+    try:
+        ctx.watch(ref)
+    except WatchError as error:
+        _log.warning("refusing to subscribe %s: %s", ref.path, error)
+        return
```

And make an adapter watchable, since it has an owner whose death is the right signal: `AdapterRef.watch_target` can return `self._cell`, exactly as `LocalActorRef` does. That is also the answer the getting-started page already gives ("Watch the actor that owns it"), made automatic. With that, the `try/except` becomes a guard against test doubles and dead-letter refs only.

Treat a delivery the subscriber refuses as the subscriber's problem, in `_deliver` and `_replay`:

```diff
-        for ref, wanted in self._subscribers.values():
+        for ref, wanted in list(self._subscribers.values()):
             if not wanted or type(event) in wanted:
-                ref.tell(event)
+                try:
+                    ref.tell(event)
+                except MessageTypeError as error:
+                    _log.warning("dropping subscriber %s: %s", ref.path, error)
+                    self._subscribers.pop(ref.path, None)
```

together with a `dead_letter` for the event, so the absence is observable. The cleaner long-term answer is for `Cluster.subscribe` to check the subscriber's declared type against the requested events up front, which is possible for a `LocalActorRef` through `cell.msg_type`.

Independently, the daemon should carry a `Behaviors.supervise(...).on_failure(SupervisorStrategy.resume())` layer for `TapioError`, since a bad request from a local caller must not end the node's membership. The `_peer` docstring already argues this for address resolution; the same argument covers every message the daemon accepts from application code.

**Test to prove it.** Before the fix the daemon terminates; after it, the listener receives `SawUp` for its own node.

```python
async def test_subscribing_through_an_adapter_keeps_the_daemon_running():
    system = ActorSystem("node1", remoting())
    try:
        cluster = Cluster(system, QUICK)
        await cluster.join_seed_nodes([cluster.address])
        daemon = system.refs.lookup(ActorPath.root("node1").child("system").child("cluster"))
        watcher: TestProbe[Ping] = TestProbe(system, Ping)
        watcher.watch(daemon)
        got: list[SawUp] = []

        def build(ctx):
            events = ctx.message_adapter(
                lambda up: SawUp(address=up.member.address), msg_type=MemberUp
            )
            cluster.subscribe(events, MemberUp)

            async def on_message(message: SawUp) -> Behavior[SawUp]:
                got.append(message)
                return Behaviors.same()

            return Behaviors.receive_message(on_message, msg_type=SawUp)

        system.spawn(Behaviors.setup(build), "listener")
        await eventually(lambda: got != [])          # fails before the fix
        await watcher.expect_no_message()
    finally:
        await system.terminate()
```

### [T-05] A remote factory whose `setup` raises stops the spawner and its children

**Severity:** High
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/remote/spawner.py:487-514`, `src/tapio/remote/spawner.py:413-422`

**What's wrong.** `_answer` guards `factory.build(args)` with `except Exception` and the spawn with three narrow clauses:

```python
try:
    ref = (
        ctx.spawn_anonymous(behavior) if name is None else ctx.spawn(behavior, name)
    )
except ActorSystemTerminating as error:
    ...
except (ActorNameError, ValueError) as error:
    ...
except TapioError as error:
    ...
```

A factory that returns `Behaviors.setup(...)`, which the module docstring recommends so that every instance is built fresh, does its real work inside `ctx.spawn`, and a user exception from that `setup` matches none of the clauses. It propagates out of `on_spawn`, the spawner has no strategy, and it stops. The docstring on `_answer` says "every failure is a reply rather than an exception" and explains why: the spawner is the parent of every actor it started. `SpawnFailure.FACTORY_FAILED` describes exactly this case and is never produced for it.

**Why it matters.** One request naming an offered factory whose construction fails, for instance because a dependency is down at that moment, stops the spawner and every worker under it. The requester gets `AskTargetTerminated` rather than `SpawnFailed(reason="factory-failed")`, so its retry logic takes the wrong branch. Observed: `stopping after a failure in Spawn` and the watching probe received `Terminated` for the spawner.

**Proposed fix.** Treat any non-tapio exception from the spawn as the factory failing, which is what it is:

```diff
     except TapioError as error:
         return SpawnFailed(
             factory=key, reason=SpawnFailure.FACTORY_FAILED, detail=str(error)
         )
+    except Exception as error:  # the factory's setup raised at spawn
+        ctx.log.exception("the factory for %r raised while starting", key)
+        return SpawnFailed(
+            factory=key,
+            reason=SpawnFailure.FACTORY_FAILED,
+            detail=f"starting {key!r} raised {type(error).__name__}: {error}",
+        )
```

Note that this still depends on T-03 being fixed, or a `setup` that spawned before raising leaves orphans under the spawner's path.

**Test to prove it.**

```python
@remote_behavior("boom")
def boom(args: NoArgs) -> Behavior[Boom]:
    return Behaviors.setup(lambda ctx: (_ for _ in ()).throw(RuntimeError("no")))

async def test_a_factory_whose_setup_raises_is_answered_with_spawn_failed():
    async with ActorSystem("t", IsolatedTapioSettings()) as system:
        starter = system.spawn(spawner(offers=["boom"]), "spawner")
        reply = await starter.ask(
            lambda r: Spawn(factory="boom", args=NoArgs(), reply_to=r),
            expect=SpawnReply,
        )  # before the fix: raises AskTargetTerminated
        assert isinstance(reply, SpawnFailed)
        assert reply.reason == SpawnFailure.FACTORY_FAILED
```

### [T-06] Self-sends and timers scheduled during the first construction skip validation

**Severity:** Medium
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/actor/cell.py:492-515`, `src/tapio/actor/cell.py:423-424`, `src/tapio/actor/timers.py:85-104`, `src/tapio/actor/timers.py:294-303`

**What's wrong.** `start` evaluates the behavior first and resolves the validator second:

```python
behavior = self._evaluate(self._initial)
...
self._msg_type = msg_type
self._validate = resolve_validator(...)
```

Until that second line runs, `self._validate` is `_accept_anything`. Everything the factory does during evaluation therefore sends unchecked: `ctx.self_ref.tell(...)` goes through `cell.validate`, and `timers.start_single` and friends call `self._cell.validate(message)` whose docstring promises the message is "checked against this actor's declared type now rather than when it fires". `_fire` deliberately uses the off-loop delivery path with no check, trusting that one. A restart is not affected, because the validator survives it; only the first construction is.

**Why it matters.** The validation module opens with "every message is validated", and the delivery-time type check is described as unconditional. A timer started in a `with_timers` factory with the wrong message type, the most common place to start one, reaches the handler as an object of a type the handler never declared. Under `validate_on_tell` the contents check is skipped too. The failure shows up as an `AttributeError` inside the handler, which supervision then treats as the handler's fault.

**Proposed fix.** Resolve the message type before running deferred construction when the initial behavior declares one, and otherwise fail closed until it is known. The simplest robust version: have `validate` raise while the type is unknown, so a factory that sends to its own actor is told at once, and have the cell resolve the validator immediately after `_evaluate` returns and before any queued self-send is read. Since the mailbox is not consumed until the task starts, the enqueued messages can be validated lazily on the first `get`:

```diff
 def validate(self, message: Message) -> None:
+    if self._msg_type is None and self._alive:
+        msg = (
+            f"{self._path} cannot accept {type(message).__name__} yet: its "
+            "message type is not known until deferred construction returns. "
+            "Send from the handler, or schedule the timer from setup's result."
+        )
+        raise MessageTypeError(msg)
     self._validate(message)
```

That refuses the send loudly rather than silently. If a self-send from `setup` is a pattern worth keeping, the alternative is to buffer such messages in the cell and validate them once the type is known, publishing the rejects as dead letters.

**Test to prove it.** Before the fix `received[0]` is an `Other`; after it, `start_single` raises `MessageTypeError` in the factory, which surfaces from `spawn`.

```python
async def test_a_timer_started_in_the_factory_is_type_checked():
    def with_timers(timers) -> Behavior[Ping]:
        timers.start_single("boom", Other(text="wrong"), timedelta(milliseconds=10))
        return Behaviors.receive_message(_record, msg_type=Ping)

    async with ActorSystem("t", IsolatedTapioSettings()) as system:
        with pytest.raises(MessageTypeError):
            system.spawn(Behaviors.with_timers(with_timers), name="timed")
```

### [T-07] An off-loop remote `tell` with no association spawns an actor from the wrong thread

**Severity:** Medium
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/remote/endpoint.py:871-890`, `src/tapio/remote/endpoint.py:454-474`, `src/tapio/remote/endpoint.py:735-753`, `src/tapio/remote/association.py:383-402`

**What's wrong.** `Association.send` hops onto the loop when called from another thread, and the docstring explains why. But `PeerOutbox.send` runs before it:

```python
association = self._endpoint.outbound(self._peer)
if association is None:
    ...
association.send(message, frame, recipient)
```

`outbound` creates the association when there is none, and `_start_association` calls `parent.spawn(...)`, which builds an `ActorCell`, creates a future on the loop and calls `loop.create_task`. All of that runs on the calling thread. `create_task` from a foreign thread appends to the loop's ready queue without waking it and without a lock. With loop debugging on it raises immediately; without it, the behaviour depends on timing.

**Why it matters.** `LocalActorRef.tell` and `AdapterRef.tell` document themselves as safe from any thread, the remoting module docstring says a remote ref "behaves like one", and `Association.send` carries explicit off-loop handling, so a reader concludes a remote `tell` is thread-safe. It is, except for the first send to a peer, or the first send after a link failed, which are the sends most likely to come from a callback thread. Observed with `loop.set_debug(True)`: `RuntimeError: Non-thread-safe operation invoked on an event loop other than the current one`, and the association's `_run` coroutine was never awaited.

**Proposed fix.** Hop before looking the association up:

```diff
 def send(self, message: Message, frame: bytes, recipient: ActorPath) -> None:
+    dispatcher = self._endpoint.dispatcher
+    if not dispatcher.is_current():
+        try:
+            dispatcher.call_soon_threadsafe(self.send, message, frame, recipient)
+        except RuntimeError:
+            _log.warning("dead letter: %s to %s sent after the loop closed", ...)
+        return
     association = self._endpoint.outbound(self._peer)
```

`Association.send` can then drop its own hop, or keep it as a second line of defence. `PeerOutbox.watch` has the same shape and the same fix, though a watch is only registered from a cell and so is always on the loop today.

**Test to prove it.** Passes after the fix, fails before with the `RuntimeError` above.

```python
async def test_a_remote_tell_from_a_thread_dials_on_the_loop():
    loop = asyncio.get_running_loop()
    loop.set_debug(True)
    alpha, beta = ActorSystem("alpha", remoting()), ActorSystem("beta", remoting())
    try:
        probe: TestProbe[Ping] = TestProbe(beta, Ping)
        ref = await alpha.resolve(uri(beta, probe.ref), expect=Ping)
        errors: list[BaseException] = []

        def send() -> None:
            try:
                ref.tell(Ping(n=1))
            except BaseException as error:
                errors.append(error)

        thread = threading.Thread(target=send)
        thread.start()
        await loop.run_in_executor(None, thread.join)
        assert errors == []
        await probe.expect_message(Ping(n=1))
    finally:
        loop.set_debug(False)
        await alpha.terminate()
        await beta.terminate()
```

### [T-08] A group router freezes its own node's routee at the moment it heard `MemberUp`

**Severity:** Medium
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/cluster/router.py:165-178`, `src/tapio/actor/system.py:410-420`

**What's wrong.** On `MemberUp` the router resolves the routee once and keeps the ref:

```python
uri = f"{member.address}{self._path}"
routee = await ctx.resolve(uri, expect=cast(Any, self._msg_type))
self._routees[member.address] = routee
```

For a member on another node that is fine: a `RemoteRef` carries a bare path and the peer looks it up on every frame. For the member that is this node, `resolve` goes through `resolve_path`, which returns the live local ref if the well-known name is registered at that instant and a `DeadLetterRef` otherwise. The router then holds that answer for as long as the member is up. The docstring on `_offer` says a ref "returns a ref whether or not the actor over there is published yet. One that is not dead-letters what it is sent", which describes a remote member and is wrong for the local one: the remote member recovers when the routee is published, the local one never does. A local routee that is stopped and respawned under the same name has the same problem, since the old ref addresses the old incarnation.

**Why it matters.** The local share of the work goes to dead letters for the life of the router, with reason `unknown-recipient`, whenever the router is spawned before the routee is published, which is the natural order (spawn the router in `setup`, publish workers when they are ready). Observed: five jobs, five dead letters at `tapio://node1/user/worker`, zero delivered.

**Proposed fix.** Resolve the local routee at send time by its well-known name, the way a peer does. The cheapest version is to keep the address and look the local one up on each forward:

```diff
 def _forward(self, ctx: ActorContext[Any], message: Message) -> None:
-    routees: Sequence[ActorRef[Any]] = list(self._routees.values())
+    routees = [self._current(address, ref) for address, ref in self._routees.items()]
```

with `_current` returning `ref` for a `RemoteRef` and `system.refs.lookup(path_without_uid)` for the local address. A cleaner fix is a `LocalWellKnownRef` returned by `resolve_path` for a uid-less local path, which looks its target up on every `tell`, so that the local and remote cases behave identically and nothing above the ref has to know.

**Test to prove it.** Fails before the fix with `counts == []` and five dead letters.

```python
async def test_a_group_router_finds_a_local_routee_published_later():
    counts: list[int] = []
    system = ActorSystem("node1", remoting())
    try:
        cluster = Cluster(system, QUICK)  # roles={"worker"}
        router = system.spawn(Routers.group(Job, role="worker", path="/user/worker"), "router")
        await cluster.join_seed_nodes([cluster.address])
        await eventually(lambda: router_has_routees())  # or a short sleep
        worker = system.spawn(Behaviors.receive_message(_count(counts), msg_type=Job), "worker")
        system.refs.register_well_known(worker)
        for n in range(5):
            router.tell(Job(n=n))
        await eventually(lambda: counts == [0, 1, 2, 3, 4])
    finally:
        await system.terminate()
```

### [T-09] `terminate()` awaited inside a handler waits out the whole deadline and cancels the caller

**Severity:** Medium
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/actor/system.py:675-688`, `src/tapio/actor/cell.py:806-825`

**What's wrong.** `terminate` starts the drain task and awaits it shielded. The drain stops `/user`, which stops each child by putting `PostStop` on its lane and waiting for its task. An actor that is inside a handler awaiting `terminate()` is waiting for a drain that is waiting for that actor. Nothing breaks the cycle until the tree deadline, when `stop` cancels the actor's task, the shield keeps the drain alive, and shutdown proceeds. The system logs `did not stop within the shutdown deadline while handling ...; cancelled` about the actor that asked for the shutdown.

Nothing in the docs says `terminate` may not be awaited from an actor, and the pattern is natural: an actor that sees `ClusterDowned`, or a session actor that decides the process is done. `Cluster._terminate_when_downed` knows about this and spawns the shutdown as a separate task, with a comment explaining why, so the trap is known to one caller and hidden from users.

**Why it matters.** A ten-second stall (the default `shutdown_timeout`) on every such shutdown, a misleading warning naming an innocent actor, and a `CancelledError` thrown into a handler that thought it was doing the right thing. Observed with a 0.5 s deadline: `elapsed: 0.50s` and the warning above.

**Proposed fix.** Either make `terminate` detect that it was called from a cell's task and return after starting the drain rather than awaiting it, or document the rule and offer the non-awaiting form explicitly:

```python
def terminate_later(self) -> asyncio.Task[None]:
    """Start the drain and return at once. For calling from inside an actor."""
    return self._begin_termination()
```

Detection is cheap: the cell task names are `tapio-cell:...`, but a cleaner signal is a contextvar set by `_run` for the duration of a message. `terminate` would then log at debug and return the drain task without awaiting.

**Test to prove it.**

```python
async def test_terminate_from_inside_a_handler_does_not_wait_the_deadline():
    settings = IsolatedTapioSettings(shutdown_timeout=timedelta(seconds=2))
    system = ActorSystem("t", settings)

    async def on_message(message: Ping) -> Behavior[Ping]:
        await system.terminate()   # or the non-awaiting form
        return Behaviors.stopped()

    ref = system.spawn(Behaviors.receive_message(on_message, msg_type=Ping), "a")
    started = time.monotonic()
    ref.tell(Ping())
    await system.when_terminated()
    assert time.monotonic() - started < 1.0   # 2.0 before the fix
```

### [T-10] The page says `Terminated` always comes with a quarantine; a failed link tells watchers and re-dials

**Severity:** Medium
**Category:** Docs
**Status:** Confirmed
**Location:** `docs/unreachable.md:13-24`, `docs/unreachable.md:61-65`, `src/tapio/remote/association.py:1102-1131`, `src/tapio/remote/failure.py:349-355`

**What's wrong.** The page states that "three things happen together" when silence outlasts `unreachable_after`: watchers are told, the association is quarantined, and `PeerUnreachable` is published. It then says "Recovery is explicit, never automatic", because "watchers here were already told `Terminated` for actors that are alive". The code tells watchers on *every* association end, quarantine or not:

```python
def _end_watches(self) -> None:
    ...
    for watchee, watchers in watching.items():
        ref = self._host.peer_ref(self._peer, watchee)
        for watcher in list(watchers.values()):
            watcher.notify_unreachable(ref, detail)
```

A link that ends with an `OSError`, a peer that closes the socket, or a refused frame all reach this with `_quarantined` false, and the next send dials again. `PeerUnreachable.quarantined` documents that second case, and `test_killing_the_peer_system_terminates_every_watcher` asserts it, so the behaviour is intended. The page does not say so, and its central argument (a link must not come back quietly after watchers were told) applies to a case the code does not handle that way.

**Why it matters.** A reader who plans around the page expects that a `Terminated` from a remote actor means the address is frozen until `reconnect`. In fact a transient socket reset delivers `Terminated` to every watcher and the ref keeps working a moment later, which is the "two nodes with contradictory beliefs" the page says is prevented. Whether that should change is a design question (see section 4); that the page describes a different rule from the code is a High-consequence doc error by the review's own rubric, softened to Medium because the `PeerUnreachable` docstring gets it right.

**Proposed fix.** Add a paragraph after "How a peer is declared unreachable":

> A link can also end without a verdict: the socket fails, the peer closes it, or a frame is refused. Watchers are told `Terminated` in that case too, because the association that held their watches is gone, and `PeerUnreachable` is published with `quarantined=False`. The address is not frozen. The next send dials again, and a watcher that wants to depend on the actor again has to watch it again. Only silence quarantines.

And qualify "Recovery is explicit, never automatic" with "after a quarantine".

**Test to prove it.** Not applicable for a docs finding. The existing test named above is the evidence.

### [T-11] Two pages still say clustering does not exist

**Severity:** Medium
**Category:** Docs
**Status:** Confirmed
**Location:** `docs/getting-started.md:282-284`, `docs/unreachable.md:43-46`, `docs/getting-started.md:376-441`

**What's wrong.** The getting-started page says, after the partition example: "Fixing that needs enough nodes to hold a vote ... That is clustering, and it is not in this version." The unreachability page says split brain "needs membership and a quorum, which v0.1 does not have." Both pages elsewhere link to the clustering page, and the README, the index page and `docs/clustering.md` describe membership, downing strategies and a lease as shipped. The list "What the runtime gives you today" on the getting-started page also omits clustering entirely.

**Why it matters.** A new reader reaches the partition example, is told the fix does not exist, and stops. The rubric for this review puts a wrong claim that misleads a caller at High; this one misleads about scope rather than into a bug, so Medium.

**Proposed fix.** Replace the sentence at `getting-started.md:284` with: "That is clustering. With a downing strategy configured, the losing side of a partition downs itself; the clustering page has the strategies and what each one promises." Replace "which v0.1 does not have" at `unreachable.md:46` with "which a bare pair of nodes does not have; a cluster with a downing strategy does". Add a bullet for `Cluster`, `ClusterSingleton` and `Routers.group` to the "What the runtime gives you today" list.

### [T-12] Docstrings say clustering replaces the decider and the peer provider; nothing does

**Severity:** Medium
**Category:** Docs
**Status:** Confirmed
**Location:** `src/tapio/remote/failure.py:7-19`, `src/tapio/remote/failure.py:297-306`, `src/tapio/remote/peers.py:11-16`, `src/tapio/remote/endpoint.py:609-629`, `src/tapio/remote/association.py:275-279`, `src/tapio/cluster/cluster.py:163-181`

**What's wrong.** `failure.py` says a `DownDecider` "says yes, alone, immediately. Clustering replaces it with a strategy over converged membership, so that a minority partition stops itself rather than both halves declaring the other dead." `peers.py` says "Clustering answers the same question from membership instead: a member that the cluster has downed is refused". Neither happens. `Association._decider` is assigned `DownAlone()` in `__init__` and never reassigned; there is no setter. `RemoteEndpoint.use_peers` is called from two tests and from nowhere in `src`. The cluster works around the association's lone verdict by calling `clear_quarantine` on every alive member every heartbeat round (`_keep_knocking`), which the clustering page documents under "One rule this contradicts". A downed member is not refused by remoting at all; it is simply no longer forgiven.

**Why it matters.** A reader of the remoting layer is told that the cluster changes the verdict at the source. It does not; the association still quarantines a cluster member on its own, watchers are still told `Terminated`, and the daemon undoes the quarantine a round later. Anyone trying to reason about the false-positive rate in a cluster from these docstrings gets it wrong. The interfaces also read as load-bearing when one of them is unreachable code.

**Proposed fix.** Rewrite both docstrings to describe what happens: the association decides alone in every deployment; a cluster relents on members every round and leaves a downed member unforgiven. Then either wire `use_peers` from `Cluster._build` with a `MembershipPeers` provider that refuses `Down` and `Removed` members, which would make the docstring true and give the daemon's `relent` a partner, or delete `use_peers` and `PeerProvider.give_up/relent` as dead code (see T-19). The first is the better outcome: refusing a downed member at the transport is the guarantee the clustering page implies with "a `Down` cannot be taken back".

### [T-13] Deferred construction has four call sites and three failure policies

**Severity:** Medium
**Category:** Quality
**Status:** Confirmed
**Location:** `src/tapio/actor/cell.py:492`, `src/tapio/actor/cell.py:1043`, `src/tapio/actor/cell.py:1143`, `src/tapio/actor/cell.py:1171-1218`, `src/tapio/remote/spawner.py:497-514`, `src/tapio/testkit/behavior.py:454-477`

**What's wrong.** `_evaluate` runs user factories. It is called from `start` (failure propagates to the spawner, children leak: T-03), from `_restart` (failure is caught and the actor stops), and from `_become` (failure escapes the task: T-02). The spawner adds a fourth site with its own partial `except` list (T-05), and the test kit re-implements the unwrapping loop in `_resolve` without `UnstashBehavior`. Three of the four bugs above are the same bug: the code that runs a factory does not own what happens when the factory raises.

This is a single-responsibility problem in `ActorCell`, which at 1,466 lines owns the receive loop, supervision, restart and backoff, both sides of death watch, adapters, timers, the stash, dead-letter accounting and the context. `DeathWatch`, `RestartLog` and `AdapterRegistry` were already extracted; construction was not.

**Why it matters.** Each new call site of `_evaluate` will reinvent the failure policy, and the existing ones already disagree. Fixing T-02, T-03 and T-05 one at a time leaves the fourth site to be found by the next reviewer.

**Proposed fix.** Give the cell one method that evaluates a behavior and accounts for failure, and route every site through it:

```python
async def _construct(self, behavior: Behavior[T], *, keep_supervisors: bool) -> Behavior[T] | None:
    """Evaluate deferred construction; on failure stop what it spawned and
    return None so the caller takes a decision rather than propagating."""
```

with `start` translating `None` into an exception for the spawner after `_finish`, `_become` and `_restart` translating it into `_on_failure` and `_stop_self` respectively, and the spawner relying on `ctx.spawn` never leaking. Move `_evaluate`'s unwrapping loop into `behavior.py` so the test kit shares it instead of copying it.

**Test to prove it.** The tests under T-02, T-03 and T-05 together.

### [T-14] A simultaneous dial resolved during a stalled write closes the surviving association

**Severity:** Medium
**Category:** Bug
**Status:** Suspected
**Location:** `src/tapio/remote/association.py:499-534`, `src/tapio/remote/association.py:536-553`, `src/tapio/remote/association.py:703-741`

**What's wrong.** `adopt` swaps the socket under a running association and retires the old handle in a new task, whose first act is `previous.close()`, which closes the old link. The association's actor may at that moment be parked in `_write` on that old link:

```python
link = self._link
if link is None:
    self._hold(outbound)
    return
try:
    async with asyncio.timeout(self._write_budget()):
        await link.write_frame(outbound.frame)
except TimeoutError:
    ...
except OSError as error:
    ...
    self.close(f"the link failed while writing: {error}")
```

`link` is a local captured before the swap. If `drain()` was waiting because the peer's receive window was full (the docstring on `adopt` says both sides connecting at once "is normal under load"), closing the writer makes `drain` raise a `ConnectionResetError` or similar `OSError`, and `_write` closes the whole association, including the link that just won the race. The frame is dead-lettered as `link-failed` although a working link had just been adopted.

**Why it matters.** The design says the losing side "survives. The queue, the mailbox, every ref pointing through it and every watch across it are unchanged." In this interleaving they are all torn down and every local watcher gets a false `Terminated`.

**What I could not verify.** Whether `StreamWriter.drain()` raises rather than returning when the transport is closed under it while paused, on the loop implementations tapio supports. Reading `asyncio.streams.StreamWriter.drain` and `FlowControlMixin._drain_helper` suggests it raises the connection-lost exception, which is an `OSError` subclass. A test would need to fill the old link's send buffer (a peer that reads nothing) and then trigger `adopt` from an inbound handshake.

**Proposed fix.** In `_write`, treat a failure on a link that is no longer `self._link` as a swap rather than a failure:

```diff
 except OSError as error:
+    if link is not self._link and not self._closing:
+        # The socket was swapped by a simultaneous dial while this write
+        # was parked. Re-queue the frame for the link that won.
+        self._hold(outbound)
+        return
     if isinstance(outbound, Outbound):
         self._dead_letter(...)
     self.close(f"the link failed while writing: {error}")
```

The same guard belongs in `_beat`.

**Test to prove it.** Sketch: two systems where `alpha` sorts first; install a link wrapper on `alpha` whose `write_frame` blocks until an event; have `alpha` send enough to park the writer; make `beta` dial `alpha` so `alpha`'s `_adopt` runs `existing.adopt`; release the event; assert `alpha.remote.associations` still holds `beta` and a probe on `beta` receives the parked frame.

### [T-15] `ask` with a union reply type raises `AttributeError`

**Severity:** Low
**Category:** Bug
**Status:** Confirmed
**Location:** `src/tapio/actor/ask.py:265-268`, `src/tapio/actor/ask.py:335`, `src/tapio/actor/ask.py:360`, `src/tapio/remote/ref.py:253-257`

**What's wrong.** `normalize_msg_type` accepts unions, `TestProbe` and `resolve` accept unions, and `ask_through` normalizes `expect`. But `ask`, `RemoteRef.ask` and the timeout message all read `expect.__name__`, and `types.UnionType` has no `__name__`. `await ref.ask(make, expect=Spawned | SpawnFailed)` fails with `AttributeError: 'types.UnionType' object has no attribute '__name__'` before sending anything. The `SpawnReply` base class exists partly to sidestep this, but nothing says a union is refused.

**Why it matters.** A confusing crash from a call the surrounding API suggests is valid; `mypy` catches it only when the caller is type-checked.

**Proposed fix.** Use `_describe` from `tapio.validation` (make it public) wherever `expect.__name__` is read, so a union renders as `A | B`, and the type annotation can widen to `MessageType`.

**Test to prove it.**

```python
async def test_ask_accepts_a_union_reply_type(system):
    replier = system.spawn(Behaviors.receive(_answer_with_pong), "r")
    reply = await replier.ask(lambda r: Ping(n=1, reply_to=r), expect=Pong | Other)
    assert isinstance(reply, Pong)
```

### [T-16] A failure inside the drain leaves `when_terminated` waiting for ever

**Severity:** Low
**Category:** Bug
**Status:** Suspected
**Location:** `src/tapio/actor/system.py:644-673`

**What's wrong.** `_drain` sets `self._runtime.terminated` and `self._terminated` only after the three awaits succeed. If `_user.stop`, `_system.stop` or `_blocking.shutdown` raises anything but `CancelledError`, the drain task ends with an exception, `terminate()` re-raises it once to its caller, and every `when_terminated()` waits for ever because the event is never set.

**What I could not verify.** A path by which `stop` raises. `cancel_and_wait` swallows the awaited task's exceptions, `asyncio.gather` re-raises a child's failure only if a `stop` raised, and I found no such raise. The hazard is structural rather than demonstrated.

**Proposed fix.** Put the last three lines of `_drain` in a `finally`, and record the exception as `self._failure` so `when_terminated` reports it rather than hiding it.

### [T-17] `decode` re-serializes the payload the encoder was careful not to

**Severity:** Low
**Category:** Quality
**Status:** Confirmed
**Location:** `src/tapio/remote/codec.py:163-167`, `src/tapio/remote/codec.py:228-257`, `src/tapio/remote/codec.py:326-328`

**What's wrong.** `encode` splices the header and the payload as text with a comment: "parsing it just to serialize it again would double the cost of every send." `decode` then does the opposite on every receive: `json.loads` the whole body, `json.dumps(payload)` to get text back, and `model_validate_json` to parse it a second time. Every inbound message is parsed twice and serialized once for nothing.

**Why it matters.** The README puts remote throughput at 19x below local and names the codec as the cost. This is a measurable slice of it, and it contradicts the encoder's own reasoning.

**Proposed fix.** Read the header without materialising the payload. The payload is the last member and the frame is produced by this library, so either scan for `,"p":` after parsing the fixed header prefix, or keep a `json.loads` of the whole body and validate with `model_validate(payload)` (Python objects, no second parse) in non-strict mode, which is what `model_validate_json` does internally anyway. The first keeps strict JSON validation; the second is a one-line change and halves the parsing.

### [T-18] A restart racing a parent's stop logs a traceback for a benign race

**Severity:** Low
**Category:** Quality
**Status:** Confirmed
**Location:** `src/tapio/actor/cell.py:1042-1050`, `src/tapio/actor/cell.py:582-588`

**What's wrong.** When a parent's stop sweep sets `_terminating` while a child is mid-restart, the child's re-run `setup` calls `ctx.spawn`, which raises `ActorSystemTerminating`, which `_restart` catches and logs with `_log.exception("failed while restarting; stopping")`. The outcome is right, the log is wrong: an error with a traceback for an ordinary shutdown ordering.

**Proposed fix.** Check `self._terminating` after `_stop_children` in `_restart` and return quietly, and catch `ActorSystemTerminating` separately with a debug line.

### [T-19] Unused public members: `describe_blocking`, `last_heard`, `logged`, `use_peers`

**Severity:** Low
**Category:** Dead code
**Status:** Confirmed
**Location:** `src/tapio/dispatch/blocking.py:177-187`, `src/tapio/remote/failure.py:97-100`, `src/tapio/actor/dead_letters.py:228-231`, `src/tapio/remote/endpoint.py:609-629`, `src/tapio/remote/endpoint.py:713-723`

These are dead in the sense that nothing in `src`, `tests`, `examples` or `docs` references them, checked by grep. Vulture at 60% confidence flagged 69 candidates; the rest are public API that tests or docs use, or properties exposed for tests by design and documented as such (`Mailbox.user_size`, `ActorCell.watchers`, `Cluster.heartbeats_sent`), which I do not count.

- **`describe_blocking`** (dispatch, `blocking.py:177`): kept, per its docstring, "because it is a published name that the reference page renders". It is not in the module's `__all__`, no docs page names it, and nothing imports it. Remove it.
- **`DeadlineDetector.last_heard`** (remote, `failure.py:97`): never read.
- **`DeadLetterOffice.logged`** (actor, `dead_letters.py:228`): never read; `total` is read by tests.
- **`RemoteEndpoint.use_peers`** (remote, `endpoint.py:609`): called only by two tests. Either wire it from the cluster (T-12) or remove it with the `give_up`/`relent` half of `PeerProvider`.
- **`RemoteEndpoint.forget_all`** (remote, `endpoint.py:713`): test-only by its own docstring, shipped in the production module. Not dead, but belongs in `tapio.testkit.remote` next to `link_faults`.

Not dead, and worth saying why: `Reachability.empty` and `VectorClock.empty` are used by tests only but are the documented constructors for a fresh table; `Behaviors.empty/ignore`, `SupervisorStrategy.resume/escalate`, `TimerScheduler.start_fixed_rate`, `StashBuffer.unstash_all`, `Cluster.leave/when_downed/join_seed_nodes` are public API exercised by tests and examples.

### [T-20] Tests that sleep a guessed duration before asserting

**Severity:** Low
**Category:** Quality
**Status:** Confirmed
**Location:** `tests/actor/test_dead_letters.py:212`, `tests/actor/test_dead_letters.py:237`, `tests/actor/test_dead_letters.py:288`, `tests/actor/test_dead_letters.py:387`, `tests/actor/test_supervision.py:220`, `tests/actor/test_supervision.py:616`, `tests/actor/test_timers.py:108-215`

**What's wrong.** The testing page says: "poll it against a deadline rather than sleeping for a guessed duration". `tests/failures.py` provides `eventually` for exactly that. Of forty `asyncio.sleep` calls in the suite, most are deliberate (a handler that wedges for 30 s, a fault injector's delay, the polling inside `eventually`). About ten are the pattern the page warns against: `ref.tell(...)`, `await asyncio.sleep(0.05)`, `assert seen == [...]`. On a slow CI leg these are the tests that fail without a code change.

**Proposed fix.** Replace each with `await eventually(lambda: len(seen) == 3)` before the exact assertion. The timer tests are harder, since they measure a schedule; those can subtract a tolerance rather than sleep a fixed amount, as `well_inside` in `tests/failures.py` already does elsewhere.

Positive note: the merge laws are property-tested with Hypothesis in `tests/cluster/test_gossip.py`, `test_reachability.py`, `test_member.py` and `test_clock.py`, over commutativity, associativity and idempotence, which is the test this review would otherwise have asked for. No test calls `cell.abort()`.

### [T-21] `resolve` skips the `expect` check for a local address though its docstring says it raises

**Severity:** Low
**Category:** Docs
**Status:** Confirmed
**Location:** `src/tapio/actor/system.py:464-481`

**What's wrong.** The docstring lists `MessageTypeError: If expect is not a Message subclass or a union of them.` The local-address branch returns before `normalize_msg_type(expect, ...)` runs, so `await system.resolve("tapio://t/user/x#1", expect=int)` succeeds for a local path and raises for a remote one.

**Proposed fix.** Move the `normalize_msg_type` call above the local branch, or say in the docstring that the claim is checked only for a remote address.

### [T-22] Every remote send re-validates the `Outbound` wrapper when `validate_on_tell` is on

**Severity:** Low
**Category:** Quality
**Status:** Confirmed
**Location:** `src/tapio/remote/association.py:403-404`, `src/tapio/actor/cell.py:524-530`, `src/tapio/validation.py:172-186`

**What's wrong.** `Association.send` delivers an `Outbound(payload=message, frame=frame, recipient=recipient)` through the association actor's ordinary `tell`, so the cell validates it like user traffic: an `isinstance` against the `AssociationMessage` union plus, under `validate_on_tell`, a strict `TypeAdapter` re-validation of a wrapper the runtime built a microsecond earlier, including the `bytes` frame and the `ActorPath`. The message itself was already validated by `RemoteRef.tell`. This is runtime-internal traffic paying the user-facing check.

**Proposed fix.** Spawn the association actor with a validator that checks the type only, for instance by building it through a runtime-internal spawn that passes `validate_on_tell=False` for that cell, or by letting `Association.send` call `cell.deliver` directly, which skips validation by design.

### [T-23] A cluster events test passes while the daemon it exercises has died

**Severity:** Low
**Category:** Quality
**Status:** Confirmed
**Location:** `tests/cluster/test_events.py:17-34`, `tests/cluster/test_events.py:70-86`

**What's wrong.** The recorder behavior declares `MemberUp | MemberRemoved`, and the test subscribes it to every event. The daemon's replay delivers the two `MemberUp` events, then `LeaderChanged`, which the recorder's type rejects, and the daemon stops (T-04). The test's `eventually` sees the two `MemberUp` entries it wanted and passes. A test that checks the daemon is still alive at the end, or a recorder declared as `ClusterEvent`, would have caught T-04 months ago. The `MessageTypeError` shows up intermittently as an unraisable warning attributed to whichever test the collector interrupts, which is how it was noticed on issue #140 and mistaken for that issue.

**Proposed fix.** Declare the recorder as `ClusterEvent`, since that is what "asked for nothing" means, and end every daemon test by asserting the daemon's well-known name still resolves:

```python
assert nodes[0].system.refs.lookup(daemon_path(nodes[0])) is not None
```

Better still, make the `cluster_of` fixture assert it on the way out, the way `actor_system` asserts no leaked tasks: a daemon that died during a test is a failed test whatever else the test observed.

## 4. Design disagreements

These are not bugs. Each is a documented decision I would argue against, with the reason.

**D-1. A partition resolved by a strategy can run two singleton instances for a few seconds, and the docs do not say so.** The singleton page argues carefully about the graceful-leave ordering (and issue #64 fixed that path), then says a crash "is only ever seen as removal". On a partition, each side computes the verdict after its own `down_after`, measured from when *it* first saw the split, so the winner can down and remove the loser a round or two before the loser decides it lost. The winner's manager starts the instance on `MemberRemoved`; the loser's manager keeps its instance until the loser's own `_apply_downing` and the application's shutdown run. The manager does not subscribe to `SelfDown` at all. Akka's split-brain resolver adds a `down-removal-margin` on the surviving side for exactly this window. I would have the manager hand off on `SelfDown`, and add a configurable margin between `MemberRemoved` and starting a successor.

**D-2. A ref inside any message makes this node dial whatever address a peer named; a heartbeat is refused the same courtesy.** `_answer` in the daemon refuses to dial an address that only the message vouches for, with a paragraph on why ("would let any peer that has finished a handshake make this node open a connection to any host and port"). `receive_frame` resolves every `ActorRef` field through `resolve_peer`, which builds a `PeerOutbox` to any addressable address, and the first `tell` dials it. The security page says the shared secret is the whole authorization model, so this is inside the trust boundary as stated. The inconsistency is that the cluster code treats the same capability as a hazard worth refusing. Either the heartbeat rule is overcautious or the codec rule is undercautious; I would apply the daemon's rule at the endpoint (dial only members and addresses this system was configured with or already has a link to) and make the exception explicit.

**D-3. Watchers are told `Terminated` when a link ends for any reason, then the ref quietly works again.** This is T-10's subject as a docs finding; as a design it is the case the unreachability page argues against. Since a non-quarantined link end is by construction not a judgement about the peer, the consistent options are to tell watchers only on a quarantine (and re-register watches on the new link), or to quarantine on every link end. The first keeps the promise the page makes.

**D-4. Every send to a peer that refuses connections re-dials with no backoff.** An association that fails to connect closes, is forgotten, and the next `tell` spawns a new actor and dials again. A hot sender to a dead-but-not-quarantined peer therefore spawns an actor and a TCP connect per round trip time, for ever. The docs promise that such sends dead-letter rather than hang, which holds. A short exponential backoff on redial, or a quarantine after N consecutive connection failures, would bound the cost. `Backoff` already exists and is pure.

**D-5. `ActorRef` equality ignores the node.** Folded into T-01's fix, but listed here because the docstring ("Refs are equal when they address the same incarnation") is a design statement that is only true inside one system.

## 5. Coverage of this review

**Tooling run, with raw output kept in the review's working notes:** `make lint` (clean), `make type` (`mypy --strict` over `src`, `examples` and `tests`: clean), `uv run coverage run -m pytest` (843 passed, 8 benchmark skips, 95% line and branch coverage), `make examples` (29 passed), `uv run ruff check --select ALL --statistics` (2,264 hits, dominated by `S101` asserts in tests, `COM812`, `CPY001` copyright headers and `ARG001`; nothing in the disabled set points at a bug), `vulture --min-confidence 60` (69 candidates, of which four are dead, see T-19). The first tooling run failed on a network timeout installing `charset-normalizer`; the rerun with a longer timeout was clean. That is the environment, not the repository.

**Read in full:** every module under `src/tapio` (65 files), `README.md`, `AGENTS.md`, `CLAUDE.md`, `Makefile`, `pyproject.toml`, `mkdocs.yml`, every page under `docs/` including the playground page's loader, `.github/workflows/ci.yml`, `tests/conftest.py`, `tests/internals.py`, `tests/cluster/conftest.py`, `tests/remote/conftest.py`, `tests/cluster/test_group_router.py`, `examples/tapio_examples/hello_world.py`, `tests/examples/test_suite.py` (first half).

**Skimmed:** `tests/remote/test_watch.py`, `tests/actor/test_cell.py`, `tests/actor/test_dead_letters.py`, `tests/remote/peers.py`, `tests/failures.py`, the sleep and Hypothesis usage across `tests/` by grep, and the imports and system names across `examples/`.

**Not reached:** the bodies of the other 28 example modules (they are asserted by `tests/examples/test_suite.py`, which passes, so I trusted that the code samples included in the docs run); `tests/benchmarks/`; `scripts/stage_playground.py`; the bodies of most test files beyond the ones named. The merge laws were checked by reading `member.py`, `reachability.py`, `clock.py` and `gossip.py` and by confirming the Hypothesis tests exist and pass, not by re-deriving every tie-break. T-14 and T-16 are marked Suspected because I did not build the interleaving they need.

**Module map used for the review.**

| Module | Responsibility | Owns | Depends on |
|---|---|---|---|
| `message` | the `Message` base: frozen, revalidated | nothing | pydantic |
| `validation` | declared-type checks and the delivery validator | `MessageType` | `message`, `settings`, `actor.path` |
| `settings` | every tunable, env-backed | defaults | `actor.mailbox` |
| `errors`, `logging`, `version` | error hierarchy, path-tagged loggers, the tag-derived version | nothing | stdlib |
| `actor.path` | `ActorPath`, the tree identity | element grammar | nothing |
| `actor.ref` | `ActorRef` base and its pydantic schema | equality by path | `remote.address`, `remote.context` |
| `actor.mailbox` | two-lane queue, overflow, `offer` waiters | the single-consumer rule | `actor.signals` |
| `actor.cell` | receive loop, supervision, restart, children, watch, timers, stash, adapters, context, termination | every task an actor has | almost everything |
| `actor.behavior` | behaviors, directives, factories, type resolution | `msg_type` | `validation` |
| `actor.supervision`, `actor.restarts` | strategies, backoff, per-layer restart budget | nothing | nothing |
| `actor.watch`, `actor.signals` | the two watch protocols and the per-actor book | watcher map | nothing |
| `actor.ask` | `PromiseRef` and `ask_through` | promise registration | `actor.watch`, `validation` |
| `actor.timers`, `actor.stash`, `actor.adapter` | per-cell facilities that outlive an incarnation | their tasks and entries | `actor.cell` (by reference) |
| `actor.router` | pool router | routees as children | `actor.cell` |
| `actor.dead_letters`, `actor.events` | the sinks | subscribers | `remote.address` |
| `actor.system` | guardians, drain, resolution, `deliver_frame` | the drain task | `remote.endpoint`, `remote.codec` |
| `dispatch.*` | loop ownership, blocking pool, `cancel_and_wait` | threads | nothing |
| `remote.address`, `remote.protocol`, `remote.registry`, `remote.context` | wire identity, version, type keys, deserialization scope | the append-only registries | `actor.path` |
| `remote.codec` | frame format and `receive_frame` | the trust boundary | `remote.registry`, `remote.context` |
| `remote.transport`, `remote.handle`, `remote.handshake` | sockets, framing, TLS, socket ownership, HMAC handshake | one socket per handle | `remote.codec` |
| `remote.association`, `remote.endpoint` | one link per peer as an actor; the listener and the table | reader tasks, handshakes in flight | most of `remote`, `actor.cell` |
| `remote.failure`, `remote.peers` | detectors, the lone decider, refusal table | quarantine | nothing |
| `remote.ref`, `remote.spawner` | `RemoteRef`, `PeerWatch`, remote factories | factory registry | `actor.ask`, `remote.codec` |
| `cluster.clock`, `cluster.member`, `cluster.reachability`, `cluster.gossip` | the pure, mergeable state and leader function | nothing time-dependent | `message` |
| `cluster.monitor`, `cluster.downing` | the ring and the strategies | monitor state | `remote.failure`, `cluster.gossip` |
| `cluster.daemon`, `cluster.cluster`, `cluster.events`, `cluster.messages` | the gossiping actor and its facade | timers, subscriptions | `actor.*`, `remote.*` |
| `cluster.singleton`, `cluster.router` | placement and group routing over membership | the keeper | `cluster.daemon` |
| `cluster.management`, `cluster.cli` | the operator HTTP port and its client | connection tasks | `remote.transport` |
| `testkit.*` | probe, kit, faults, leak checks, fixtures, isolated settings | fault wrappers | `actor.*`, `remote.*` |
