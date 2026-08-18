# Technical Debt Backlog

**Repository**: tapio (distribution `tapio-py`)
**Analysis date**: 2026-08-18
**Scope analyzed**: The full shipped library under `src/tapio/`: the actor
runtime (`actor/`), remoting (`remote/`), clustering (`cluster/`), the
dispatch layer (`dispatch/`), the shipped `testkit/`, and the cross-cutting
modules (`message.py`, `validation.py`, `settings.py`, `errors.py`,
`logging.py`). Every source file was read in full.
**Excluded**: `tests/` (read selectively, to judge coverage of the findings
below, not audited for its own debt), `examples/tapio_examples/` (dev-only,
never shipped), `docs/` (prose, not code), and the benchmark harness under
`tests/benchmarks/`. The process-wide message/factory registries were reviewed
but their append-only-for-the-life-of-the-process design is deliberate and
documented, so it is not flagged.

## Summary
This is a mature, unusually disciplined codebase: a typed exception hierarchy,
`mypy --strict`, ruff with the docstring and annotation rule sets on, a test
tree that mirrors the source, and roughly 93% coverage. Docstrings are
consistent Google style and explain *why*. There are no `TODO`/`FIXME`
markers, no bare `except`, and no `type: ignore` in shipped runtime code
(only five in `testkit`, each a documented pydantic-settings `call-arg`
suppression). The design invariants in `AGENTS.md` hold in the code I read.
The three items worth acting on are: (1) a remote spawner can be crashed by a
factory whose arguments model carries an `ActorRef`, which contradicts both a
documented invariant and the "one bad request must not stop the spawner"
guarantee; (2) `Reachability` ships two byte-for-byte identical public methods
(`status` and `says`); (3) a small set of open questions for the team, below.
For the overwhelming majority of the code, "no change needed" is the honest
verdict.

## Conventions in use
Inferred from reading every module and from `pyproject.toml`/`AGENTS.md`:
- **Naming**: `snake_case` functions/methods, `PascalCase` classes and
  `Message` subclasses, `_leading_underscore` for private members, module-level
  `UPPER_SNAKE` constants each carrying a one-line docstring. Followed
  everywhere.
- **Errors**: one hierarchy rooted at `TapioError`; no tapio error inherits
  `ValueError` (deliberate, so pydantic does not fold it into a
  `ValidationError`). "Errors about the message raise at the send site; errors
  about the recipient become dead letters" is applied consistently.
- **Logging**: `runtime_logger(name)` for runtime subsystems, path-tagged
  `ctx.log`/`actor_logger(path)` for actors. Uniform.
- **Typing**: full annotations under `mypy --strict`; `MessageType` alias and
  `TypeVar`s bound to `Message`. No partial hints found.
- **Docstrings**: Google style, single backticks, mkdocstrings cross-refs,
  plain English, no em dashes. The most consistent dimension of the codebase.
- **Async**: asyncio-native, one loop per system funnelled through
  `Dispatcher`.
The only genuine divergence from the codebase's own conventions is the
duplicated method pair in TD-02.

## Quick wins (< 1 day)

- [ ] **[TD-01]** `src/tapio/remote/spawner.py:446-453` — Stop a spawner from crashing when a factory's arguments model carries an `ActorRef`
  - **Severity**: Medium
  - **Category**: Bug
  - **Problem**: `_answer` builds the arguments model with
    `factory.arguments(message.args)` (`spawner.py:447`), which calls
    `args_type.model_validate(...)` (`RemoteFactory.arguments`,
    `spawner.py:265-277`). That runs *outside* any deserialization context (the
    decode's `use_context` has already exited by the time the spawner's handler
    runs). If the arguments model declares an `ActorRef` field, its validator
    calls `resolve_ref`, which finds no ambient context and raises
    `RefResolutionError` (`remote/context.py:96-103`). `RefResolutionError`
    derives from `TapioError`, not `ValueError` (`errors.py:69`), so the
    `except ValidationError` around the call (`spawner.py:448`) does not catch
    it. It propagates out of the `on_spawn` handler, becomes a supervision
    failure, and the default decision stops the spawner and every actor it has
    started. This contradicts two documented guarantees: the module docstring
    states "Arguments models may not carry an `ActorRef`" (`spawner.py:298-304`)
    yet nothing enforces it, and `_answer`'s own contract is that "one bad
    request must not stop the spawner" (`spawner.py:411-420`).
  - **Impact**: A single malformed `Spawn` (or a version-skewed peer whose
    factory registration drifted) takes down a whole spawner and its remote
    children. The requester observes it only as lost links, with no
    `SpawnFailed` reply, which is exactly the failure mode the reply protocol
    exists to avoid.
  - **Fix**: Two options, not mutually exclusive. Defensive (smaller): add
    `RefResolutionError` to the `except` at `spawner.py:448` and reply
    `SpawnFailure.INVALID_ARGS`. Preventive (better): reject at registration in
    `_check_args_type` (`spawner.py:547-558`) any args model that contains a
    ref-typed field, so the misconfiguration fails at import where it is
    written, matching how `remote_behavior` already fails fast on unresolvable
    arguments. Add a test under `tests/remote/test_spawner.py` for a factory
    whose args model holds an `ActorRef`.
  - **Estimate**: 3-5 hours
  - **Confidence**: Reproduced. A live in-process run (a spawner offering a
    factory whose args model declares an `ActorRef`, sent a `Spawn` whose args
    carry a ref string, as a peer's would) raised `RefResolutionError` out of
    `arguments` → `_answer` → the `on_spawn` handler, stopped the spawner
    (`Terminated`), and sent no `SpawnFailed` reply. No existing test exercises
    an args model containing a ref.
  - **Depends on**: -

- [ ] **[TD-02]** `src/tapio/cluster/reachability.py:87-101,131-147` — Remove the duplicate of `Reachability.status`/`says`
  - **Severity**: Low
  - **Category**: Structure
  - **Problem**: `Reachability.status(observer, observed)` (lines 87-101) and
    `Reachability.says(observer, observed)` (lines 131-147) have identical
    bodies: both scan `self.records` for the matching pair and default to
    `REACHABLE`. Only the docstrings differ. Production code and most tests use
    `says` (`daemon.py:464`, `tests/cluster/test_link_bridge.py`); `status` is
    referenced only once, in `tests/cluster/test_reachability.py:18`. Two
    public methods that must stay in lockstep are a latent drift hazard: a fix
    to one (say, the default, or the lookup) silently leaves the other wrong.
  - **Impact**: A future change to reachability semantics can be applied to one
    copy and not the other, producing an inconsistency that only shows up in
    whichever call site happens to use the stale method.
  - **Fix**: Keep `says` (the name used in production), delete `status`, and
    update `tests/cluster/test_reachability.py:18` to call `says`. This is the
    one place the codebase diverges from its own no-duplication norm.
  - **Estimate**: 30 minutes
  - **Confidence**: Verified.
  - **Depends on**: -

## Targeted fixes (1-5 days)
None. No defect or structural problem found in this scope warrants an
effort of this size.

## Structural refactoring (> 5 days)
None. The module boundaries (`actor` / `remote` / `cluster` / `dispatch`) are
clean, the failure-detector and down-decider seams are already interfaces with
a documented second implementation in mind, and the CRDT membership layer is
split into pure values plus a timing daemon exactly as the "test pure functions
as pure functions" goal requires. No large-scale restructuring is justified by
present pain, and the audit bar for proposing one is deliberately high.

## Needs investigation
All four items opened here have been resolved with the maintainer. Kept for the
record, with the decision on each.

1. **Should `_MAX_CATCH_UP` be reachable/tunable?** `actor/timers.py:35` fixes
   the fixed-rate catch-up burst at 10 ticks as a module constant.
   **Resolved: no change.** The drop-and-resync path is already covered by
   `tests/actor/test_timers.py::test_a_fixed_rate_timer_caps_the_burst_after_a_long_stall`
   (asserts "ticks behind and dropped them"). The constant stays
   non-configurable: no present need justifies a per-timer knob.

2. **`Heartbeat` names two unrelated frame types.** `remote/transport.Heartbeat`
   (a `LinkFrame`, link liveness, used in `association.py:726`) and
   `cluster/messages.Heartbeat` (a `WireMessage`, member liveness, used in
   `daemon.py:241,364`) share a class name across layers. Confirmed that no
   module imports both, so there is no bug. **Resolved: leave as-is.** The
   collision is contained and both are documented; a rename is not worth the
   churn.

3. **`expect_no_message` bypasses `receive`'s queue discipline.**
   `testkit/probe.py:228` awaits `self._messages.get()` directly. A message
   arriving after the window closes is left on the queue for the next
   `receive`, and there was no test pinning that contract; there is also a
   theoretical tie race if a message arrives in the same instant the window
   times out. **Resolved: regression test added.**
   `tests/testkit/test_probe.py::test_expect_no_message_leaves_a_later_message_for_receive`
   pins the common-case contract; the tie boundary is called out in the test's
   docstring and left unhardened, since this is a negative-assertion test helper
   and the impact is negligible.

4. **Confirm the TD-01 crash against a live spawner.** **Resolved: reproduced.**
   A live in-process run confirmed the crash (see TD-01's updated Confidence).
   The reproduction doubles as the shape of the regression test that should
   accompany the TD-01 fix.
