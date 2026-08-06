# tapio examples

Examples are a first-class deliverable. They are teaching material,
integration tests, and the docs' source of truth at once. This directory is an
importable package (`tapio_examples`), type-checked under mypy strict and run
by CI.

Run one with:

```bash
uv run python -m tapio_examples.<name>
```

Every example follows the same rules. It is runnable on its own, deterministic
with no wall-clock sleeps in its assertions, finishes in under 2 seconds,
describes itself in its module docstring, and is asserted by a test in
`tests/examples/`. Tiers 1 to 3 teach exactly one concept each. Nothing
reaches outside the machine: the remoting examples use loopback ports the OS
picks, so they need no orchestration and no second machine.

## Suggested reading order

### Tier 1: Fundamentals

| Example | Teaches |
|---|---|
| `hello_world` | spawn, `tell`, `ActorRef` as a message field, system shutdown |
| `ping_pong` | bidirectional messaging, `Behaviors.same()` |
| `counter` | mutable state in a class-based behavior; replying via a `reply_to` field |

### Tier 2: Lifecycle and failure

| Example | Teaches |
|---|---|
| `dead_letters` | where a message goes when nobody is there to receive it |
| `supervision_backoff` | `Restart` with exponential backoff and window exhaustion |
| `death_watch` | `ctx.watch`, `Terminated`, evicting a stopped actor from a registry with no leak |
| `escalation` | `ChildFailed`, subtree restart, and escalation reaching the guardian |
| `graceful_shutdown` | SIGINT, bottom-up drain, `PostStop` ordering |

### Tier 3: Patterns

| Example | Teaches |
|---|---|
| `ask_timeout` | `ask` as sugar over `reply_to`; `AskTimeoutError`, no leaks |
| `worker_pool` | round-robin router, bounded mailboxes, backpressure |
| `state_machine` | behavior-switching as a protocol FSM |
| `rate_limiter` | token bucket in one actor, the mailbox as mutex |
| `stash_on_startup` | stashing traffic during startup, then unstashing |

### Tier 4: Realistic composition

| Example | Teaches |
|---|---|
| `chat_sessions` | per-user session actors over a simulated flaky LLM |
| `order_saga` | compensating transactions, unwinding on failure |
| `fastapi_app` | `ActorSystem` in a FastAPI lifespan, and the deployment story |


### Tier 5: Distribution

Every example here starts two systems in one process and shuts both down.

| Example | Teaches |
|---|---|
| `two_nodes` | `resolve`, a `tell` across an association, and a reply arriving through a `reply_to` that crossed the wire |
| `remote_ask` | `ask` across a link, and `AskTargetUnreachable` told apart from a timeout |
| `node_failure` | watching a ref on another node, and rebuilding the work when that node goes |
| `partition` | both nodes alive, both cut off, both wrong about the other, and the explicit repair |
| `remote_spawn` | `@remote_behavior`, a spawner with an allowlist, and the peer supervising its own child |
| `worker_pool_remote` | why `offer` is not end-to-end backpressure across a link, and the credit protocol that is |

Tiers 1, 2, 3 and 5 have landed and run in CI, and Tier 4 has begun with
`blocking_offload`.
