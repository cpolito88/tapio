# tapio examples

Examples are a first-class deliverable: teaching material, integration tests,
and the docs' source of truth at once. This directory is an importable package
(`tapio_examples`), type-checked under mypy strict and executed by CI.

Run one with:

```bash
uv run python -m tapio_examples.<name>
```

Every example obeys the same rules: runnable standalone, deterministic (no
network, no wall-clock sleeps in assertions), under 2 seconds, self-describing
in its module docstring, asserted by a test in `tests/examples/`, and, for
Tiers 1-3, teaching exactly one concept.

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
| `death_watch` | `ctx.watch`, `Terminated`, evicting a child with no leak |
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
| `fastapi_app` | `ActorSystem` in a FastAPI lifespan; the deployment story |
| `blocking_offload` | `ctx.run_blocking`, counted against the same code without it |

*No examples have landed yet: the tables above are what they will fill in.*
