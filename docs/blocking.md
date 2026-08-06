# Blocking calls

This is the sharpest footgun in an asyncio actor system, and it deserves a
page rather than a warning box.

Every actor in a system shares one event loop. An actor that calls something
blocking, a database driver, `requests`, `time.sleep`, hashing a large file,
does not just stall itself. It stalls **every other actor in the system**,
because none of them can run while that thread is inside the call. Nothing
raises and nothing is logged. The system simply stops for a while, and in
production it looks like latency nobody can account for.

The example counts the damage rather than describing it:

```python
--8<-- "examples/tapio_examples/blocking_offload.py"
```

Its output is the whole page in two lines:

```
on the loop: the rest of the system processed 0 messages during the call
run_blocking: the rest of the system processed 48 messages during the call
```

Same call, same duration. In one case the rest of the system got nothing done.

## What `run_blocking` does

`await ctx.run_blocking(fn, *args)` runs `fn` on a bounded pool of threads
that belongs to the system, and hands the result back to the actor. The pool
is sized by `blocking_pool_size` and is created on the first call, so a system
that never blocks starts no threads at all.

It is deliberately not `asyncio.to_thread`. That submits to the loop's default
executor, which is shared with every other library in the process and whose
size tapio does not control, so a bound could not be honoured.

## What it does not do

**The actor is still parked.** It is awaiting, so it is not reading its
mailbox. Messages queue up behind the call, and on a bounded mailbox the
overflow strategy will fire while it waits. The loop is free, which is the
point; this actor is not. An actor that spends most of its time in blocking
calls is a bottleneck of one, and the answer is a pool of them behind a router.

**The call cannot be cancelled.** Python cannot interrupt a thread that is
inside a C call. Cancelling the actor abandons the result and the thread keeps
running. Shutdown drops work that has not started and waits for what has, and
past the deadline it logs what is still running and gives up on it. So pass
whatever timeout the library you are calling offers: a call with no timeout of
its own is a call that can outlive the system that made it.

**It is not a way to get parallelism.** Threads run Python bytecode one at a
time. `run_blocking` is for calls that wait, on a socket, a disk, or a lock
inside a C extension. Work that is genuinely CPU-bound wants a process, and
tapio has no opinion about which one.

## Choosing the pool size

`blocking_pool_size` defaults to 16. It bounds how many blocking calls can be
in flight at once across the whole system, so it is a queue in front of
whatever is on the other side of those calls: a connection pool, a disk, an
API with a rate limit. Sizing it above what that thing can take moves the
queue rather than shortening it.

Every thread in the pool is a real thread with a real stack, so the number is
not free either. If a system needs hundreds of concurrent blocking calls, the
call is the thing to fix, usually by finding the async client for whatever it
talks to.

## The check that keeps it honest

The pool is the one piece of the runtime that is not a task, so the task-leak
invariant does not cover it. `assert_no_leaked_threads()` is the companion,
and `system.blocking.threads` is what it reads: a system that has terminated
must leave no thread behind.
