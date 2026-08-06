# Testing actor code

Actors are asynchronous, and a test that pokes one and then hopes is a test
that fails on somebody else's laptop. The TestKit exists so that a test says
what it is waiting for, and fails saying what it was waiting for.

It ships with the library, in `tapio.testkit`, and it is three things for
three kinds of test. Every block below is included from tapio's own suite, so
the code on this page is code that runs.

## A probe, for testing an actor

A `TestProbe` is a real actor in a real system. It has a path, a mailbox and a
ref you can put in a `reply_to` field, so the code under test cannot tell it
from anything else, and no part of the runtime is stubbed out to make the test
pass.

```python
--8<-- "tests/docs/test_testing_page.py:probe"
```

`expect_message` compares by equality, which is what a test should assert
either side of a link: a message that crossed one was rebuilt from JSON, so it
is equal to what was sent and never the same object. When the contents are not
known in advance, `expect_message_of(Greeted)` asserts the type and hands back
the message narrowed to it. `receive()` takes the next message and asserts
nothing about it.

Every wait has a deadline. Running out of one is an `AssertionError` naming
what was expected, where, and how long it waited, because a test that hangs
tells you nothing.

`expect_no_message()` is the one that asserts an absence, and the one wait a
passing test really spends. It is short by default, since the case it catches
is a reply sent immediately.

A probe watches, which is how a test asserts that an actor stopped without
reaching into the runtime:

```python
--8<-- "tests/docs/test_testing_page.py:watch"
```

Note what stops that worker: a message its behavior answers with
`Behaviors.stopped()`. Reaching for the cell and cancelling it would not be a
supervision decision, and the `CancelledError` that followed would land in the
test rather than in the runtime.

A probe is also how an absence becomes observable, by subscribing it to the
dead letter stream:

```python
--8<-- "tests/docs/test_testing_page.py:dead_letters"
```

Without that, "the message was dropped" and "the code never ran" look
identical from outside.

## A kit, for testing a behavior

Most of what there is to test about a behavior is the function it is: what it
returns, what it sends, and what it spawns. None of that needs a running
system, and testing it without one makes the test deterministic by
construction. There is no loop to yield to, so there is nothing to poll.

```python
--8<-- "tests/docs/test_testing_page.py:kit"
```

`Behaviors.setup(...)` runs when the kit is built, exactly as a spawn would run
it. Refs handed out by the kit record what they are told instead of delivering
it, which is what makes `self_inbox` and a child's inbox readable. Spawns and
watches are recorded as effects:

```python
--8<-- "tests/docs/test_testing_page.py:effects"
```

Two things the kit deliberately does not do. It does not supervise, so a
handler that raises raises into the test, where a unit test wants it. And it
cannot provide timers, a stash, or a resolved ref, because those belong to a
cell; asking for one is an error naming the alternative, which is a real
system and a probe.

## Fixtures, with nothing to import

tapio registers a pytest plugin through an entry point, so installing the
library is all it takes. The test above asked for `actor_system` and
`make_probe` by name and no `conftest.py` was involved.

`actor_system` is a running system, terminated however the test ends, and it
asserts on the way out that the test left no task and no thread behind.
`make_probe` builds probes in it. `tapio_settings` is what the system is built
from, and overriding that fixture is how a test changes them, including
switching remoting on.

The fixtures are async, so an asyncio test runner has to be installed.
`pytest-asyncio` in auto mode is what tapio's own suite uses.

## The invariant the suite has to keep honest

The runtime does not use `asyncio.TaskGroup`, because a task group cancels
siblings when one fails and supervision needs the opposite. The price is that
"no orphaned tasks" is an invariant tapio holds rather than one the language
enforces, which makes it a thing to assert:

```python
--8<-- "tests/docs/test_testing_page.py:leaks"
```

Tasks already running when the block opens are ignored, so both checks nest
inside a test runner that has tasks of its own. The thread check is the
companion for the blocking pool, which is the one piece of the runtime that is
not a task.

## Two nodes, and a network that can be broken

Remoting cannot be tested by waiting, because the interesting failure is
silence. `two_nodes()` starts a pair on loopback ports the OS picks, and the
faults sit inside the link, so nothing real is broken and the system under
test cannot tell the difference:

```python
--8<-- "tests/docs/test_testing_page.py:two_nodes"
```

`heal()` lets the frames through again and repairs nothing by itself: a node
that gave up on a peer stays given up on until `remote.reconnect` says
otherwise, which is the behaviour worth testing. `drop(n)` loses the next few
frames and `delay(seconds)` holds them, which is what makes a failure detector
fire on purpose rather than by sleeping and hoping.

## Waiting for something that has not happened yet

A `tell` returns before the message is handled, so a test that asserts
immediately after one asserts too early. Use a probe: `expect_message` waits
for the effect the test is about. When the effect is runtime state rather than
a message, poll it against a deadline rather than sleeping for a guessed
duration, so that a passing test costs a millisecond instead of the guess.
