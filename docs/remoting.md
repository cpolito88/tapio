# Remoting

tapio can address actors on another node. Two systems that have configured
remoting can send to, ask, and watch each other's actors, using the same calls
as if the target were in the same process.

That last claim is the one worth being careful about, so this page starts with
exactly how far it goes.

## What is and is not transparent

| | Local | Remote |
|---|---|---|
| `tell` never blocks, never raises about the recipient | yes | yes |
| Ordering, per sender and recipient pair | FIFO | FIFO while the association lives |
| Delivery | at most once | at most once |
| Message identity | `received is sent` | `received == sent` |
| Undeliverable goes to dead letters | yes | yes, on whichever side noticed |
| `watch` fires `Terminated` when the actor stops | always right | **also fires on unreachable, which can be wrong** |
| `ask` failure modes | timeout, target terminated | plus target unreachable |
| Backpressure from `offer` | the receiving mailbox | the local outbound buffer only |
| Supervision of a child | yes | **never crosses the wire** |
| `ctx.spawn` places the actor | yes | **no, placement is a different call** |
| Message types | anything a Pydantic field accepts | JSON-representable and registered |
| Latency and failure | in process | a network is in the middle |

Location transparency in tapio means **addressing is uniform**. An actor
holding a ref sends, asks and watches without knowing which node the target is
on. It deliberately stops in two places, and both are in bold above.

**Failure is not uniform**, because a network is in the middle and no API
hides that. The [unreachability page](unreachable.md) is about the one entry
that can lie to you.

**Placement is not uniform** either. Starting an actor on another node is a
different call, and it is awaited.

## Switching it on

Remoting is off unless it is configured, and when it is configured the port is
bound while the system is being constructed. That is what settles the
canonical address before any ref can write itself down, and it is what makes a
configuration that would listen to the world fail to start rather than fail to
be secure.

```python
--8<-- "examples/tapio_examples/two_nodes.py"
```

A ref that crosses a link is written down as its full address, and `resolve`
turns that string back into a ref. `expect=` is how the caller says what it
believes is at the other end, and the claim is checked against the actor's
real message type where every claim about a peer is checked: on the receiving
node.

## Asking across a link

`ask` works unchanged, and the reply comes back to a promise actor under
`/system/promises`, addressed like anything else.

```python
--8<-- "examples/tapio_examples/remote_ask.py"
```

What is new is a third way for it to fail. Locally an ask times out or the
target stops; remotely the peer can also be unreachable, and
`AskTargetUnreachable` says so at once instead of waiting out a timeout that
was never going to be met. Telling those apart is what lets a caller retry
somewhere else rather than retrying into silence.

## Backpressure does not cross a link

`await ref.offer(msg)` on a remote ref waits for room in **this node's**
outbound buffer. That is a real thing to wait on, and it is a socket that is
not draining, not a worker that is falling behind. The two come apart exactly
when it matters: a worker with a large mailbox reads every frame as it
arrives, so the buffer stays empty, `offer` never waits, and the backlog piles
up on the other node where this one cannot see it.

Nothing in a fire-and-forget wire protocol can do better, so end-to-end flow
control is built out of messages, where the receiver is the one who knows:

```python
--8<-- "examples/tapio_examples/worker_pool_remote.py"
```

The grant is the backpressure. It is chosen by the worker, obeyed by the
producer, and nothing in the transport enforces it.

## Starting an actor on another node

There is no placement setting, and no parent-child relationship across a link.
An actor is started elsewhere by asking a **spawner** there to start it
locally, and what crosses the wire is a key naming a registered factory plus a
model holding its arguments.

```python
--8<-- "examples/tapio_examples/remote_spawn.py"
```

The reason placement is not transparent is supervision. If a local parent
supervised a remote child, every restart, stop and failure report would be a
frame on a link that can be quarantined halfway through the decision, and
supervision is the one thing in this library that has to be able to answer. So
the tree stays inside one node: the spawned actor is the spawner's child,
supervised over there at in-process latency, and the requester holds a ref and
watches it.

**Both nodes have to be running the same code.** A behavior is a closure and a
closure does not cross a socket. A factory key the peer has never heard of
comes back as `SpawnFailed(reason="unknown-factory")`, which is what a version
skew between two deployments looks like from the requesting side.

## What actually goes over the wire

A frame is a four-byte big-endian length followed by a JSON object:

```json
{"v": 1, "to": "/user/checkout/session-7#f3a1c8",
 "from": "tapio://web@10.0.0.9:25520",
 "t": "orders.protocol.Reserve",
 "p": {"sku": "X-1", "qty": 2}}
```

`to` omits the address, because a frame arriving on an association is by
definition addressed to the node that received it. `from` is the sending
*system* rather than a sending actor: a `tell` carries no sender, so there is
none to name. It is a diagnostic, so a dead letter can say which node produced
a frame it could not read. Replies go to the `reply_to` a message carries,
which is a complete ref and the only thing that ever addresses an actor.

`v` is the wire protocol version, and the handshake pins it. It is
deliberately not the tapio version: a release that does not change the wire
does not change this number, so a fleet can roll from one release to the next
instead of stopping to swap every node at once.

**`t` is a registry key, never an import path.** Resolving a dotted name that
arrived on a socket into an importable object is remote code execution.
`@register_message()` is what puts a class in the registry, and a key nobody
registered becomes a dead letter naming the key. Nothing is imported to find
out what it might have meant.

A message crossing a link is rebuilt from JSON, so it is `==` to what was sent
and never `is` it. Locally, a `tell` delivers the very object that was passed.
Both facts are worth knowing when writing a test.

## Delivery, and what it does not promise

Delivery across a link is **at most once, FIFO per association**, which is the
same guarantee as a local send. There are no acknowledgements and no retries.

That is deliberate. A retry is only safe when the receiver can tolerate a
repeat, and the library does not know which of your messages those are.
Upgrading at-most-once to at-least-once belongs in your protocol, where an
idempotency key or a sequence number can be attached by someone who knows what
the message means.
