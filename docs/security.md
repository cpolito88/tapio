# Security of the remote transport

**This transport is designed for a trusted network between services you
deploy. It is not designed to face the public internet.**

That sentence is the page. The rest is what the library does to hold you to
it, and what it cannot do for you.

Opening a port that accepts frames naming actor paths and message types is a
serious surface, so the defaults are set for somebody who has not thought
about it yet.

## Loopback by default, and a refusal rather than a risk

`bind_host` is `127.0.0.1`. Binding to anything else without a `secret` raises
`InsecureRemoteConfig` while the system is being constructed, naming both
settings.

"Anything else" includes the empty string. It reads like a setting nobody
filled in, which is exactly how it reaches a deployment, and the sockets layer
reads it as every interface. So `bind_host=""` is `0.0.0.0` written
differently, and it is refused the same way. Only `127.0.0.1`, the other
loopback literals, and `localhost` count as loopback.

A misconfigured deployment therefore fails to start instead of quietly serving
strangers. It happens during construction, before the port is listening and
before any ref has been handed out, so there is no window in which the wrong
configuration is running.

## The handshake

Before any application frame is read, both sides exchange the system name, the
canonical address, a **system uid**, the wire protocol version, the tapio
version, and an HMAC of a server-supplied nonce with the shared secret. A bad
HMAC or a protocol mismatch closes the connection with a logged reason, and
nothing further is read.

Protocol equality is required rather than negotiated. An incompatible wire
format that half works is worse than one that refuses.

The **tapio version is not checked**, only reported. It moves for a fixed
docstring and a faster mailbox, neither of which a peer can observe, so
pinning a link to it would make every release a flag day: during a rolling
deploy half the nodes would refuse the other half. Two nodes on different
releases talk to each other as long as neither release changed the wire. What
they cannot do is disagree about the protocol.

The system uid is minted per system incarnation, and it is what makes a
restarted peer a *different* peer rather than the same one returning. An
association is bound to the uid it handshook with, so a peer that reconnects
presenting a new uid means the old one died: the previous association is
quarantined and its watchers are told, instead of the new connection silently
inheriting the old one's identity.

## The secret authenticates, TLS is what makes it private

`secret` proves that a peer knows something a stranger does not. It does not
encrypt anything. Over plaintext, the shared secret protects the handshake and
nothing else: every frame after it is readable and modifiable by anything on
the path.

`tls` takes a certificate, a key, and an optional CA for mutual
authentication. **Use both for anything crossing a machine boundary.** The
secret without TLS is only appropriate where the network itself is the
boundary, such as a container network or a loopback interface.

## The decoder is the trust boundary

Everything a peer sends arrives at one place, and that place is written like
the boundary it is:

- **A frame size cap before allocation.** Frames over `max_frame_bytes`, four
  megabytes by default, are refused without reading them, which is the
  cheapest way to stop a hostile or buggy peer from exhausting memory.
- **Registry-only type resolution.** The `t` field of a frame is a registry
  key, never an import path. Resolving a dotted name from a socket into an
  importable object is remote code execution, and it is how this goes wrong
  casually. An unregistered key becomes a dead letter naming the key, and
  nothing is imported to find out what it might have meant.
- **Strict validation as the decode.** A frame becomes a message by being
  validated against the registered model, so a field that is the wrong type or
  missing is a decoding failure and not a surprise inside a handler.
- **No reflection anywhere in the path.** Nothing a peer sends selects code to
  run. It selects a registered model to validate against, and that is all.

An actor path in a frame is likewise only ever looked up in the local
registry. A path naming an actor that does not exist is a dead letter, so a
peer can discover nothing by probing except which of its own guesses failed.

## What is left to you

A peer that passes the handshake can send any registered message to any actor
whose path it knows. There is no per-actor authorization, and there are no
capabilities beyond the ones you build: **the shared secret is the whole
authorization model.** Everything behind it is one trust domain.

The one place the library gives you a lever is remote spawning. A spawner
offers named factories and refuses everything else, and the allowlist,
`spawner(offers=["worker"])`, is checked where the spawner is written.

An actor that will start anything registered, on request, is a capability
handed to whoever can reach the port. Keep the list short, and keep it to
things that are safe to have started by a peer.

## A checklist

For anything beyond one machine:

- `secret` set, from the environment or a secret manager, never in the source.
- `tls` configured, with mutual authentication where both ends are yours.
- `bind_host` set to the interface you meant, not to `0.0.0.0` or `""` because
  it was easier.
- The port reachable only from the services that need it, at the network
  level, because tapio has no per-peer allowlist.
- `max_frame_bytes` no larger than your largest legitimate message.
- Spawner allowlists reviewed the way you would review a public endpoint,
  because that is what they are.
