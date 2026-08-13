"""A protocol as a state machine, with one behavior per state.

Concepts: behavior-switching as the states of a protocol, and
`ctx.message_adapter` for talking to a service whose replies are not in your
protocol.

The usual way to write a connection is a field called `state`, an enum, and a
`match` at the top of every handler deciding whether this message is legal
right now. An actor needs none of that. The state is the behavior. A
connection that has not authenticated is a different function from one that
has, so a `Send` arriving too early is not an illegal combination to check
for. It is simply a message that state does not handle.

The gain is that the illegal transitions cannot be written. No branch in
`ready` can accidentally accept a second `Authenticate`, because `ready` never
mentions it. And there is no state variable to leave inconsistent, because
switching state means returning a different behavior and nothing else.

The token service is the second half. It is somebody else's actor and it
answers in its own vocabulary, `Token`, which does not belong in a
connection's protocol. Widening the connection to accept a `Token` would let
anyone send it one. Instead the connection hands out an adapter, which accepts
a `Token` and turns it into the `Authenticated` the connection understands.
The translation runs inside the connection, so a mistake in it is the
connection's failure and not the token service's.

What to watch in the output: the `Send` that arrives while connecting is
refused rather than queued, and the identical `Send` after authentication goes
through. Same message, same actor, different state.

Run it with `uv run python -m tapio_examples.state_machine`.
"""

import asyncio

from tapio import ActorSystem, Behavior, Behaviors, Message
from tapio.actor import ActorContext, ActorRef

__all__ = ["Close", "Connect", "Send", "main"]


class Token(Message):
    """What the token service answers with. Not the connection's protocol."""

    value: str


class Issue(Message):
    """What the token service accepts."""

    reply_to: ActorRef[Token]


class Connect(Message):
    """Open the connection, which starts authentication."""


class Authenticated(Message):
    """A token has arrived, translated into the connection's own protocol."""

    token: str


class Send(Message):
    """Send a payload, which only works once the connection is ready."""

    payload: str


class Close(Message):
    """Close the connection, draining what it is already doing."""


Protocol = Connect | Authenticated | Send | Close


def tokens() -> Behavior[Issue]:
    """A service that mints tokens, in its own vocabulary."""

    async def on_issue(message: Issue) -> Behavior[Issue]:
        message.reply_to.tell(Token(value="t-42"))
        return Behaviors.same()

    return Behaviors.receive_message(on_issue)


def connection(
    lines: list[str], marks: dict[int, asyncio.Event], service: ActorRef[Issue]
) -> Behavior[Protocol]:
    """A connection whose protocol states are its behaviors."""

    def say(line: str) -> None:
        """Write a line down, and signal the points the script waits for."""
        lines.append(line)
        mark = marks.get(len(lines))
        if mark is not None:
            mark.set()

    def build(ctx: ActorContext[Protocol]) -> Behavior[Protocol]:
        # Handed to the token service in place of this actor's own ref, so the
        # service can answer without the connection accepting `Token` into its
        # protocol. The lambda has no annotation, so the type is passed in.
        as_authenticated: ActorRef[Token] = ctx.message_adapter(
            lambda token: Authenticated(token=token.value), Token
        )

        def disconnected() -> Behavior[Protocol]:
            """Nothing is open. The only thing that can happen is `Connect`."""

            async def on_message(message: Protocol) -> Behavior[Protocol]:
                if not isinstance(message, Connect):
                    say(f"conn: refused {type(message).__name__}, not open")
                    return Behaviors.same()
                say("conn: connecting, asking for a token")
                service.tell(Issue(reply_to=as_authenticated))
                return authenticating()

            return Behaviors.receive_message(on_message)

        def authenticating() -> Behavior[Protocol]:
            """Waiting for a token. Still not a state that can send anything."""

            async def on_message(message: Protocol) -> Behavior[Protocol]:
                if not isinstance(message, Authenticated):
                    say(f"conn: refused {type(message).__name__}, still connecting")
                    return Behaviors.same()
                say(f"conn: authenticated with {message.token}")
                return ready(message.token)

            return Behaviors.receive_message(on_message)

        def ready(token: str) -> Behavior[Protocol]:
            """Open. This is the only state that mentions `Send` at all."""

            async def on_message(message: Protocol) -> Behavior[Protocol]:
                match message:
                    case Send(payload=payload):
                        say(f"conn: sent {payload!r} with {token}")
                    case Close():
                        # A marker message. Everything already queued sits
                        # ahead of a message sent now, so when this one comes
                        # back the mailbox is drained and it is safe to stop.
                        say("conn: closing, draining what is queued")
                        ctx.self_ref.tell(Close())
                        return closing()
                    case _:
                        say(f"conn: refused {type(message).__name__}, open")
                return Behaviors.same()

            return Behaviors.receive_message(on_message)

        def closing() -> Behavior[Protocol]:
            """Going away. What was already queued is answered, then it stops."""

            async def on_message(message: Protocol) -> Behavior[Protocol]:
                if isinstance(message, Close):
                    say("conn: closed")
                    return Behaviors.stopped()
                say(f"conn: dropped {type(message).__name__}, closing")
                return Behaviors.same()

            return Behaviors.receive_message(on_message)

        return disconnected()

    return Behaviors.setup(build)


async def main() -> list[str]:
    """Run the example.

    Returns:
        One line per thing the connection did, in order.
    """
    lines: list[str] = []
    marks = {n: asyncio.Event() for n in (3, 4, 8)}

    async with ActorSystem("state-machine") as system:
        service = system.spawn(tokens(), name="tokens")
        conn = system.spawn(connection(lines, marks, service), name="conn")

        # Too early. The connection is not open, and this state does not
        # handle a Send at all.
        conn.tell(Send(payload="hello"))
        conn.tell(Connect())
        # Also too early, and refused by a different state for its own reason.
        conn.tell(Send(payload="hello again"))
        await marks[3].wait()

        # The token arrives through the adapter, and the connection is ready.
        await marks[4].wait()
        conn.tell(Send(payload="hello"))
        conn.tell(Close())
        conn.tell(Send(payload="too late"))
        await marks[8].wait()

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
