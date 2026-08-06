"""One actor per user, over a model that sometimes falls over.

Concepts: state, supervision, ask and watch doing one job together, which is
the shape most real services end up in.

A session is an actor per user. Its conversation is ordinary local state, and
it needs no lock, because one actor handles one message at a time. The model
client is its child, supervised by it, so a model call that blows up is
restarted underneath a session that keeps its history.

The registry above them keeps the map from user to session. It watches every
session it starts, so a session that stops is evicted by the `Terminated` that
follows rather than by whoever remembered to clean up. That is the pattern
worth taking away: watching is how a map of live actors stays true, and it
keeps working when the thing that stopped was not the thing that asked.

What to watch in the output: the third and fourth lines. The model crashed
while it was holding a request, so no answer came back and the ask timed out.
That is not a bug to fix with a bigger timeout: a crash is not a reply, and it
never will be. The session asked again, the restarted model answered, and the
turn count shows the session's own state was never touched.

Run it with:

```
uv run python -m tapio_examples.chat_sessions
```
"""

import asyncio
from datetime import timedelta

from tapio import ActorSystem, Behavior, Behaviors, Message
from tapio.actor import (
    ActorContext,
    ActorRef,
    Signal,
    SupervisorStrategy,
    Terminated,
)
from tapio.errors import AskTimeoutError

__all__ = [
    "Close",
    "Completion",
    "Prompt",
    "Reply",
    "Say",
    "main",
    "model",
    "registry",
    "session",
]

MODEL_TIMEOUT = timedelta(milliseconds=100)
"""How long a session waits for the model before deciding nothing is coming."""

FAILS_ON_CALL = 2
"""Which call to a model client blows up, so the example is a test as well."""


class Completion(Message):
    """What the model answers."""

    text: str


class Prompt(Message):
    """What the model is asked."""

    text: str
    reply_to: ActorRef[Completion]


class Reply(Message):
    """What the user gets back, and how much of the conversation it has seen."""

    user: str
    text: str
    turns: int


class Say(Message):
    """One thing a user said, and where the answer goes."""

    user: str
    text: str
    reply_to: ActorRef[Reply]


class Close(Message):
    """Ends a user's session."""

    user: str


def model() -> Behavior[Prompt]:
    """Build a stand-in for a model client, scripted to fail on one call.

    The call counter sits here rather than inside `build`, so it survives the
    restart. A restart re-runs `setup`, which is exactly what rebuilds state
    an actor should not keep; anything the restart must not forget has to live
    outside the part that is re-run.

    Returns:
        The behavior to spawn.
    """
    calls = [0]

    def build(ctx: ActorContext[Prompt]) -> Behavior[Prompt]:
        async def on_prompt(message: Prompt) -> Behavior[Prompt]:
            calls[0] += 1
            if calls[0] == FAILS_ON_CALL:
                msg = "the model client fell over"
                raise RuntimeError(msg)
            message.reply_to.tell(Completion(text=f"about {message.text!r}, then"))
            return Behaviors.same()

        return Behaviors.receive_message(on_prompt, msg_type=Prompt)

    return Behaviors.setup(build)


def session(user: str, lines: list[str]) -> Behavior[Say | Close]:
    """Build one user's session, with its own model client underneath it.

    The client is a child, so this session is its supervisor and decides what
    a failing model call means. A restart happens here, in process, and
    nothing above this actor hears about it.

    Args:
        user: Whose session this is.
        lines: Where to write what happened.

    Returns:
        The behavior to spawn.
    """

    def build(ctx: ActorContext[Say | Close]) -> Behavior[Say | Close]:
        # Ordinary local state. One actor handles one message at a time, so
        # nothing here needs a lock and nothing can interleave with it.
        history: list[str] = []
        client = ctx.spawn(
            Behaviors.supervise(model()).on_failure(
                SupervisorStrategy.restart(), on=RuntimeError
            ),
            "model",
        )

        async def on_message(message: Say | Close) -> Behavior[Say | Close]:
            if isinstance(message, Close):
                return Behaviors.stopped()
            history.append(message.text)
            try:
                completion = await client.ask(
                    lambda reply_to: Prompt(text=message.text, reply_to=reply_to),
                    expect=Completion,
                    timeout=MODEL_TIMEOUT,
                )
            except AskTimeoutError:
                # The model client crashed while it was holding the request,
                # so nobody ever answered. A crash is not a reply. Its
                # supervisor has restarted it by now, and the ref is the same
                # one, so asking again is the whole of the recovery.
                lines.append(f"chat: no answer for {user}, so ask the new client")
                completion = await client.ask(
                    lambda reply_to: Prompt(text=message.text, reply_to=reply_to),
                    expect=Completion,
                    timeout=MODEL_TIMEOUT,
                )
            message.reply_to.tell(
                Reply(user=user, text=completion.text, turns=len(history))
            )
            return Behaviors.same()

        return Behaviors.receive_message(on_message, msg_type=Say | Close)

    return Behaviors.setup(build)


def registry(lines: list[str]) -> Behavior[Say | Close]:
    """Build the actor that owns the map from user to session.

    Args:
        lines: Where to write what happened.

    Returns:
        The behavior to spawn.
    """

    def build(ctx: ActorContext[Say | Close]) -> Behavior[Say | Close]:
        sessions: dict[str, ActorRef[Say | Close]] = {}
        users: dict[str, str] = {}

        def session_for(user: str) -> ActorRef[Say | Close]:
            existing = sessions.get(user)
            if existing is not None:
                return existing
            started = ctx.spawn(session(user, lines), f"session-{user}")
            # Watched, not just remembered. The eviction below then happens
            # because the session stopped, whatever stopped it, rather than
            # because somebody remembered to tidy up after one particular way
            # of stopping it.
            ctx.watch(started)
            sessions[user] = started
            users[str(started.path)] = user
            lines.append(f"chat: {user} has a session at {started.path.name}")
            return started

        async def on_message(message: Say | Close) -> Behavior[Say | Close]:
            session_for(message.user).tell(message)
            return Behaviors.same()

        async def on_signal(
            ctx: ActorContext[Say | Close], signal: Signal
        ) -> Behavior[Say | Close]:
            if isinstance(signal, Terminated):
                user = users.pop(str(signal.ref.path), None)
                if user is not None:
                    del sessions[user]
                    lines.append(f"chat: {user}'s session stopped, so it is evicted")
            return Behaviors.same()

        return Behaviors.receive_message(
            on_message, msg_type=Say | Close, on_signal=on_signal
        )

    return Behaviors.setup(build)


async def main() -> list[str]:
    """Run the example.

    Returns:
        The lines the chat produced, in the order it produced them.
    """
    lines: list[str] = []
    async with ActorSystem("chat") as system:
        desk = system.spawn(registry(lines), "sessions")

        for user, text in (("alice", "hello"), ("bob", "hi"), ("alice", "again")):
            answer = await desk.ask(
                lambda reply_to, user=user, text=text: Say(  # type: ignore[misc]
                    user=user, text=text, reply_to=reply_to
                ),
                expect=Reply,
            )
            lines.append(
                f"chat: {answer.user} heard {answer.text!r} on turn {answer.turns}"
            )

        desk.tell(Close(user="alice"))
        # The eviction is a signal arriving at the registry, so give it a turn
        # of the loop to be delivered.
        await asyncio.sleep(0.01)
        # Snapshotted here rather than after the block, because shutdown stops
        # bob's session too and the registry evicts that one as well. That is
        # correct and it is not what this example is about.
        lines = list(lines)

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
