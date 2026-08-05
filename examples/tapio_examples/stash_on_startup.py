"""Holding traffic while an actor loads the state it needs to answer it.

Concepts: `Behaviors.with_stash`, `stash.unstash_all`, and behavior-switching
as the way an actor says "I am ready now".

An actor that has to load something before it can work has three options, and
only one is good. Dropping what arrives loses work. Blocking the receive loop
on the load leaves the actor unable to answer anything, including a stop: the
actor is not slow, it is absent. The third option is to accept the messages,
put them aside, and replay them once the state exists.

Replay puts the held messages back at the front of the mailbox, rather than
handing them to the behavior one at a time. Two things follow. The held
messages keep their arrival order and stay ahead of anything that queued up
while the actor was loading, so nothing is reordered. And the actor stays an
ordinary actor throughout: a signal still outranks the backlog, so a stop
arriving mid-replay is honoured instead of queued behind work nobody wants.

What to watch in the output: greetings 1 and 2 arrived before the template and
3 arrived after, and all three are answered in the order they were sent. The
stash is what makes that true. Without it the first two would have been
answered wrongly, or not at all.

Run it with:

```
uv run python -m tapio_examples.stash_on_startup
```
"""

import asyncio
from datetime import timedelta

from tapio import ActorSystem, Behavior, Behaviors, Message
from tapio.actor import ActorContext, StashBuffer, TimerScheduler

__all__ = ["Greet", "Loaded", "main"]

LOAD_TIME = timedelta(milliseconds=20)
"""How long the pretend store takes to answer."""


class Greet(Message):
    """Ask for someone to be greeted, which needs the template."""

    whom: str


class Loaded(Message):
    """The template has arrived, so the actor can start working."""

    template: str


Traffic = Greet | Loaded


def greeter(lines: list[str], done: asyncio.Event) -> Behavior[Traffic]:
    """A greeter that cannot greet until its template has loaded.

    The stash capacity is required, and it matters. A stash holds traffic the
    actor is not keeping up with, so an unbounded one is a memory leak.
    Overflow raises in this actor, where the decision about what to drop
    belongs.
    """

    def ready(template: str) -> Behavior[Traffic]:
        """What the greeter becomes once it has something to greet with."""

        async def on_greet(message: Traffic) -> Behavior[Traffic]:
            if isinstance(message, Greet):
                lines.append("greeter: " + template.format(message.whom))
                if message.whom == "carol":
                    done.set()
            return Behaviors.same()

        return Behaviors.receive_message(on_greet)

    def with_scheduler(timers: TimerScheduler[Traffic]) -> Behavior[Traffic]:
        def with_buffer(stash: StashBuffer[Traffic]) -> Behavior[Traffic]:
            def build(ctx: ActorContext[Traffic]) -> Behavior[Traffic]:
                # Stands in for the ask or the child actor a real load would
                # use. All that matters here is that the answer arrives later,
                # as a message, like everything else an actor learns.
                timers.start_single("load", Loaded(template="hello, {}!"), LOAD_TIME)
                lines.append("greeter: loading, holding what arrives")

                async def while_loading(message: Traffic) -> Behavior[Traffic]:
                    if isinstance(message, Loaded):
                        lines.append(f"greeter: loaded, replaying {stash.size} held")
                        # One call switches state and replays the backlog.
                        # Everything held goes back in front of whatever
                        # arrived while this message was being handled.
                        return stash.unstash_all(ready(message.template))
                    stash.stash(message)
                    lines.append(f"greeter: not ready, stashed {message.whom}")
                    return Behaviors.same()

                return Behaviors.receive_message(while_loading)

            return Behaviors.setup(build)

        return Behaviors.with_stash(16, with_buffer)

    return Behaviors.with_timers(with_scheduler)


async def main() -> list[str]:
    """Run the example.

    Returns:
        One line per thing the greeter did, in order.
    """
    lines: list[str] = []
    done = asyncio.Event()

    async with ActorSystem("stash-on-startup") as system:
        desk = system.spawn(greeter(lines, done), name="greeter")

        # Two arrive before the template does.
        desk.tell(Greet(whom="ada"))
        desk.tell(Greet(whom="grace"))

        # And one after, which must not overtake them.
        await asyncio.sleep(LOAD_TIME.total_seconds() * 2)
        desk.tell(Greet(whom="carol"))
        await done.wait()

    for line in lines:
        print(line)
    return lines


if __name__ == "__main__":
    asyncio.run(main())
