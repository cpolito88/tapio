"""Tests for the association: messages over a real link."""

import asyncio
import contextlib
from datetime import timedelta

import pytest

from tapio.actor import (
    ActorContext,
    ActorSystem,
    Behavior,
    Behaviors,
    DeadLetter,
    Signal,
    Terminated,
)
from tapio.actor.dead_letters import DeadLetterReason
from tapio.dispatch.dispatcher import Dispatcher
from tapio.errors import MessageEncodingError
from tapio.remote.address import Address
from tapio.remote.association import Association, _cancel_and_wait
from tapio.remote.codec import LENGTH_PREFIX, encode
from tapio.remote.transport import framed, is_link_frame, link_body
from tapio.settings import RemoteSettings
from tapio.testkit import assert_no_leaked_tasks
from tests.failures import eventually
from tests.remote.peers import (
    GHOST,
    Ping,
    Pong,
    Tick,
    Unregistered,
    collecting,
    counting,
    dial,
    echoing,
    failing_writes,
    relaying,
    remoting,
    silent_peer,
    stalled_writes,
    uri,
)


async def test_a_message_crosses_an_association(alpha: ActorSystem, beta: ActorSystem):
    seen: list[int] = []
    ticker = beta.spawn(counting(seen), "ticker")

    remote = await alpha.resolve(uri(beta, ticker), expect=Tick)
    remote.tell(Tick(n=1))

    await eventually(lambda: seen == [1])


async def test_a_reply_arrives_at_a_reply_to_that_crossed_the_wire(
    alpha: ActorSystem, beta: ActorSystem
):
    answers: list[Pong] = []
    echo = beta.spawn(echoing(), "echo")
    cart = alpha.spawn(collecting(answers), "cart")

    remote = await alpha.resolve(uri(beta, echo), expect=Ping)
    remote.tell(Ping(n=7, reply_to=cart))

    await eventually(lambda: [answer.n for answer in answers] == [7])


async def test_the_reply_travels_back_over_the_same_association(
    alpha: ActorSystem, beta: ActorSystem
):
    # One connection per peer pair: beta never resolved anything, so its reply
    # has to reuse the link alpha opened.
    answers: list[Pong] = []
    echo = beta.spawn(echoing(), "echo")
    cart = alpha.spawn(collecting(answers), "cart")

    remote = await alpha.resolve(uri(beta, echo), expect=Ping)
    remote.tell(Ping(n=1, reply_to=cart))
    await eventually(lambda: len(answers) == 1)

    assert alpha.remote is not None
    assert beta.remote is not None
    assert alpha.remote.associations == (beta.address,)
    assert beta.remote.associations == (alpha.address,)


async def test_a_message_off_the_wire_equals_what_was_sent_without_being_it(
    alpha: ActorSystem, beta: ActorSystem
):
    # A message rebuilt from JSON is equal to what was sent, never the same
    # object. `Message` is frozen, so equality is enough.
    answers: list[Pong] = []
    echo = beta.spawn(echoing(), "echo")
    cart = alpha.spawn(collecting(answers), "cart")
    sent = Ping(n=3, reply_to=cart)

    remote = await alpha.resolve(uri(beta, echo), expect=Ping)
    remote.tell(sent)
    await eventually(lambda: len(answers) == 1)

    assert answers[0] == Pong(n=3)
    assert answers[0] is not sent


async def test_fifo_holds_for_ten_thousand_messages(
    alpha: ActorSystem, beta: ActorSystem
):
    # 10,000 messages fills the socket buffer, so the writer waits in `drain`,
    # which is where ordering would break.
    seen: list[int] = []
    ticker = beta.spawn(counting(seen), "ticker")
    remote = await alpha.resolve(uri(beta, ticker), expect=Tick)

    for n in range(10_000):
        await remote.offer(Tick(n=n))

    await eventually(lambda: len(seen) == 10_000, within=30.0)
    assert seen == list(range(10_000))


async def test_an_unregistered_message_raises_at_the_send_site(
    alpha: ActorSystem, beta: ActorSystem
):
    # Errors about the message belong to the sender, and nothing is sent.
    ticker = beta.spawn(counting([]), "ticker")
    remote = await alpha.resolve(uri(beta, ticker), expect=Unregistered)

    with pytest.raises(MessageEncodingError, match="no wire key"):
        remote.tell(Unregistered(n=1))


async def test_a_full_outbound_buffer_dead_letters_instead_of_raising():
    # Errors about the recipient are dead letters, never an exception.
    async with ActorSystem("alpha", remoting(outbound_capacity=2)) as one:
        letters: list[DeadLetter] = []
        one.dead_letters.subscribe(letters.append)
        remote = await one.resolve(f"{GHOST}/user/ticker#1", expect=Tick)

        # No await in the loop, so the association cannot drain and the
        # overflow is certain rather than a race.
        for n in range(64):
            remote.tell(Tick(n=n))

        full = [
            letter
            for letter in letters
            if letter.reason == DeadLetterReason.OUTBOUND_BUFFER_FULL
        ]
        assert full
        assert full[0].peer == str(GHOST)
        assert isinstance(full[0].message, Tick)


async def test_a_tell_to_a_peer_that_was_never_reachable_dead_letters(
    alpha: ActorSystem,
):
    # The dial fails behind the send, so the message is dead-lettered rather
    # than left hanging.
    letters: list[DeadLetter] = []
    alpha.dead_letters.subscribe(letters.append)

    remote = await alpha.resolve(f"{GHOST}/user/ticker#1", expect=Tick)
    remote.tell(Tick(n=1))

    await eventually(lambda: bool(letters))
    assert letters[0].peer == str(GHOST)
    assert letters[0].message == Tick(n=1)


async def test_a_failed_association_is_forgotten_so_the_next_send_dials_again(
    alpha: ActorSystem,
):
    letters: list[DeadLetter] = []
    alpha.dead_letters.subscribe(letters.append)
    remote = await alpha.resolve(f"{GHOST}/user/ticker#1", expect=Tick)
    remote.tell(Tick(n=1))

    await eventually(lambda: bool(letters))
    assert alpha.remote is not None
    await eventually(lambda: alpha.remote.associations == ())  # type: ignore[union-attr]


async def test_a_type_key_the_peer_does_not_know_dead_letters_over_there(
    beta: ActorSystem,
):
    # The dead letter names the key and the sender. Nothing is imported to
    # find out what the key meant.
    letters: list[DeadLetter] = []
    beta.dead_letters.subscribe(letters.append)
    ticker = beta.spawn(counting([]), "ticker")
    link = await dial(beta)

    body = encode(Tick(n=1), to=ticker.path)[LENGTH_PREFIX:].replace(
        b'"t":"tests.remote.peers.Tick"', b'"t":"orders.protocol.Unknown"'
    )
    await link.write_frame(framed(body))

    await eventually(lambda: bool(letters))
    assert letters[0].reason == DeadLetterReason.UNKNOWN_MESSAGE_TYPE
    assert letters[0].peer == str(GHOST)
    await link.close()


async def test_an_oversized_frame_is_refused_and_the_link_closed(beta: ActorSystem):
    # The length is checked before the body is read, so a peer announcing a
    # gigabyte costs a header and a refusal.
    letters: list[DeadLetter] = []
    beta.dead_letters.subscribe(letters.append)
    settings = beta.settings.remote
    assert settings is not None
    link = await dial(beta)

    await link.write_frame((settings.max_frame_bytes + 1).to_bytes(4, "big") + b"{")

    await eventually(
        lambda: any(
            letter.reason == DeadLetterReason.FRAME_TOO_LARGE for letter in letters
        )
    )
    with pytest.raises((asyncio.IncompleteReadError, ConnectionError)):
        await link.read_frame()
    await link.close()


async def test_a_frame_arriving_before_the_handshake_is_refused(beta: ActorSystem):
    ticker = beta.spawn(counting([]), "ticker")
    port = beta.address.port
    assert port is not None
    reader, writer = await asyncio.open_connection("127.0.0.1", port)

    writer.write(encode(Tick(n=1), to=ticker.path))
    await writer.drain()

    # The peer never said who it was, so nothing it wrote is read.
    with pytest.raises(asyncio.IncompleteReadError):
        await reader.readexactly(1024)
    writer.close()
    # Awaited, not just asked for: a transport still closing when the loop
    # goes away is collected unclosed, and this suite turns that warning into
    # an error at whichever test happens to run next.
    await writer.wait_closed()


async def test_an_idle_association_heartbeats(beta: ActorSystem):
    # Heartbeats are what tells a dead peer from a quiet one.
    link = await dial(beta)
    try:
        frame = await asyncio.wait_for(link.read_frame(), 2.0)
        assert is_link_frame(frame)
        assert link_body(frame)["link"] == "heartbeat"
    finally:
        await link.close()


async def test_a_link_frame_this_version_does_not_know_is_ignored(beta: ActorSystem):
    # A peer running something newer must not break a working link.
    seen: list[int] = []
    ticker = beta.spawn(counting(seen), "ticker")
    link = await dial(beta)
    try:
        await link.write_frame(framed(b'{"link":"from-the-future"}'))
        await link.write_frame(encode(Tick(n=1), to=ticker.path))

        await eventually(lambda: seen == [1])
    finally:
        await link.close()


async def test_both_ends_dialling_at_once_end_up_with_one_association(
    alpha: ActorSystem, beta: ActorSystem
):
    # Without a rule for this the pair keeps two connections, and FIFO per
    # association stops meaning anything.
    here: list[int] = []
    there: list[int] = []
    mine = alpha.spawn(counting(here), "ticker")
    theirs = beta.spawn(counting(there), "ticker")

    to_beta = await alpha.resolve(uri(beta, theirs), expect=Tick)
    to_alpha = await beta.resolve(uri(alpha, mine), expect=Tick)
    to_beta.tell(Tick(n=1))
    to_alpha.tell(Tick(n=2))

    await eventually(lambda: there == [1] and here == [2])
    assert alpha.remote is not None
    assert beta.remote is not None
    assert alpha.remote.associations == (beta.address,)
    assert beta.remote.associations == (alpha.address,)


async def test_traffic_survives_the_link_that_lost_the_dial(
    alpha: ActorSystem, beta: ActorSystem
):
    # Closing the losing link must not drop what was already queued on it.
    there: list[int] = []
    theirs = beta.spawn(counting(there), "ticker")
    mine = alpha.spawn(counting([]), "ticker")

    to_beta = await alpha.resolve(uri(beta, theirs), expect=Tick)
    await beta.resolve(uri(alpha, mine), expect=Tick)
    for n in range(20):
        to_beta.tell(Tick(n=n))

    await eventually(lambda: there == list(range(20)))


async def test_two_systems_sharing_a_secret_talk():
    async with (
        ActorSystem("alpha", remoting(secret="shh")) as one,
        ActorSystem("beta", remoting(secret="shh")) as two,
    ):
        seen: list[int] = []
        ticker = two.spawn(counting(seen), "ticker")

        remote = await one.resolve(uri(two, ticker), expect=Tick)
        remote.tell(Tick(n=1))

        await eventually(lambda: seen == [1])


async def test_a_peer_with_the_wrong_secret_gets_nothing_through():
    async with (
        ActorSystem("alpha", remoting(secret="right")) as one,
        ActorSystem("beta", remoting(secret="wrong")) as two,
    ):
        letters: list[DeadLetter] = []
        one.dead_letters.subscribe(letters.append)
        seen: list[int] = []
        ticker = two.spawn(counting(seen), "ticker")

        remote = await one.resolve(uri(two, ticker), expect=Tick)
        remote.tell(Tick(n=1))

        # The handshake fails, so the queued message is dead-lettered. Nothing
        # is delivered and nothing raises at the sender.
        await eventually(lambda: bool(letters))
        assert seen == []
        assert letters[0].reason == DeadLetterReason.LINK_FAILED


async def test_an_adapter_ref_handed_to_a_peer_is_answerable(
    alpha: ActorSystem, beta: ActorSystem
):
    # An adapter is addressable like the actor behind it, so it has to write
    # itself down with the system's canonical address. Without that a peer
    # reads it as a ref naming a system and nowhere to dial, and the answer
    # dead-letters instead of arriving.
    seen: list[int] = []
    echo = beta.spawn(echoing(), "echo")
    remote = await alpha.resolve(uri(beta, echo), expect=Ping)
    relay = alpha.spawn(relaying(remote, seen), "relay")

    relay.tell(Tick(n=5))

    await eventually(lambda: seen == [5])


async def test_a_write_that_fails_dead_letters_the_message_and_ends_the_link():
    # The flush that brings the link up succeeds, so the association is
    # properly connected, and the send after it meets a broken socket. That is
    # the ordinary case: a link that worked and then did not.
    with assert_no_leaked_tasks():
        one = ActorSystem("alpha", remoting(heartbeat_interval=timedelta(seconds=60)))
        two = ActorSystem("beta", remoting())
        try:
            assert one.remote is not None
            one.remote.set_link_filter(failing_writes(after=1))
            letters: list[DeadLetter] = []
            one.dead_letters.subscribe(letters.append)
            seen: list[int] = []
            ticker = two.spawn(counting(seen), "ticker")

            remote = await one.resolve(uri(two, ticker), expect=Tick)
            remote.tell(Tick(n=1))
            await eventually(lambda: seen == [1])

            remote.tell(Tick(n=2))

            await eventually(lambda: bool(letters))
            assert letters[0].reason == DeadLetterReason.LINK_FAILED
            assert letters[0].peer == str(two.address)
            # The message is reported as what its sender sent, not as the
            # Outbound wrapper it was travelling in.
            assert isinstance(letters[0].message, Tick)
            # And the link is given up rather than kept in a state where every
            # further write would fail the same way.
            await eventually(lambda: one.remote.associations == ())  # type: ignore[union-attr]
        finally:
            await one.terminate()
            await two.terminate()


async def test_a_frame_that_never_flushed_is_put_back_and_accounted_for():
    # `_open` fails on the very first write, so the frame it had taken off the
    # queue goes back rather than being lost between the two. The association
    # then stops and reports it, which is what makes at-most-once auditable.
    with assert_no_leaked_tasks():
        one = ActorSystem("alpha", remoting())
        two = ActorSystem("beta", remoting())
        try:
            assert one.remote is not None
            one.remote.set_link_filter(failing_writes(after=0))
            letters: list[DeadLetter] = []
            one.dead_letters.subscribe(letters.append)
            seen: list[int] = []
            ticker = two.spawn(counting(seen), "ticker")

            remote = await one.resolve(uri(two, ticker), expect=Tick)
            remote.tell(Tick(n=1))

            await eventually(lambda: bool(letters))
            assert seen == []
            assert letters[0].reason == DeadLetterReason.LINK_FAILED
            assert isinstance(letters[0].message, Tick)
        finally:
            await one.terminate()
            await two.terminate()


async def test_frames_held_for_a_link_that_never_comes_up_are_shed():
    # A dial that hangs is the one state in which frames pile up in the hold
    # buffer rather than in the mailbox. Past `outbound_capacity` they are shed
    # with the peer named, instead of growing without a bound.
    with assert_no_leaked_tasks():
        async with silent_peer() as address:
            system = ActorSystem("alpha", remoting(outbound_capacity=4))
            try:
                letters: list[DeadLetter] = []
                system.dead_letters.subscribe(letters.append)
                remote = await system.resolve(f"{address}/user/ticker#1", expect=Tick)

                # A turn between sends, so each one reaches the hold buffer
                # rather than queueing in the mailbox: this is about `_hold`,
                # not about the mailbox's own overflow.
                for n in range(9):
                    remote.tell(Tick(n=n))
                    await asyncio.sleep(0)

                await eventually(lambda: bool(letters))
                shed = [
                    letter
                    for letter in letters
                    if "waiting for a link" in (letter.detail or "")
                ]
                assert shed, [letter.detail for letter in letters]
                assert shed[0].reason == DeadLetterReason.OUTBOUND_BUFFER_FULL
                assert shed[0].peer == str(address)
            finally:
                await system.terminate()


async def test_a_peer_that_accepts_no_bytes_is_declared_unreachable():
    # The case the failure detector could not see. A peer that holds the
    # connection open while reading nothing parks the association inside
    # `drain`, so its mailbox fills, the heartbeat tick is never handled, and
    # nothing ever asks whether the peer is still there. The write deadline is
    # what gives the actor its loop back.
    with assert_no_leaked_tasks():
        one = ActorSystem(
            "alpha",
            remoting(
                unreachable_after=timedelta(milliseconds=200),
                heartbeat_interval=timedelta(seconds=60),
            ),
        )
        two = ActorSystem("beta", remoting())
        try:
            assert one.remote is not None
            one.remote.set_link_filter(stalled_writes(after=1))
            letters: list[DeadLetter] = []
            one.dead_letters.subscribe(letters.append)
            seen: list[int] = []
            ticker = two.spawn(counting(seen), "ticker")

            remote = await one.resolve(uri(two, ticker), expect=Tick)
            remote.tell(Tick(n=1))
            await eventually(lambda: seen == [1])

            remote.tell(Tick(n=2))

            # The stalled write is given up on, the message is accounted for,
            # and the peer is quarantined rather than left half-connected.
            await eventually(lambda: bool(letters), within=5.0)
            assert letters[0].reason == DeadLetterReason.LINK_FAILED
            assert "accepted no bytes" in (letters[0].detail or "")
            await eventually(
                lambda: one.remote.is_quarantined(two.address),  # type: ignore[union-attr]
                within=5.0,
            )
        finally:
            await one.terminate()
            await two.terminate()


async def test_a_close_from_the_reader_survives_a_full_outbound_lane():
    # The interleaving TAP-09 describes. The link is up, the peer stops
    # reading, the bounded outbound lane fills, and then the peer closes the
    # socket. The reader raises on the closed link and asks the association to
    # close, and that Close travels the same full lane. It must not be refused:
    # if it is, the reader task dies with a MailboxFullError, `_release` never
    # runs, and the socket and task leak while `_closing` blocks every retry.
    with assert_no_leaked_tasks():
        one = ActorSystem(
            "alpha",
            remoting(
                outbound_capacity=2,
                unreachable_after=timedelta(milliseconds=300),
                heartbeat_interval=timedelta(seconds=60),
            ),
        )
        two = ActorSystem("beta", remoting())
        try:
            assert one.remote is not None
            one.remote.set_link_filter(stalled_writes(after=1))
            seen: list[int] = []
            ticker = two.spawn(counting(seen), "ticker")

            remote = await one.resolve(uri(two, ticker), expect=Tick)
            remote.tell(Tick(n=1))
            await eventually(lambda: seen == [1])

            # This one parks the association inside a stalled write, so the
            # writes that follow queue in the mailbox behind it.
            remote.tell(Tick(n=2))
            await asyncio.sleep(0.05)
            # Fill the bounded lane, so a Close has no slot to land in.
            remote.tell(Tick(n=3))
            remote.tell(Tick(n=4))

            # The peer closing is what makes the reader give up on the link and
            # call close() while the lane is full.
            await two.terminate()

            # With the guard, the actor stops on its next turn and the
            # association is released. Without it, this never happens.
            await eventually(
                lambda: one.remote.associations == (),  # type: ignore[union-attr]
                within=5.0,
            )
        finally:
            await one.terminate()
            await two.terminate()


async def test_a_watch_that_cannot_be_sent_is_answered_at_once():
    # A watch travels through the same mailbox as user traffic, so a full
    # outbound buffer can drop it. The peer then never registers it and no
    # Terminated is ever coming, so this end must not go on believing it holds
    # a watch: it is answered here instead of waiting forever.
    with assert_no_leaked_tasks():
        async with silent_peer() as address:
            system = ActorSystem("alpha", remoting(outbound_capacity=1))
            try:
                seen: list[str] = []
                remote = await system.resolve(f"{address}/user/ticker#1", expect=Tick)

                def build(ctx: ActorContext[Tick]) -> Behavior[Tick]:
                    # One turn, no awaits: the association actor cannot drain
                    # any of this, so the watch frame behind it has nowhere to
                    # go and the drop is certain rather than a race.
                    for n in range(8):
                        remote.tell(Tick(n=n))
                    ctx.watch(remote)

                    async def on_message(message: Tick) -> Behavior[Tick]:
                        return Behaviors.same()

                    async def on_signal(
                        ctx: ActorContext[Tick], signal: Signal
                    ) -> Behavior[Tick]:
                        if isinstance(signal, Terminated):
                            seen.append(str(signal.ref.path))
                        return Behaviors.same()

                    return Behaviors.receive_message(
                        on_message, msg_type=Tick, on_signal=on_signal
                    )

                system.spawn(Behaviors.setup(build), "watcher")

                await eventually(lambda: bool(seen))
                assert seen[0].endswith("/user/ticker#1")
                # And the entry is gone, so nothing reports it a second time
                # when the association ends.
                assert system.remote is not None
                association = system.remote.association_for(address)
                assert association is not None
                assert association.watching == ()
            finally:
                await system.terminate()


class _RecordingLink:
    """A link that records only whether it was closed.

    Enough of the link surface for an association to hold it and close it,
    with none of a real socket, so a test can watch it being released.
    """

    def __init__(self) -> None:
        """Start open."""
        self.closed = False

    @property
    def peer(self) -> str:
        """A fixed peer address, since nothing here dials."""
        return "tapio://retired@127.0.0.1:1"

    async def read_frame(self) -> bytes:
        """Never called: this link is only ever retired."""
        raise AssertionError("a retired link is not read")

    async def write_frame(self, data: bytes) -> None:
        """Never called: this link is only ever retired."""
        raise AssertionError("a retired link is not written")

    async def write_link(self, message: object) -> None:
        """Never called: this link is only ever retired."""
        raise AssertionError("a retired link is not written")

    async def close(self) -> None:
        """Record that the association closed this link."""
        self.closed = True


class _LoneHost:
    """The little of an endpoint an isolated association needs to shut down.

    Reports the system as closing, so ending the association raises no watch,
    and forgets nothing, since it holds no table.
    """

    def __init__(self) -> None:
        """Bind to the running loop, with default remoting settings."""
        self.settings = RemoteSettings(_env_file=None, bind_port=0)  # type: ignore[call-arg]
        self.dispatcher = Dispatcher.from_running_loop()
        self.is_closing = True

    def forget(self, association: object) -> None:
        """Do nothing: there is no table to remove the association from."""


def _lone_association() -> Association:
    """An association with no reader and no link, ready to be shut down."""
    peer = Address.parse("tapio://peer@127.0.0.1:2551")
    return Association(host=_LoneHost(), peer=peer, initiator=peer)  # type: ignore[arg-type]


async def test_release_closes_the_link_a_dial_race_retired():
    # A simultaneous dial retires the losing link into `_retiring`, closed by
    # the `_resume` task. A shutdown can cancel that task before it runs a line,
    # so `_release` must close the retired link itself or its socket is left for
    # the garbage collector. Without that, this link is never closed.
    with assert_no_leaked_tasks():
        association = _lone_association()
        retired = _RecordingLink()
        association._retiring = retired

        await association._release()

        assert retired.closed


async def test_detach_closes_the_link_a_dial_race_retired():
    # The same retired link, on the endpoint's late-adopt path: an association
    # adopted after the stop sweep gets `detach` rather than a `PostStop`, and
    # it too must close a link a dial race left behind.
    with assert_no_leaked_tasks():
        association = _lone_association()
        retired = _RecordingLink()
        association._retiring = retired

        await association.detach()

        assert retired.closed


class _ResumeProbe(Association):
    """An association whose `_run` records that reads resumed, rather than dial.

    A subclass so the test can tell whether `_resume` fell through to resume
    reads after being told to stop, without a real dial happening.
    """

    resumed = False

    async def _run(self) -> None:  # type: ignore[override]
        self.resumed = True


async def _pending() -> None:
    """A task that never finishes on its own, for the reader slot."""
    await asyncio.Event().wait()


async def test_resume_does_not_resume_reads_after_its_own_cancellation():
    # `_resume` runs as the reader task. It cancels the link that lost a dial
    # and awaits it, then reads the link that won. When a shutdown cancels the
    # reader while it sits at that await, the cancellation is the reader's own,
    # so it must stop, not go on to `_run` and resume reads on a socket it was
    # told to abandon. The cancel is requested before `_resume` runs, so it is
    # delivered exactly at the `await reader` the bug needs, with no race.
    with assert_no_leaked_tasks():
        peer = Address.parse("tapio://peer@127.0.0.1:2551")
        association = _ResumeProbe(host=_LoneHost(), peer=peer, initiator=peer)  # type: ignore[arg-type]

        reader: asyncio.Task[None] = asyncio.ensure_future(_pending())
        resume: asyncio.Task[None] = asyncio.ensure_future(association._resume(reader))
        # One turn: `_resume` cancels the retired reader and parks at
        # `await reader`, which is the window the bug needs.
        await asyncio.sleep(0)

        resume.cancel()
        with pytest.raises(asyncio.CancelledError):
            await resume

        assert association.resumed is False

        # `_resume` already cancelled the reader; drain it for a clean exit.
        with contextlib.suppress(asyncio.CancelledError):
            await reader


async def test_cancel_and_wait_reraises_the_callers_own_cancellation():
    # The caller is cancelled while waiting, so it must stop rather than run on.
    reached = False

    async def caller() -> None:
        nonlocal reached
        await _cancel_and_wait(asyncio.ensure_future(_pending()))
        reached = True

    task: asyncio.Task[None] = asyncio.ensure_future(caller())
    await asyncio.sleep(0)  # park inside `_cancel_and_wait` at `await task`
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert reached is False


async def test_cancel_and_wait_swallows_the_awaited_tasks_cancellation():
    # The awaited task's own cancellation is not the caller's, so the caller
    # carries on: this is the ordinary retire path.
    reached = await _returns_after_waiting(asyncio.ensure_future(_pending()))
    assert reached is True


async def test_cancel_and_wait_swallows_the_awaited_tasks_exception():
    async def boom() -> None:
        raise RuntimeError("the reader failed")

    task: asyncio.Task[None] = asyncio.ensure_future(boom())
    with contextlib.suppress(RuntimeError):
        await task  # let it finish and retrieve the exception

    reached = await _returns_after_waiting(task)
    assert reached is True


async def _returns_after_waiting(task: "asyncio.Task[None]") -> bool:
    """Run `_cancel_and_wait` from an uncancelled caller and say it returned."""
    await _cancel_and_wait(task)
    return True
