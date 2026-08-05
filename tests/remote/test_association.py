"""Tests for the association: messages over a real link."""

import asyncio

import pytest

from tapio.actor import ActorSystem, DeadLetter
from tapio.actor.dead_letters import DeadLetterReason
from tapio.errors import MessageEncodingError
from tapio.remote.codec import LENGTH_PREFIX, encode
from tapio.remote.transport import framed, is_link_frame, link_body
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
    remoting,
    uri,
)

# --- messages cross ----------------------------------------------------------


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


# --- what the sender hears, and what it does not -----------------------------


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


# --- what a peer can inflict -------------------------------------------------


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


async def test_asking_across_an_association_says_what_to_do_instead(
    alpha: ActorSystem, beta: ActorSystem
):
    # A remote ask needs the remote death watch to fail fast. Until that
    # lands, an ask here could only time out, so it refuses instead.
    echo = beta.spawn(echoing(), "echo")
    remote = await alpha.resolve(uri(beta, echo), expect=Ping)

    with pytest.raises(NotImplementedError, match="reply_to"):
        await remote.ask(lambda reply_to: Ping(n=1, reply_to=reply_to), expect=Pong)


# --- simultaneous dial -------------------------------------------------------


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


# --- the same code, with a secret and without --------------------------------


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
