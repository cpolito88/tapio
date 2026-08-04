"""The wire format, end to end, with no socket anywhere.

Two systems in one process, and bytes handed from one to the other by the test
instead of by a link. That is the point of proving the codec before opening a
port: everything a peer can get wrong is decided in `receive_frame`, so all of
it is testable without a network, a timeout, or a flaky port.
"""

import asyncio
from collections.abc import AsyncIterator

import pytest

from tapio import Message
from tapio.actor import (
    ActorPath,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    DeadLetter,
    DeadLetterReason,
    MailboxConfig,
    OverflowStrategy,
)
from tapio.errors import (
    FrameTooLargeError,
    MessageDecodingError,
    MessageRegistrationError,
    RefResolutionError,
)
from tapio.remote.address import Address, format_ref
from tapio.remote.codec import LENGTH_PREFIX, decode, encode, frame_length
from tapio.remote.registry import register_message
from tapio.settings import RemoteSettings, TapioSettings
from tapio.testkit import assert_no_leaked_tasks
from tests.failures import eventually


@register_message()
class Reserve(Message):
    """A request that carries the ref its answer should go to."""

    sku: str
    qty: int
    reply_to: ActorRef["Reserved"]


@register_message()
class Reserved(Message):
    """The answer."""

    sku: str


@register_message()
class Unwanted(Message):
    """A registered message that no actor under test accepts."""

    n: int


class Unregistered(Message):
    """A message with no wire key, so nothing can name it on a frame."""

    n: int


def settings_for(port: int) -> TapioSettings:
    """Settings for a system that advertises itself on a loopback port."""
    return TapioSettings(
        _env_file=None, remote=RemoteSettings(_env_file=None, bind_port=port)
    )


def collecting(seen: list[Reserved]) -> Behavior[Reserved]:
    """An actor that writes down every answer it is given."""

    async def on_message(message: Reserved) -> Behavior[Reserved]:
        seen.append(message)
        return Behaviors.same()

    return Behaviors.receive_message(on_message, msg_type=Reserved)


def reserving(seen: list[Reserve]) -> Behavior[Reserve]:
    """An actor that answers every request, and stops on a negative quantity."""

    async def on_message(message: Reserve) -> Behavior[Reserve]:
        if message.qty < 0:
            return Behaviors.stopped()
        seen.append(message)
        message.reply_to.tell(Reserved(sku=message.sku))
        return Behaviors.same()

    return Behaviors.receive_message(on_message, msg_type=Reserve)


def tamper(frame: bytes, old: bytes, new: bytes) -> bytes:
    """Rewrite part of a frame's body and put an honest length in front of it.

    Without recomputing the prefix every one of these tests would be asserting
    the truncation check rather than the thing it means to.
    """
    body = frame[LENGTH_PREFIX:].replace(old, new)
    return len(body).to_bytes(LENGTH_PREFIX, "big") + body


@pytest.fixture
async def alpha() -> AsyncIterator[ActorSystem]:
    """One of the two systems, addressed as `tapio://alpha@127.0.0.1:25520`."""
    running = ActorSystem("alpha", settings_for(25520))
    try:
        yield running
    finally:
        await running.terminate()


@pytest.fixture
async def beta() -> AsyncIterator[ActorSystem]:
    """The other, addressed as `tapio://beta@127.0.0.1:25521`."""
    running = ActorSystem("beta", settings_for(25521))
    try:
        yield running
    finally:
        await running.terminate()


@pytest.fixture
def letters(beta: ActorSystem) -> list[DeadLetter]:
    """Everything beta could not deliver, in order."""
    seen: list[DeadLetter] = []
    beta.dead_letters.subscribe(seen.append)
    return seen


def link(source: ActorSystem, target: ActorSystem) -> None:
    """Let `source` resolve refs for `target`, standing in for a link.

    An association would do this by handing the ref an outbound buffer and a
    socket. In one process the same job is a lookup in the other system's own
    registry, which is what makes the resolution rules testable without any
    I/O at all.
    """

    def resolve(address: Address, path: ActorPath) -> ActorRef[Message] | None:
        if address != target.address:
            return None
        return target.resolve(address, path)

    source.set_peer_resolver(resolve)


# --- the frame itself --------------------------------------------------------


async def test_a_frame_is_a_length_prefix_and_a_body(alpha: ActorSystem):
    stock = alpha.spawn(collecting([]), "stock")
    frame = encode(Reserved(sku="X-1"), to=stock.path)

    declared = int.from_bytes(frame[:LENGTH_PREFIX], "big")
    assert declared == len(frame) - LENGTH_PREFIX


async def test_a_frame_names_its_recipient_without_an_address(alpha: ActorSystem):
    # The recipient needs no address: a frame arriving on a link is by
    # definition addressed to the system that received it.
    stock = alpha.spawn(collecting([]), "stock")
    decoded = decode(encode(Reserved(sku="X-1"), to=stock.path), system="alpha")
    assert decoded.to == stock.path


async def test_a_frame_names_its_sender_in_full(alpha: ActorSystem):
    # The sender needs a complete address: the receiver may have to talk to a
    # system it has never dialled.
    stock = alpha.spawn(collecting([]), "stock")
    frame = encode(Reserved(sku="X-1"), to=stock.path, sender=stock)
    assert decode(frame, system="alpha").sender == format_ref(alpha.address, stock.path)


async def test_a_type_key_is_a_registry_key_and_not_an_import_path(alpha: ActorSystem):
    stock = alpha.spawn(collecting([]), "stock")
    assert decode(encode(Reserved(sku="X-1"), to=stock.path), system="alpha").key == (
        f"{__name__}.Reserved"
    )


async def test_an_unregistered_type_cannot_be_written_to_a_frame(alpha: ActorSystem):
    stock = alpha.spawn(collecting([]), "stock")
    with pytest.raises(MessageRegistrationError, match="no wire key"):
        encode(Unregistered(n=1), to=stock.path)


async def test_an_oversized_frame_raises_at_the_send_site(alpha: ActorSystem):
    # An error about the message belongs to the sender, exactly as it does for
    # a local tell.
    stock = alpha.spawn(collecting([]), "stock")
    with pytest.raises(FrameTooLargeError, match="over the 32 byte frame limit"):
        encode(Reserved(sku="X" * 100), to=stock.path, max_frame_bytes=32)


# --- what arrives ------------------------------------------------------------


async def test_a_message_encoded_on_one_system_arrives_on_the_other(
    alpha: ActorSystem, beta: ActorSystem
):
    seen: list[Reserve] = []
    stock = beta.spawn(reserving(seen), "stock")
    cart = alpha.spawn(collecting([]), "cart")

    beta.deliver_frame(
        encode(Reserve(sku="X-1", qty=2, reply_to=cart), to=stock.path, sender=cart),
        peer=alpha.address,
    )

    await eventually(lambda: len(seen) == 1)
    assert seen[0].sku == "X-1"


async def test_a_reply_to_that_crossed_a_frame_reaches_the_original_actor(
    alpha: ActorSystem, beta: ActorSystem
):
    # What all of this is for: the ref in a decoded message is a working
    # handle on the actor that was named, over on the other system.
    answers: list[Reserved] = []
    stock = beta.spawn(reserving([]), "stock")
    cart = alpha.spawn(collecting(answers), "cart")
    link(beta, alpha)

    beta.deliver_frame(
        encode(Reserve(sku="X-1", qty=2, reply_to=cart), to=stock.path, sender=cart),
        peer=alpha.address,
    )

    await eventually(lambda: [answer.sku for answer in answers] == ["X-1"])


async def test_a_reply_can_travel_back_as_a_frame_too(
    alpha: ActorSystem, beta: ActorSystem
):
    # Without a link, the decoded `reply_to` still carries the address and uid
    # it was written with, which is all a reply frame needs.
    answers: list[Reserved] = []
    requests: list[Reserve] = []
    stock = beta.spawn(reserving(requests), "stock")
    cart = alpha.spawn(collecting(answers), "cart")

    beta.deliver_frame(
        encode(Reserve(sku="X-1", qty=2, reply_to=cart), to=stock.path, sender=cart),
        peer=alpha.address,
    )
    await eventually(lambda: len(requests) == 1)

    reply_to = requests[0].reply_to
    assert reply_to.address == alpha.address
    alpha.deliver_frame(
        encode(Reserved(sku="X-1"), to=reply_to.path), peer=beta.address
    )

    await eventually(lambda: [answer.sku for answer in answers] == ["X-1"])


async def test_a_message_off_the_wire_equals_what_was_sent_without_being_it(
    alpha: ActorSystem, beta: ActorSystem
):
    # Where the identity invariant stops, stated as a test. Within a system
    # the recipient gets the object the sender passed; across a link the
    # message was rebuilt from JSON, so equality is the strongest thing that
    # can be true.
    seen: list[Reserve] = []
    stock = beta.spawn(reserving(seen), "stock")
    cart = alpha.spawn(collecting([]), "cart")
    sent = Reserve(sku="X-1", qty=2, reply_to=cart)

    beta.deliver_frame(encode(sent, to=stock.path), peer=alpha.address)

    await eventually(lambda: len(seen) == 1)
    assert seen[0] == sent
    assert seen[0] is not sent


async def test_a_ref_for_the_reading_system_resolves_to_the_live_local_ref(
    alpha: ActorSystem, beta: ActorSystem
):
    # Not a proxy back to itself: a system reading its own address in a ref
    # hands back the ref it already has, so the reply is an ordinary local
    # tell.
    seen: list[Reserve] = []
    stock = beta.spawn(reserving(seen), "stock")
    local = beta.spawn(collecting([]), "local")

    beta.deliver_frame(
        encode(Reserve(sku="X-1", qty=1, reply_to=local), to=stock.path),
        peer=alpha.address,
    )

    await eventually(lambda: len(seen) == 1)
    assert seen[0].reply_to is local


async def test_an_unaddressable_ref_from_the_reading_system_resolves_locally(
    beta: ActorSystem,
):
    # A system with remoting off writes its refs with a name and no host. The
    # system that owns that name still knows what it means.
    local = beta.spawn(collecting([]), "local")
    text = format_ref(Address(system="beta"), local.path)

    with beta.as_deserialization_context():
        message = Reserve.model_validate({"sku": "X", "qty": 1, "reply_to": text})

    assert message.reply_to is local


# --- frames that do not survive the decode -----------------------------------


def framed(body: bytes) -> bytes:
    """Put an honest length prefix in front of a body written by hand."""
    return len(body).to_bytes(LENGTH_PREFIX, "big") + body


def test_a_length_prefix_that_is_not_four_bytes_is_refused():
    with pytest.raises(MessageDecodingError, match="length prefix is 4 bytes"):
        frame_length(b"\x00\x01")


def test_a_body_shorter_than_its_prefix_says_so():
    # The half-read frame a reader hands over when a peer disappears mid-write.
    with pytest.raises(MessageDecodingError, match="declares 99 bytes and carries"):
        decode((99).to_bytes(LENGTH_PREFIX, "big") + b"{}", system="beta")


def test_a_body_that_is_not_an_object_is_refused():
    with pytest.raises(MessageDecodingError, match="a JSON object, got list"):
        decode(framed(b"[]"), system="beta")


def test_a_frame_missing_a_field_is_refused():
    with pytest.raises(MessageDecodingError, match="malformed frame"):
        decode(framed(b'{"v":1,"to":"/user/x"}'), system="beta")


def test_a_type_key_that_is_not_a_string_is_refused():
    body = b'{"v":1,"to":"/user/x","t":17,"p":{}}'
    with pytest.raises(MessageDecodingError, match="type key is a string"):
        decode(framed(body), system="beta")


def test_a_recipient_that_is_not_a_path_is_refused():
    body = b'{"v":1,"to":"user/x","t":"k","p":{}}'
    with pytest.raises(MessageDecodingError, match="an absolute path"):
        decode(framed(body), system="beta")


# --- what a peer can get wrong ----------------------------------------------


async def test_a_stale_uid_dead_letters_instead_of_reaching_the_newcomer(
    alpha: ActorSystem, beta: ActorSystem, letters: list[DeadLetter]
):
    # The reason a bare path is not enough. The frame is addressed to an
    # incarnation that has stopped, and a new actor now holds that name.
    seen: list[Reserve] = []
    first = beta.spawn(reserving(seen), "stock")
    stale = first.path
    cart = alpha.spawn(collecting([]), "cart")
    first.tell(Reserve(sku="X-1", qty=-1, reply_to=cart))
    await eventually(lambda: beta.refs.lookup(stale) is None)
    beta.spawn(reserving(seen), "stock")

    beta.deliver_frame(
        encode(Reserve(sku="X-1", qty=1, reply_to=cart), to=stale), peer=alpha.address
    )

    await eventually(lambda: len(letters) == 1)
    assert letters[0].reason == DeadLetterReason.UNKNOWN_RECIPIENT
    assert letters[0].recipient == str(stale)
    assert seen == []


async def test_an_unregistered_type_key_dead_letters_with_the_key_named(
    alpha: ActorSystem, beta: ActorSystem, letters: list[DeadLetter]
):
    stock = beta.spawn(reserving([]), "stock")
    frame = tamper(
        encode(Reserved(sku="X-1"), to=stock.path),
        f"{__name__}.Reserved".encode(),
        b"orders.protocol.Nothing",
    )

    beta.deliver_frame(frame, peer=alpha.address)

    assert len(letters) == 1
    assert letters[0].reason == DeadLetterReason.UNKNOWN_MESSAGE_TYPE
    assert letters[0].detail is not None
    assert "orders.protocol.Nothing" in letters[0].detail
    assert letters[0].peer == str(alpha.address)


async def test_an_oversized_frame_dead_letters_with_the_size_named(
    beta: ActorSystem, letters: list[DeadLetter]
):
    # Refused from the length prefix alone, before the body is read: a peer
    # announcing a gigabyte must not cost a gigabyte.
    stock = beta.spawn(reserving([]), "stock")
    frame = encode(Reserved(sku="X-1"), to=stock.path)
    huge = (1_000_000_000).to_bytes(LENGTH_PREFIX, "big") + frame[LENGTH_PREFIX:]

    beta.deliver_frame(huge)

    assert len(letters) == 1
    assert letters[0].reason == DeadLetterReason.FRAME_TOO_LARGE
    assert letters[0].detail is not None
    assert "1000000000" in letters[0].detail


async def test_a_body_that_is_not_json_dead_letters(
    beta: ActorSystem, letters: list[DeadLetter]
):
    beta.deliver_frame((3).to_bytes(LENGTH_PREFIX, "big") + b"not")

    assert len(letters) == 1
    assert letters[0].reason == DeadLetterReason.MALFORMED_FRAME


async def test_a_frame_from_a_future_version_dead_letters(
    beta: ActorSystem, letters: list[DeadLetter]
):
    stock = beta.spawn(reserving([]), "stock")
    frame = tamper(encode(Reserved(sku="X-1"), to=stock.path), b'"v":1', b'"v":99')

    beta.deliver_frame(frame)

    assert len(letters) == 1
    assert letters[0].reason == DeadLetterReason.MALFORMED_FRAME
    assert letters[0].detail is not None
    assert "99" in letters[0].detail


async def test_a_payload_that_does_not_validate_dead_letters(
    beta: ActorSystem, letters: list[DeadLetter]
):
    stock = beta.spawn(reserving([]), "stock")
    frame = tamper(encode(Reserved(sku="X-1"), to=stock.path), b'"X-1"', b"17")

    beta.deliver_frame(frame)

    assert len(letters) == 1
    assert letters[0].reason == DeadLetterReason.MALFORMED_FRAME


async def test_a_message_the_recipient_does_not_accept_dead_letters(
    alpha: ActorSystem, beta: ActorSystem, letters: list[DeadLetter]
):
    # The authoritative check, and the only one that can be trusted: what the
    # sender declared and what the recipient actually accepts are two
    # independently deployed pieces of code.
    stock = beta.spawn(reserving([]), "stock")

    beta.deliver_frame(encode(Unwanted(n=1), to=stock.path), peer=alpha.address)

    assert len(letters) == 1
    assert letters[0].reason == DeadLetterReason.WRONG_MESSAGE_TYPE
    assert letters[0].message == Unwanted(n=1)
    assert letters[0].peer == str(alpha.address)


async def test_a_full_mailbox_dead_letters_rather_than_raising(
    alpha: ActorSystem, beta: ActorSystem, letters: list[DeadLetter]
):
    # A local sender would have `MailboxFullError` raised at its call site,
    # where it can retry or shed. The sender here is on the other side of a
    # link and has moved on, so there is nobody to raise into.
    gate = asyncio.Event()

    async def blocked(message: Reserve) -> Behavior[Reserve]:
        await gate.wait()
        return Behaviors.same()

    stock = beta.spawn(
        Behaviors.receive_message(blocked, msg_type=Reserve),
        "stock",
        MailboxConfig(capacity=1, on_overflow=OverflowStrategy.FAIL),
    )
    cart = alpha.spawn(collecting([]), "cart")
    frame = encode(Reserve(sku="X-1", qty=1, reply_to=cart), to=stock.path)

    for _ in range(4):
        beta.deliver_frame(frame, peer=alpha.address)
        await asyncio.sleep(0)
    gate.set()

    assert [letter.reason for letter in letters] == [DeadLetterReason.MAILBOX_FULL] * 2
    assert letters[0].peer == str(alpha.address)


async def test_a_ref_for_a_system_with_no_link_dead_letters_on_use(
    alpha: ActorSystem, beta: ActorSystem
):
    # `tell` stays total. A ref for an address nothing can reach accepts the
    # message and accounts for it, rather than raising into the sender.
    seen: list[DeadLetter] = []
    beta.dead_letters.subscribe(seen.append)
    requests: list[Reserve] = []
    stock = beta.spawn(reserving(requests), "stock")
    cart = alpha.spawn(collecting([]), "cart")

    beta.deliver_frame(
        encode(Reserve(sku="X-1", qty=1, reply_to=cart), to=stock.path),
        peer=alpha.address,
    )

    await eventually(lambda: len(seen) == 1)
    assert seen[0].reason == DeadLetterReason.NO_ASSOCIATION
    assert seen[0].peer == str(alpha.address)


# --- the deserialization context --------------------------------------------


async def test_a_ref_does_not_deserialize_without_a_system(alpha: ActorSystem):
    cart = alpha.spawn(collecting([]), "cart")
    dumped = Reserve(sku="X-1", qty=1, reply_to=cart).model_dump()

    with pytest.raises(RefResolutionError, match="without a system"):
        Reserve.model_validate(dumped)


async def test_the_context_is_put_back_when_the_block_ends(alpha: ActorSystem):
    cart = alpha.spawn(collecting([]), "cart")
    dumped = Reserve(sku="X-1", qty=1, reply_to=cart).model_dump()

    with alpha.as_deserialization_context():
        assert Reserve.model_validate(dumped).reply_to is cart

    with pytest.raises(RefResolutionError):
        Reserve.model_validate(dumped)


async def test_receiving_a_frame_leaves_no_context_behind(
    alpha: ActorSystem, beta: ActorSystem
):
    stock = beta.spawn(reserving([]), "stock")
    cart = alpha.spawn(collecting([]), "cart")

    beta.deliver_frame(encode(Reserve(sku="X-1", qty=1, reply_to=cart), to=stock.path))

    with pytest.raises(RefResolutionError):
        Reserve.model_validate(Reserve(sku="X", qty=1, reply_to=cart).model_dump())


async def test_two_systems_terminate_leaving_nothing_behind():
    with assert_no_leaked_tasks():
        one = ActorSystem("alpha", settings_for(25520))
        two = ActorSystem("beta", settings_for(25521))
        stock = two.spawn(reserving([]), "stock")
        cart = one.spawn(collecting([]), "cart")
        two.deliver_frame(
            encode(Reserve(sku="X-1", qty=1, reply_to=cart), to=stock.path)
        )
        await one.terminate()
        await two.terminate()

    assert one.refs.paths() == ()
    assert two.refs.paths() == ()
