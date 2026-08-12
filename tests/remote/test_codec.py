"""Tests for the wire format, with no socket anywhere.

Two systems in one process, with the test handing bytes from one to the other
instead of a link. Everything a peer can get wrong is decided in
`receive_frame`, so it is all testable without a network.
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
from tapio.remote.codec import (
    LENGTH_PREFIX,
    UndecodableFrame,
    decode,
    encode,
    frame_length,
)
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
    """A message with no wire key, so it cannot be encoded."""

    n: int


def settings_for() -> TapioSettings:
    """Settings for a system listening on a loopback port the OS picks."""
    return TapioSettings(
        _env_file=None, remote=RemoteSettings(_env_file=None, bind_port=0)
    )


def unlinked(running: ActorSystem) -> ActorSystem:
    """Stop a system from dialling a peer on its own.

    These systems listen, since a ref needs a dialable address to mean
    anything, but every frame in this file is carried by the test. Removing
    the resolver keeps it that way.
    """
    running.set_peer_resolver(lambda address, path: None)
    return running


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
    """Rewrite part of a frame's body and fix up the length prefix.

    Without recomputing the prefix these tests would all be asserting the
    truncation check instead of what they mean to.
    """
    body = frame[LENGTH_PREFIX:].replace(old, new)
    return len(body).to_bytes(LENGTH_PREFIX, "big") + body


@pytest.fixture
async def alpha() -> AsyncIterator[ActorSystem]:
    """One of the two systems, on a loopback port the OS picks."""
    running = unlinked(ActorSystem("alpha", settings_for()))
    try:
        yield running
    finally:
        await running.terminate()


@pytest.fixture
async def beta() -> AsyncIterator[ActorSystem]:
    """The other, on a different loopback port."""
    running = unlinked(ActorSystem("beta", settings_for()))
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
        return target.resolve_path(address, path)

    source.set_peer_resolver(resolve)


async def test_a_frame_is_a_length_prefix_and_a_body(alpha: ActorSystem):
    stock = alpha.spawn(collecting([]), "stock")
    frame = encode(Reserved(sku="X-1"), to=stock.path)

    declared = int.from_bytes(frame[:LENGTH_PREFIX], "big")
    assert declared == len(frame) - LENGTH_PREFIX


async def test_a_frame_names_its_recipient_without_an_address(alpha: ActorSystem):
    # The recipient needs no address: a frame off a link is addressed to the
    # system that received it.
    stock = alpha.spawn(collecting([]), "stock")
    decoded = decode(encode(Reserved(sku="X-1"), to=stock.path), system="alpha")
    assert decoded.to == stock.path


async def test_a_frame_names_the_system_that_sent_it(alpha: ActorSystem):
    # The sending system, not a sending actor. A `tell` carries no sender, so
    # there is no actor to name; what this is for is letting a dead letter on
    # the far side say which node produced the frame.
    stock = alpha.spawn(collecting([]), "stock")
    frame = encode(Reserved(sku="X-1"), to=stock.path, sender=alpha.address)
    assert decode(frame, system="alpha").sender == str(alpha.address)


async def test_a_frame_with_no_sender_decodes_to_none(alpha: ActorSystem):
    # Nothing requires the field, and a peer that omits it is readable.
    stock = alpha.spawn(collecting([]), "stock")
    assert decode(
        encode(Reserved(sku="X-1"), to=stock.path), system="alpha"
    ).sender is (None)


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
    # Errors about the message belong to the sender, as with a local tell.
    stock = alpha.spawn(collecting([]), "stock")
    with pytest.raises(FrameTooLargeError, match="over the 32 byte frame limit"):
        encode(Reserved(sku="X" * 100), to=stock.path, max_frame_bytes=32)


async def test_a_message_encoded_on_one_system_arrives_on_the_other(
    alpha: ActorSystem, beta: ActorSystem
):
    seen: list[Reserve] = []
    stock = beta.spawn(reserving(seen), "stock")
    cart = alpha.spawn(collecting([]), "cart")

    beta.deliver_frame(
        encode(
            Reserve(sku="X-1", qty=2, reply_to=cart),
            to=stock.path,
            sender=alpha.address,
        ),
        peer=alpha.address,
    )

    await eventually(lambda: len(seen) == 1)
    assert seen[0].sku == "X-1"


async def test_a_reply_to_that_crossed_a_frame_reaches_the_original_actor(
    alpha: ActorSystem, beta: ActorSystem
):
    # The ref in a decoded message is a working handle on the actor it names.
    answers: list[Reserved] = []
    stock = beta.spawn(reserving([]), "stock")
    cart = alpha.spawn(collecting(answers), "cart")
    link(beta, alpha)

    beta.deliver_frame(
        encode(
            Reserve(sku="X-1", qty=2, reply_to=cart),
            to=stock.path,
            sender=alpha.address,
        ),
        peer=alpha.address,
    )

    await eventually(lambda: [answer.sku for answer in answers] == ["X-1"])


async def test_a_reply_can_travel_back_as_a_frame_too(
    alpha: ActorSystem, beta: ActorSystem
):
    # With no link, the decoded `reply_to` still carries the address and uid
    # it was written with, which is all a reply frame needs.
    answers: list[Reserved] = []
    requests: list[Reserve] = []
    stock = beta.spawn(reserving(requests), "stock")
    cart = alpha.spawn(collecting(answers), "cart")

    beta.deliver_frame(
        encode(
            Reserve(sku="X-1", qty=2, reply_to=cart),
            to=stock.path,
            sender=alpha.address,
        ),
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
    # Within a system the recipient gets the object the sender passed. Across
    # a link the message was rebuilt from JSON, so equality is the most that
    # can hold.
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
    # A system reading its own address hands back the ref it already has, so
    # the reply is an ordinary local tell and not a proxy back to itself.
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
    # A system with remoting off writes its refs with a name and no host, and
    # still resolves its own.
    local = beta.spawn(collecting([]), "local")
    text = format_ref(Address(system="beta"), local.path)

    with beta.as_deserialization_context():
        message = Reserve.model_validate({"sku": "X", "qty": 1, "reply_to": text})

    assert message.reply_to is local


def framed(body: bytes) -> bytes:
    """Put an honest length prefix in front of a body written by hand."""
    return len(body).to_bytes(LENGTH_PREFIX, "big") + body


def test_a_length_prefix_that_is_not_four_bytes_is_refused():
    with pytest.raises(MessageDecodingError, match="length prefix is 4 bytes"):
        frame_length(b"\x00\x01")


def test_a_body_shorter_than_its_prefix_says_so():
    # What a reader hands over when a peer disappears mid-write.
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


async def test_a_stale_uid_dead_letters_instead_of_reaching_the_newcomer(
    alpha: ActorSystem, beta: ActorSystem, letters: list[DeadLetter]
):
    # The frame names an incarnation that has stopped, and a new actor now
    # holds that path. This is why a bare path is not enough.
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
    # Refused from the length prefix, before the body is read: a peer
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
    # Sender and recipient are deployed separately, so what the sender
    # declared cannot be trusted. The recipient's own check decides.
    stock = beta.spawn(reserving([]), "stock")

    beta.deliver_frame(encode(Unwanted(n=1), to=stock.path), peer=alpha.address)

    assert len(letters) == 1
    assert letters[0].reason == DeadLetterReason.WRONG_MESSAGE_TYPE
    assert letters[0].message == Unwanted(n=1)
    assert letters[0].peer == str(alpha.address)


async def test_a_full_mailbox_dead_letters_rather_than_raising(
    alpha: ActorSystem, beta: ActorSystem, letters: list[DeadLetter]
):
    # A local sender gets `MailboxFullError` at its call site and can retry.
    # This sender is across a link and has moved on, so there is nobody to
    # raise into.
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
    # `tell` never raises about the recipient, so a ref for an unreachable
    # address accepts the message and dead-letters it.
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
        one = ActorSystem("alpha", settings_for())
        two = ActorSystem("beta", settings_for())
        stock = two.spawn(reserving([]), "stock")
        cart = one.spawn(collecting([]), "cart")
        two.deliver_frame(
            encode(Reserve(sku="X-1", qty=1, reply_to=cart), to=stock.path)
        )
        await one.terminate()
        await two.terminate()

    assert one.refs.paths() == ()
    assert two.refs.paths() == ()


async def test_a_dead_letter_names_the_system_a_bad_frame_came_from(
    alpha: ActorSystem, beta: ActorSystem, letters: list[DeadLetter]
):
    # The point of carrying `from` at all. A frame refused for its type key
    # has no message to report, so without it a subscriber learns that
    # something arrived and nothing about where from.
    stock = beta.spawn(collecting([]), "stock")
    frame = encode(Reserved(sku="X-1"), to=stock.path, sender=alpha.address)

    beta.deliver_frame(tamper(frame, f"{__name__}.Reserved".encode(), b"nope.Nope"))

    assert len(letters) == 1
    assert letters[0].reason == DeadLetterReason.UNKNOWN_MESSAGE_TYPE
    assert isinstance(letters[0].message, UndecodableFrame)
    assert letters[0].message.sender == str(alpha.address)
