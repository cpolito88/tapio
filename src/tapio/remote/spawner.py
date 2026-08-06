"""Starting an actor on another node, without supervising it from here.

An actor is started elsewhere by asking a **spawner** there to start it
locally. There is no configuration that places actors, and there is no
parent-child relationship across a link.

```python
spawner = await ctx.resolve(
    "tapio://compute@10.0.0.7:25520/user/spawner", expect=Spawn)

reply = await spawner.ask(
    lambda r: Spawn(factory="worker", args=WorkerArgs(size=4), reply_to=r),
    expect=SpawnReply,
)
match reply:
    case Spawned(ref=worker):
        ctx.watch(worker)      # death watch replaces the parent-child link
    case SpawnFailed(reason=reason):
        ...
```

**Why not `ctx.spawn` with a placement setting**, which is the obvious design.
It would make the local parent the supervisor of a remote child, so every
lifecycle operation becomes a frame: restart, stop, resume and the failure
report itself all cross a link that can be quarantined halfway through a
decision. Supervision is the one thing in this library that has to be able to
answer. So the tree stays inside one node, and what crosses the wire is the
weaker relationship that survives a partition honestly:

* **The spawned actor is the spawner's child**, supervised by whatever
  strategy its factory wrapped it in, on the spawner's node, at in-process
  latency. A worker that keeps failing is restarted by a parent one process
  boundary away from it, and the requester never hears about it.
* **The requester holds a ref and watches it.** `Terminated` arrives when the
  actor stops, when its node stops, and when the association is quarantined.
  All three mean the same thing to the requester: that worker is gone, ask for
  another. It is a smaller contract than supervision, and it is the largest
  one a network can keep.
* **`Spawn` is an ordinary message and a spawner is an ordinary actor.** The
  wire protocol does not grow by a single frame type for any of this.

**Both nodes must be running the same code.** A behavior is a closure and a
closure does not cross a socket, so what crosses is a key and an arguments
model. The peer looks the key up in its own registry, exactly as it looks up a
message type, and imports nothing. When the peer has never heard of the key it
answers `SpawnFailed` with reason `unknown-factory`, which is what a version
skew between two nodes looks like from here.

**A spawner offers named factories, not the registry.** An actor that will
start anything registered, on request, is a capability handed to whoever can
reach the port. The allowlist is checked at construction, so a spawner that
offers a key nobody registered fails where it is written rather than when a
peer asks.
"""

import inspect
import typing
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Annotated, Any, Final, TypeAlias, TypeVar, final

from pydantic import PlainValidator, ValidationError

from tapio.actor.behavior import Behavior, Behaviors
from tapio.actor.context import ActorContext
from tapio.actor.ref import ActorRef
from tapio.errors import (
    ActorNameError,
    ActorSystemTerminating,
    BehaviorRegistrationError,
    TapioError,
)
from tapio.message import Message
from tapio.remote.registry import register_message

__all__ = [
    "Arguments",
    "NoArgs",
    "RemoteFactory",
    "Spawn",
    "SpawnFailed",
    "SpawnFailure",
    "SpawnReply",
    "Spawned",
    "factory_for_key",
    "offered_keys",
    "remote_behavior",
    "spawner",
]

FactoryFunction: TypeAlias = Callable[[Any], Behavior[Any]]
"""Builds a behavior from one arguments model."""

F = TypeVar("F", bound=FactoryFunction)

_GENERATED_PREFIX: Final = "$"
"""What a generated name starts with, and what a peer may not ask for."""


def _as_arguments(value: object) -> dict[str, Any]:
    """Take an arguments model, or a JSON object, and keep a JSON object.

    Arguments travel as a JSON object rather than as a message, because which
    model to build them into is the peer's answer and not the sender's. The
    peer looks the factory up first and validates the object against the model
    that factory declared, so nothing is constructed from a frame before the
    key naming it has been checked.
    """
    if isinstance(value, Message):
        return value.model_dump(mode="json")
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        return dict(value)
    msg = (
        f"a Spawn carries an arguments model or a JSON object, not "
        f"{type(value).__name__}"
    )
    raise ValueError(msg)


Arguments: TypeAlias = Annotated[Any, PlainValidator(_as_arguments)]
"""What a factory is called with, on the wire.

Write the model and it is dumped for you: `Spawn(factory="worker",
args=WorkerArgs(size=4))`. Whether that model is the one the factory wants is
checked on the peer, like every other claim a sender makes about a peer, which
is why the field admits `Any` and holds a JSON object.
"""


class NoArgs(Message):
    """The arguments of a factory that needs none.

    Every factory takes exactly one arguments model, so that the rule is one
    sentence long rather than two with a special case. A factory that needs
    nothing declares this one.
    """


class SpawnReply(Message):
    """What a spawner answers, either way.

    Both answers share a base so that one `ask` can receive either:
    `expect=SpawnReply` accepts a refusal as an ordinary reply rather than
    failing the ask with `AskTypeError`. A refusal is news the requester can
    act on, not a broken protocol.
    """


@register_message()
class Spawned(SpawnReply):
    """An actor was started, and this is the handle to it."""

    factory: str
    """The key it was started from."""

    name: str
    """The name it was given, generated when the request named none."""

    ref: ActorRef[Any]
    """A ref to the new actor.

    Typed as `Any` because the type parameter cannot cross a socket: a frame
    carries a path and an address, and nothing that could reconstruct a type
    argument. Assign it to an `ActorRef[YourProtocol]` and the claim is checked
    where every claim about a peer is checked, on the receiving node, against
    the actor's real message type.
    """


@register_message()
class SpawnFailed(SpawnReply):
    """No actor was started, and this is why."""

    factory: str
    """The key that was asked for."""

    reason: str
    """One of the constants in [SpawnFailure][tapio.remote.spawner.SpawnFailure]."""

    detail: str
    """What happened, in a sentence, for a log or an error message."""


@register_message()
class Spawn(Message):
    """Ask a spawner to start an actor on its own node."""

    factory: str
    """The key of the factory to start, as registered on the peer."""

    args: Arguments = {}  # noqa: RUF012 - a Pydantic field default, copied per use
    """What to call the factory with. Pass the arguments model itself."""

    name: str | None = None
    """What to call the actor. Generated by the peer when omitted.

    A generated name cannot collide, so a name is worth asking for only when
    something else has to be able to find the actor by path.
    """

    reply_to: ActorRef[SpawnReply]
    """Where the answer goes. `ask` fills this in."""


class SpawnFailure:
    """Why a spawner refused.

    String constants in a namespace rather than an enum, for the reason a dead
    letter's reason is one: the set grows, and a peer running an older version
    has to be able to read a reason it has never seen instead of failing to
    decode the reply.
    """

    UNKNOWN_FACTORY = "unknown-factory"
    """The peer has no factory registered under that key. Almost always version
    skew: both nodes have to be running the same code, since what crosses the
    wire is a key and never the behavior itself."""

    NOT_ALLOWED = "not-allowed"
    """The key is registered on the peer, and that spawner does not offer it. A
    spawner's allowlist is the whole of what it will start, because a spawner
    that starts anything registered is a capability handed to whoever can reach
    the port."""

    INVALID_ARGS = "invalid-args"
    """The arguments did not validate against the model the factory declared.
    The sender's idea of what a factory takes is a claim about the peer, and
    this is the check that decides."""

    NAME_REFUSED = "name-refused"
    """The requested name is already taken by a live child of that spawner, or
    is not a name an actor can have."""

    FACTORY_FAILED = "factory-failed"
    """The factory raised while building the behavior. The spawner replies
    rather than failing, because it is the parent of every actor it has
    started and one bad request must not stop the rest of them."""

    TERMINATING = "terminating"
    """The peer, or the spawner itself, is shutting down."""


@final
@dataclass(frozen=True, slots=True)
class RemoteFactory:
    """One behavior a peer can ask for, by key."""

    key: str
    """What a frame names it by."""

    build: FactoryFunction
    """Called with the arguments model to produce the behavior."""

    args_type: type[Message]
    """The model the arguments are validated into, before `build` sees them."""

    def arguments(self, args: dict[str, Any]) -> Message:
        """Build the arguments model from what arrived.

        Args:
            args: The JSON object the request carried.

        Returns:
            The model this factory declared.

        Raises:
            pydantic.ValidationError: If the object does not satisfy it.
        """
        return self.args_type.model_validate(args)


_BY_KEY: dict[str, RemoteFactory] = {}


def remote_behavior(
    key: str | None = None, *, args: type[Message] | None = None
) -> Callable[[F], F]:
    """Register a behavior a peer can ask a spawner to start.

    ```python
    @remote_behavior("worker")
    def worker(args: WorkerArgs) -> Behavior[Job]: ...
    ```

    The factory takes exactly one arguments model and returns the behavior to
    start. Wrap that behavior in `Behaviors.supervise(...)` here if it should
    be supervised: supervision happens entirely on the node that runs the
    actor, so this is the only place it can be declared.

    Arguments models may not carry an `ActorRef`. They are validated on the
    peer after the factory key has been checked, which is deliberately outside
    the decode that resolves refs, so a ref in them has nothing to resolve
    against. Send the new actor a message instead: the requester holds
    [Spawned.ref][tapio.remote.spawner.Spawned] and can tell it whatever it
    needs to know, and refs in *that* message resolve normally.

    Args:
        key: What a frame names this factory by. The function's name when
            omitted. Unlike a message key, the default is not qualified by
            module: a spawner's allowlist is written out by hand and reads
            better short.
        args: The arguments model, when the annotation cannot say. Read from
            the factory's single parameter when omitted.

    Returns:
        The decorator, which returns the function unchanged.

    Raises:
        BehaviorRegistrationError: If the key is already taken, or if the
            arguments model cannot be resolved. Both are raised at import
            time: a duplicate key would otherwise decide itself by import
            order, and a factory whose arguments cannot be built is one no
            peer could ever call.
    """

    def decorate(factory: F) -> F:
        wire_key = key if key is not None else factory.__name__
        taken = _BY_KEY.get(wire_key)
        if taken is not None and taken.build is not factory:
            msg = (
                f"cannot register {_name_of(factory)} under {wire_key!r}: "
                f"{_name_of(taken.build)} already has that key. Two factories "
                "sharing a key would start whichever imported last, so pass an "
                "explicit key to one of them"
            )
            raise BehaviorRegistrationError(msg)
        _BY_KEY[wire_key] = RemoteFactory(
            key=wire_key,
            build=factory,
            args_type=_resolve_args_type(factory, explicit=args),
        )
        return factory

    return decorate


def factory_for_key(key: str) -> RemoteFactory | None:
    """Return the factory registered under a key, if this node has one.

    Args:
        key: The key a request named.

    Returns:
        The factory, or `None`. `None` is the answer a peer running different
        code produces, and it is reported rather than guessed at: nothing is
        imported to find out what the key might have meant.
    """
    return _BY_KEY.get(key)


def offered_keys() -> tuple[str, ...]:
    """Every factory key registered in this process, sorted.

    Registration says a behavior *may* be started by a peer. A spawner still
    has to offer it. This is here for a test, and for the error a spawner
    raises when it is asked to offer a key nobody registered.
    """
    return tuple(sorted(_BY_KEY))


def spawner(offers: Iterable[str]) -> Behavior[Spawn]:
    """Build the actor that starts other actors on this node, on request.

    ```python
    system.spawn(spawner(offers=["worker"]), name="spawner")
    ```

    It is an ordinary actor. Nothing about it is special to the runtime: the
    request is a message, the answer is a message, and the actors it starts are
    its own children, supervised by it and never by whoever asked. Give it a
    path a peer can name, since a peer reaches it by `resolve` like anything
    else.

    Args:
        offers: The factory keys this spawner will start. Nothing else, whether
            or not it is registered.

    Returns:
        The behavior to spawn.

    Raises:
        BehaviorRegistrationError: If a key is offered that no
            `@remote_behavior` registered. The allowlist is checked here rather
            than when a peer asks, so a typo in it fails where it was written.
    """
    allowed = frozenset(offers)
    unknown = sorted(key for key in allowed if key not in _BY_KEY)
    if unknown:
        msg = (
            f"a spawner cannot offer {unknown}: no @remote_behavior is "
            f"registered under {'those keys' if len(unknown) > 1 else 'that key'}. "
            f"This process registers {list(offered_keys())}"
        )
        raise BehaviorRegistrationError(msg)

    async def on_spawn(ctx: ActorContext[Spawn], message: Spawn) -> Behavior[Spawn]:
        message.reply_to.tell(_answer(ctx, message, allowed))
        return Behaviors.same()

    return Behaviors.receive(on_spawn, msg_type=Spawn)


def _answer(
    ctx: ActorContext[Spawn], message: Spawn, allowed: frozenset[str]
) -> SpawnReply:
    """Start what was asked for, or say why not.

    Every failure is a reply rather than an exception. A spawner is the parent
    of every actor it has started, so failing it over one request would stop
    workers that have nothing to do with that request, and the requester would
    learn about it as a lost link instead of as an answer.
    """
    key = message.factory
    factory = _BY_KEY.get(key)
    if factory is None:
        ctx.log.warning("refusing to spawn %r: no factory has that key", key)
        return SpawnFailed(
            factory=key,
            reason=SpawnFailure.UNKNOWN_FACTORY,
            detail=(
                f"no behavior is registered under {key!r} on {ctx.path.system}. "
                "Both nodes have to be running the same code: a key crosses the "
                "wire and a behavior never does"
            ),
        )
    if key not in allowed:
        ctx.log.warning("refusing to spawn %r: this spawner does not offer it", key)
        return SpawnFailed(
            factory=key,
            reason=SpawnFailure.NOT_ALLOWED,
            detail=(
                f"{ctx.path} does not offer {key!r}; it offers "
                f"{sorted(allowed)}. A spawner starts what it was told to start "
                "and nothing else"
            ),
        )

    try:
        args = factory.arguments(message.args)
    except ValidationError as error:
        return SpawnFailed(
            factory=key,
            reason=SpawnFailure.INVALID_ARGS,
            detail=f"{key!r} takes {factory.args_type.__name__}: {error}",
        )

    name = message.name
    if name is not None and name.startswith(_GENERATED_PREFIX):
        return SpawnFailed(
            factory=key,
            reason=SpawnFailure.NAME_REFUSED,
            detail=(
                f"{name!r} starts with {_GENERATED_PREFIX!r}, which is reserved "
                "for names the runtime generates. Ask for another, or ask for "
                "none and take the generated one"
            ),
        )

    try:
        behavior = factory.build(args)
    except Exception as error:  # a reply beats stopping the spawner
        ctx.log.exception("the factory for %r raised", key)
        return SpawnFailed(
            factory=key,
            reason=SpawnFailure.FACTORY_FAILED,
            detail=f"building {key!r} raised {type(error).__name__}: {error}",
        )

    try:
        ref = (
            ctx.spawn_anonymous(behavior) if name is None else ctx.spawn(behavior, name)
        )
    except ActorSystemTerminating as error:
        return SpawnFailed(
            factory=key, reason=SpawnFailure.TERMINATING, detail=str(error)
        )
    except (ActorNameError, ValueError) as error:
        return SpawnFailed(
            factory=key, reason=SpawnFailure.NAME_REFUSED, detail=str(error)
        )
    except TapioError as error:
        # A behavior with no resolvable message type is the case in hand, and
        # it is the factory's bug rather than the requester's.
        return SpawnFailed(
            factory=key, reason=SpawnFailure.FACTORY_FAILED, detail=str(error)
        )

    ctx.log.info("started %s from %r", ref.path, key)
    return Spawned(factory=key, name=ref.path.name, ref=ref)


def _resolve_args_type(
    factory: FactoryFunction, *, explicit: type[Message] | None
) -> type[Message]:
    """Work out which model a factory is called with.

    Explicit wins, an annotation is the fallback, and neither is a loud
    failure. A factory whose arguments cannot be built is one no peer could
    call, so it is rejected where it is written.
    """
    name = _name_of(factory)
    if explicit is not None:
        return _check_args_type(explicit, origin=name)

    try:
        hints = typing.get_type_hints(factory)
        parameters = [
            parameter
            for parameter in inspect.signature(factory).parameters.values()
            if parameter.kind
            in (parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD)
        ]
    except Exception as error:  # every failure to read a signature is the same
        msg = (
            f"cannot read the signature of {name}: {error}. Pass args= to say "
            "what this factory takes."
        )
        raise BehaviorRegistrationError(msg) from error

    if len(parameters) != 1:
        msg = (
            f"{name} takes {len(parameters)} positional parameters; a remote "
            "behavior takes exactly one arguments model. A factory that needs "
            "no arguments declares NoArgs, so that the rule stays one sentence."
        )
        raise BehaviorRegistrationError(msg)

    annotation = hints.get(parameters[0].name)
    if annotation is None:
        msg = (
            f"cannot resolve the arguments of {name}: its parameter "
            f"{parameters[0].name!r} has no annotation. Annotate it, or pass "
            "args= explicitly."
        )
        raise BehaviorRegistrationError(msg)
    return _check_args_type(annotation, origin=name)


def _check_args_type(candidate: object, *, origin: str) -> type[Message]:
    """Accept one `Message` subclass, and reject everything else by name."""
    if isinstance(candidate, type) and issubclass(candidate, Message):
        return candidate
    described = getattr(candidate, "__name__", repr(candidate))
    msg = (
        f"{origin} declares {described} as its arguments, which is not a "
        "tapio.Message subclass. Arguments are rebuilt from JSON on the peer, "
        "so they have to be a model with one type: a union has no single "
        "answer, and anything else has no way back from a frame."
    )
    raise BehaviorRegistrationError(msg)


def _name_of(obj: object) -> str:
    """Best available name for a callable, for an error message."""
    return getattr(obj, "__qualname__", None) or repr(obj)
