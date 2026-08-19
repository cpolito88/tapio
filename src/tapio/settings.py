"""System-wide settings, read from the environment with a `TAPIO_` prefix."""

from datetime import timedelta
from typing import Annotated

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from tapio.actor.mailbox import MailboxConfig, OverflowStrategy

__all__ = [
    "ClusterSettings",
    "ManagementSettings",
    "RemoteSettings",
    "TLSSettings",
    "TapioSettings",
]


class TLSSettings(BaseSettings):
    """Certificates for a link, and optionally for the peer on the other end.

    The shared secret proves who a peer is. TLS keeps the conversation
    private. They answer different questions, so they are configured
    separately, and both are recommended for anything crossing a machine
    boundary. A secret sent in plaintext protects the handshake and nothing
    after it.
    """

    model_config = SettingsConfigDict(env_prefix="TAPIO_REMOTE_TLS_", frozen=True)

    certfile: str
    """This system's certificate, presented to every peer."""

    keyfile: str | None = None
    """The private key, when it is not in `certfile`."""

    cafile: str | None = None
    """The authority peers' certificates are checked against.

    Set on both ends for mutual authentication: a server with this set requires
    a client certificate, and a client with it set verifies the server's.
    """

    check_hostname: bool = True
    """Whether a dialled peer's certificate must match the host dialled.

    Off only for the deployments where the canonical host is not what the
    certificate names, which is a thing to know about rather than to discover.
    """


class RemoteSettings(BaseSettings):
    """Where this system listens, and how peers address it.

    Nested under `TapioSettings.remote` rather than spread across it, so
    "is this system reachable from outside the process" is one `is None`
    check, not a handful of defaults that each look harmless on their own.

    Bind and canonical are separate because they often differ. With
    containers, NAT or port mapping, the address a peer dials is not the one
    the socket is bound to. A ref always writes down the canonical one.
    """

    model_config = SettingsConfigDict(env_prefix="TAPIO_REMOTE_", frozen=True)

    bind_host: str = "127.0.0.1"
    """The interface to listen on. Loopback by default: a port that accepts
    frames naming actor paths and message types is a serious surface, and the
    default is set for someone who has not thought about it yet."""

    bind_port: int = 25520
    """The port to listen on. `0` takes whatever the OS hands out, and the
    canonical port then follows the one it bound, so a test or a sidecar that
    cannot pick a port in advance still advertises a dialable address."""

    canonical_host: str | None = None
    """The host peers dial. `bind_host` when omitted."""

    canonical_port: int | None = None
    """The port peers dial. `bind_port` when omitted."""

    max_frame_bytes: int = 4 * 1024 * 1024
    """Refuse a frame larger than this, before its body is read."""

    secret: SecretStr | None = None
    """The shared secret both ends prove they hold during the handshake.

    Required to bind anywhere but loopback: a system that accepts frames naming
    actor paths and message types from any host that can reach the port, with
    nothing to prove, fails to start rather than serving strangers.
    """

    tls: TLSSettings | None = None
    """Certificates for the link, or `None` for plaintext."""

    handshake_timeout: timedelta = timedelta(seconds=5)
    """How long a link has to be dialled, accepted and handshaken.

    One deadline for the whole opening, so a peer that accepts a connection and
    then says nothing costs this and not a parked task.
    """

    heartbeat_interval: timedelta = timedelta(seconds=1)
    """How often an idle association writes a heartbeat.

    A link that carries traffic needs none of these; they exist so that silence
    can be told from a peer with nothing to say.
    """

    unreachable_after: timedelta = timedelta(seconds=10)
    """How long a link may be silent before the peer is declared unreachable.

    Nothing arriving for this long, heartbeats included, means the peer is
    gone as far as this system can tell. Every local watcher of an actor over
    there is told `Terminated`, the association is quarantined, and recovery
    is an explicit `remote.reconnect`. That verdict can be wrong: a partition,
    a long pause and an overloaded peer all look the same from one node, and
    resolving which it was needs membership and a quorum that a single system
    does not have. Set it well above the peer's `heartbeat_interval`.
    """

    outbound_capacity: int = 10_000
    """Frames one association will hold for a peer that is not reading.

    Backpressure against a socket, and deliberately not backpressure from the
    receiving actor: nothing in a fire-and-forget wire protocol can offer the
    latter. What overflows here goes to dead letters with the peer named.
    """


class ClusterSettings(BaseSettings):
    """How this node gossips, and how patient it is while joining.

    Passed to [Cluster][tapio.cluster.cluster.Cluster] rather than nested in
    `TapioSettings`, because a cluster is something an application starts and
    hands a list of seeds to. Remoting has to be configured before the system
    exists, since the port settles the canonical address; joining a cluster is
    an action taken afterwards.
    """

    model_config = SettingsConfigDict(env_prefix="TAPIO_CLUSTER_", frozen=True)

    roles: frozenset[str] = frozenset()
    """What this node says it is for. Every cluster-aware feature filters on
    these, and they are fixed for the life of the member: a role is part of
    what the rest of the cluster agreed on when it accepted the node."""

    gossip_interval: timedelta = timedelta(seconds=1)
    """How often this node sends its view to one other member.

    One peer per round, chosen at random, which is what keeps the traffic
    linear in the number of nodes rather than quadratic."""

    join_retry_interval: timedelta = timedelta(seconds=1)
    """How often an unjoined node asks the seeds to let it in again.

    Joining is at-most-once like every other send, so it is retried rather
    than acknowledged. The retries stop as soon as the node sees itself in the
    gossip it receives."""

    seed_form_after: timedelta = timedelta(seconds=5)
    """How long the first seed waits before forming a cluster on its own.

    Only the first node in the seed list may do this, and only if it has heard
    from nobody at all in that time. That is the rule that stops a restart
    from producing a second cluster that never meets the first, so this has to
    stay comfortably longer than the time it takes a running seed to answer a
    join with gossip."""

    monitored_peers: Annotated[int, Field(ge=1)] = 5
    """How many other members this node watches, by their place on the ring.

    Every node sorts the member addresses, finds itself, and watches the few
    that follow it. So every member is watched by this many others whatever
    the traffic does, and the heartbeat traffic stays linear in the number of
    nodes. All-to-all monitoring is quadratic, and it is what makes naive
    implementations fall over at a few dozen nodes."""

    heartbeat_interval: timedelta = timedelta(seconds=1)
    """How often this node asks each member it watches whether it is answering.

    Separate from the link heartbeat in
    [RemoteSettings][tapio.settings.RemoteSettings]: that one keeps a
    connection warm and judges the connection, and this one judges a member,
    including one this node would otherwise never send anything to."""

    unreachable_after: timedelta = timedelta(seconds=5)
    """How long a watched member may go without answering before it is called
    unreachable.

    An unreachable member blocks convergence and is not written off: deciding
    to stop waiting for it is downing, and downing is a separate decision with
    strategies of its own. Set this well above `heartbeat_interval`, since a
    fixed window has no opinion about how variable the network is."""

    phi_accrual: bool = False
    """Judge a watched member with a phi-accrual detector, not a fixed window.

    A fixed window (`unreachable_after`) has to be set well above
    `heartbeat_interval` or a slow moment reads as death, and that slack is
    latency a real failure waits out. Phi-accrual learns the spread of a
    member's answer times instead, and suspects it on a scale that means the
    same confidence whether the link is fast and steady or slow and jittery.
    Off by default so behaviour does not change under anyone until they ask for
    it; when on, `unreachable_after` is not consulted and `phi_threshold` and
    `phi_acceptable_pause` take over."""

    phi_threshold: Annotated[float, Field(gt=0.0)] = 8.0
    """How much suspicion is enough to call a member unreachable.

    A log-scale value: 8 is roughly a one-in-a-hundred-million chance the
    member is merely slow rather than gone. Higher is more patient and less
    likely to be wrong, at the cost of noticing a real death later. Only
    consulted when `phi_accrual` is set."""

    phi_acceptable_pause: timedelta = timedelta(seconds=3)
    """How much silence to tolerate on top of a member's learned rhythm.

    Rides out a scheduler or garbage-collection pause without relearning it as
    the normal interval. Set it to cover the longest hiccup that is not a
    failure. Only consulted when `phi_accrual` is set."""

    down_after: timedelta = timedelta(seconds=7)
    """How long an unreachable split must hold still before a strategy acts.

    Downing cannot be taken back, so it waits for the split to settle: the set
    of unreachable members has to stay the same for this long before the leader
    downs anybody. A shorter disturbance, a pause or a link that flaps, is
    ridden out rather than resolved. Only consulted when a downing strategy is
    configured, and measured from when the split was first seen, so the whole
    wait before a member is downed is this on top of `unreachable_after`. Set
    it above the few gossip rounds a real partition takes to be seen the same
    way across a side."""

    join_timeout: timedelta = timedelta(seconds=30)
    """How long `join_seed_nodes` waits to see this node reach `Up`."""

    leave_timeout: timedelta = timedelta(seconds=30)
    """How long `leave` waits to see this node reach `Removed` everywhere."""


class ManagementSettings(BaseSettings):
    """Where a node answers an operator, and what proves the operator is one.

    Passed to [Cluster][tapio.cluster.cluster.Cluster] alongside the cluster,
    the same way [ClusterSettings][tapio.settings.ClusterSettings] is: managing
    a cluster is something an operator does to a system that already exists, not
    part of how the system is addressed. A node with these open runs a small
    HTTP surface that reads its membership and asks it to let a member leave or
    down one. The [tapio-cluster][tapio.cluster.cli] command speaks to it.

    Off unless it is configured, like remoting, since a port that can down a
    member is a serious surface. When it is on, it binds loopback by default,
    for the same reason remoting does: the default is set for someone who has
    not thought about who can reach it yet.
    """

    model_config = SettingsConfigDict(env_prefix="TAPIO_MANAGEMENT_", frozen=True)

    bind_host: str = "127.0.0.1"
    """The interface to answer operators on. Loopback by default: this port can
    down a member, so reaching it from another host is a decision to make on
    purpose rather than a default to inherit."""

    bind_port: int = 25530
    """The port to answer operators on. `0` takes whatever the OS hands out,
    which is what a test binds so no two of them argue over a number."""

    token: SecretStr | None = None
    """The bearer token an operator presents, or `None` to ask for nothing.

    One of the two ways to prove an operator is one, the other being a client
    certificate under `tls`. At least one is required to bind anywhere but
    loopback: a port that can down a member, reachable from any host with
    nothing to prove, fails to start rather than serving strangers. On loopback
    it is optional, since reaching the port at all already means being on the
    machine. Compared in constant time, and presented as
    `Authorization: Bearer <token>`.
    """

    tls: TLSSettings | None = None
    """Certificates for the management port, or `None` for plaintext HTTP.

    The same [TLSSettings][tapio.settings.TLSSettings] remoting uses. With
    `certfile` and `keyfile`, the port speaks HTTPS and an operator's client
    verifies it. Add `cafile` and the port also requires a client certificate
    signed by that authority, which authenticates the operator the way a token
    does: mutual TLS is therefore a second way to satisfy the bind-beyond-
    loopback rule, where a bearer token alone travels in a header an eavesdropper
    on a plaintext link would read.
    """


class TapioSettings(BaseSettings):
    """Tunables for one actor system.

    Every field can be set with an environment variable, upper-cased and
    prefixed: `TAPIO_VALIDATE_ON_TELL=0`, `TAPIO_ASK_TIMEOUT=PT2S`.
    """

    model_config = SettingsConfigDict(env_prefix="TAPIO_", frozen=True)

    validate_on_tell: bool = True
    """Re-validate a message's *contents* on delivery, not just its type.

    The type check is unconditional and cheap. This switch controls the
    expensive half, a full re-validation whose result is discarded, so the
    cost is measurable and tunable in one place rather than at call sites.
    Turning it off changes cost and nothing else: the recipient always receives
    the object the sender passed.
    """

    default_mailbox_capacity: int | None = None
    """User-lane capacity for new mailboxes; `None` means unbounded.

    The system lane is always unbounded, whatever this says: a capacity limit
    that could refuse a stop signal would make shutdown unreliable.
    """

    default_mailbox_overflow: OverflowStrategy = OverflowStrategy.FAIL
    """What a bounded mailbox does when full, unless a spawn overrides it.

    Never consulted while `default_mailbox_capacity` is `None`.
    """

    ask_timeout: timedelta = timedelta(seconds=5)
    """Default deadline for `ActorRef.ask` when the call does not give one."""

    shutdown_timeout: timedelta = timedelta(seconds=10)
    """One deadline for the whole tree, not a per-actor timeout.

    Shutdown races a single clock, so worst-case shutdown time tracks this
    value rather than depth times timeout.
    """

    blocking_pool_size: int = 16
    """Threads available to `ctx.run_blocking`.

    A private, bounded pool rather than the loop's default executor, which is
    shared with every other library in the process and whose size tapio does
    not control, so a bound could not be honoured.
    """

    dead_letter_log_first: int = 10
    """Log this many dead letters in full, then switch to periodic summaries.

    A dead actor in a hot send loop must not drown the log.
    """

    dead_letter_summary_interval: timedelta = timedelta(seconds=60)
    """How often to log a summary once `dead_letter_log_first` is spent."""

    remote: RemoteSettings | None = None
    """How this system is addressed from outside the process.

    `None` means remoting is off, which is the default: a system that has not
    asked to be reachable is not. Refs it hands out still serialize, carrying
    the system name and no host, so a peer reading one can tell which system it
    names and that there is nowhere to dial.
    """

    @property
    def default_mailbox(self) -> MailboxConfig:
        """The mailbox configuration a spawn gets when it asks for nothing."""
        return MailboxConfig(
            capacity=self.default_mailbox_capacity,
            on_overflow=self.default_mailbox_overflow,
        )
