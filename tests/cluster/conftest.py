"""A cluster of systems in one process, on loopback ports the OS picks.

Everything here runs the real thing: real sockets, real handshakes, real
gossip. What is scaled down is patience, since a test that waits a second per
gossip round is a test nobody runs.
"""

import shutil
import subprocess
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta

import pytest

from tapio.actor import ActorSystem
from tapio.cluster import Cluster, DownStrategy, MemberStatus
from tapio.settings import (
    ClusterSettings,
    ManagementSettings,
    RemoteSettings,
    TapioSettings,
)
from tapio.testkit.remote import LinkFaults, link_faults

QUICK = ClusterSettings(
    _env_file=None,  # type: ignore[call-arg]
    gossip_interval=timedelta(milliseconds=20),
    join_retry_interval=timedelta(milliseconds=20),
    seed_form_after=timedelta(milliseconds=100),
    heartbeat_interval=timedelta(milliseconds=200),
    unreachable_after=timedelta(seconds=5),
    join_timeout=timedelta(seconds=10),
    leave_timeout=timedelta(seconds=10),
)
"""Gossip fast enough that a whole cluster converges inside a test.

Gossip is what these settings hurry. Probing is left slow on purpose: a test
about membership has no use for it, and a probe every few milliseconds only
adds links to open and close while the test is tearing its nodes down. The
window stays long for the same reason, so a busy loop never reads as a dead
node. A test about reachability asks for `WATCHFUL` instead.
"""

WATCHFUL = QUICK.model_copy(
    update={
        "heartbeat_interval": timedelta(milliseconds=20),
        "unreachable_after": timedelta(milliseconds=300),
    }
)

WATCHFUL_PHI = WATCHFUL.model_copy(
    update={
        "phi_accrual": True,
        "phi_acceptable_pause": timedelta(milliseconds=100),
    }
)
"""Watch with a phi-accrual detector instead of a fixed window.

The same fast probing as `WATCHFUL`, so a partition is seen inside a test, but
the verdict is learned from each member's rhythm rather than a deadline. The
pause covers a handful of missed probes so a busy moment is not read as death.
"""
"""Probe often, and give up quickly, for the tests that are about giving up.

Fifteen probes fit inside the window, which is enough that a busy moment does
not read as a dead node and short enough that a test does not wait seconds to
see one.
"""


def remoting() -> TapioSettings:
    """Settings for a system listening on a loopback port the OS picks."""
    return TapioSettings(
        _env_file=None,  # type: ignore[call-arg]
        remote=RemoteSettings(_env_file=None, bind_port=0),  # type: ignore[call-arg]
    )


@dataclass(frozen=True, slots=True)
class Node:
    """One system and its membership in the cluster under test."""

    system: ActorSystem
    cluster: Cluster
    faults: LinkFaults

    @property
    def address(self) -> str:
        """This node's canonical address, in the form members are named by."""
        return self.cluster.address

    @property
    def status(self) -> MemberStatus | None:
        """This node's own status, as it currently sees it."""
        member = self.cluster.self_member
        return member.status if member is not None else None

    def status_of(self, address: str) -> MemberStatus | None:
        """What this node believes about another member, if it knows one."""
        member = self.cluster.state.member(address)
        return member.status if member is not None else None


def seeds_of(nodes: Sequence[Node]) -> list[str]:
    """The seed list every node in a group is given, in one order."""
    return [node.address for node in nodes]


@dataclass(frozen=True, slots=True)
class TlsCerts:
    """Paths to a CA and the server and client certificates it signed."""

    ca: str
    server_cert: str
    server_key: str
    client_cert: str
    client_key: str


@pytest.fixture(scope="session")
def mutual_tls_certs(tmp_path_factory: pytest.TempPathFactory) -> TlsCerts:
    """Generate a CA and a server and client certificate it signed, once.

    Real certificates rather than mocks, so a test drives the actual TLS
    handshake. The server certificate names the loopback address so a client
    checking the hostname is satisfied, and every certificate carries the key
    usage extensions a strict verifier requires: Python 3.13 turns on strict
    X.509 checking by default, where a CA with no keyUsage extension is refused.
    Built with openssl, since neither cryptography nor trustme is a dependency;
    a machine without openssl skips the tests that ask for this.
    """
    if shutil.which("openssl") is None:
        pytest.skip("openssl is not available to generate certificates")
    root = tmp_path_factory.mktemp("tls")

    def openssl(*args: str) -> None:
        subprocess.run(["openssl", *args], check=True, capture_output=True)

    def path(name: str) -> str:
        return str(root / name)

    openssl(
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-keyout",
        path("ca.key"),
        "-out",
        path("ca.pem"),
        "-days",
        "1",
        "-nodes",
        "-subj",
        "/CN=Test CA",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
        "-addext",
        "keyUsage=critical,keyCertSign,cRLSign",
    )
    openssl(
        "req",
        "-newkey",
        "rsa:2048",
        "-keyout",
        path("server.key"),
        "-out",
        path("server.csr"),
        "-nodes",
        "-subj",
        "/CN=127.0.0.1",
        "-addext",
        "subjectAltName=IP:127.0.0.1",
        "-addext",
        "keyUsage=critical,digitalSignature,keyEncipherment",
        "-addext",
        "extendedKeyUsage=serverAuth",
    )
    openssl(
        "x509",
        "-req",
        "-in",
        path("server.csr"),
        "-CA",
        path("ca.pem"),
        "-CAkey",
        path("ca.key"),
        "-CAcreateserial",
        "-out",
        path("server.pem"),
        "-days",
        "1",
        "-copy_extensions",
        "copy",
    )
    openssl(
        "req",
        "-newkey",
        "rsa:2048",
        "-keyout",
        path("client.key"),
        "-out",
        path("client.csr"),
        "-nodes",
        "-subj",
        "/CN=operator",
        "-addext",
        "keyUsage=critical,digitalSignature",
        "-addext",
        "extendedKeyUsage=clientAuth",
    )
    openssl(
        "x509",
        "-req",
        "-in",
        path("client.csr"),
        "-CA",
        path("ca.pem"),
        "-CAkey",
        path("ca.key"),
        "-CAcreateserial",
        "-out",
        path("client.pem"),
        "-days",
        "1",
        "-copy_extensions",
        "copy",
    )
    return TlsCerts(
        ca=path("ca.pem"),
        server_cert=path("server.pem"),
        server_key=path("server.key"),
        client_cert=path("client.pem"),
        client_key=path("client.key"),
    )


@asynccontextmanager
async def cluster_of(
    count: int,
    *,
    settings: ClusterSettings = QUICK,
    downing: DownStrategy | None = None,
    terminate_on_down: bool = False,
    management: ManagementSettings | None = None,
) -> AsyncIterator[tuple[Node, ...]]:
    """Start `count` systems, each with a cluster daemon and nothing joined yet.

    The systems are named `node1` upwards, and every one is terminated however
    the test ends, so a failure leaves no port bound. Fault injection is
    installed on each before it sends anything, so a test can partition a node
    from the rest without having to arrange it beforehand.

    Args:
        count: How many nodes to start.
        settings: How they gossip.
        downing: The strategy every node resolves a split with, or `None` to
            leave a split blocking convergence, which is the default.
        terminate_on_down: Whether a node that downs itself shuts its own system
            down, as opposed to leaving it running for the test to inspect.
        management: Open a management endpoint on every node, or `None` to leave
            it off. Each node binds port 0, so the endpoints do not collide.

    Yields:
        The nodes, in the order they were started.
    """
    nodes: list[Node] = []
    try:
        for index in range(1, count + 1):
            system = ActorSystem(f"node{index}", remoting())
            faults = link_faults(system)
            nodes.append(
                Node(
                    system=system,
                    cluster=Cluster(
                        system,
                        settings,
                        downing=downing,
                        terminate_on_down=terminate_on_down,
                        management=management,
                    ),
                    faults=faults,
                )
            )
        yield tuple(nodes)
    finally:
        for node in reversed(nodes):
            await node.system.terminate()
