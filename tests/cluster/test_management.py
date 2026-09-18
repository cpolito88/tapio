"""The management endpoint, exercised over a real socket against a live cluster.

Every test here starts a real system with a real management port and talks to it
the way an operator's command does: a socket, an HTTP request, a JSON answer. A
read is checked against what the cluster believes, and a leave or a down is
checked by watching the member it named actually move.
"""

import asyncio
import contextlib
import json
import socket
from typing import Any

import pytest

from tapio.actor import ActorSystem
from tapio.cluster import Cluster, MemberStatus
from tapio.cluster.management import _MAX_CONNECTIONS, verify_management_security
from tapio.errors import ActorNameError, InsecureRemoteConfig
from tapio.testkit import (
    IsolatedManagementSettings,
    IsolatedTLSSettings,
    assert_no_leaked_tasks,
)
from tests.cluster.conftest import Node, cluster_of, remoting, seeds_of
from tests.failures import eventually

MANAGED = IsolatedManagementSettings(bind_port=0)
"""A management endpoint on a loopback port the OS picks, asking for no token."""

GUARDED = IsolatedManagementSettings(
    bind_port=0,
    token="s3cret",  # type: ignore[arg-type]
)
"""The same endpoint, but one that requires a bearer token."""


def _port(node: Node) -> int:
    """The management port a node bound, read from the address it reports."""
    address = node.cluster.management_address
    assert address is not None
    return int(address.rsplit(":", 1)[1])


async def _request(
    port: int,
    method: str,
    path: str,
    *,
    body: dict[str, object] | None = None,
    token: str | None = None,
) -> tuple[int, dict[str, Any]]:
    """Make one HTTP request to a management port and read its JSON answer.

    Done with a raw asyncio connection rather than a blocking client so the
    request runs on the loop the endpoint answers on, without a thread.
    """
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    lines = [f"{method} {path} HTTP/1.1", "Host: 127.0.0.1"]
    if token is not None:
        lines.append(f"Authorization: Bearer {token}")
    payload = b""
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        lines.append("Content-Type: application/json")
        lines.append(f"Content-Length: {len(payload)}")
    lines.append("Connection: close")
    writer.write(("\r\n".join(lines) + "\r\n\r\n").encode("latin-1") + payload)
    await writer.drain()
    raw = await reader.read()
    writer.close()
    await writer.wait_closed()
    head, _, tail = raw.partition(b"\r\n\r\n")
    code = int(head.split(b"\r\n")[0].split(b" ")[1])
    parsed = json.loads(tail) if tail else {}
    return code, parsed


async def _joined(nodes: tuple[Node, ...]) -> None:
    """Join every node to one cluster and wait for it to be Up."""
    seeds = seeds_of(nodes)
    await asyncio.gather(*(node.cluster.join_seed_nodes(seeds) for node in nodes))


async def test_status_reports_the_view_the_node_holds():
    with assert_no_leaked_tasks():
        async with cluster_of(1, management=MANAGED) as nodes:
            await _joined(nodes)
            node = nodes[0]

            code, payload = await _request(_port(node), "GET", "/status")

            assert code == 200
            assert payload["address"] == node.address
            assert payload["leader"] == node.address
            assert payload["converged"] is True
            members = payload["members"]
            assert [m["address"] for m in members] == [node.address]
            assert members[0]["status"] == MemberStatus.UP.value
            assert members[0]["reachable"] is True


async def test_down_moves_the_member_it_names():
    with assert_no_leaked_tasks():
        async with cluster_of(2, management=MANAGED) as nodes:
            await _joined(nodes)
            first, second = nodes

            code, payload = await _request(
                _port(first), "POST", "/down", body={"address": second.address}
            )

            assert code == 202
            assert payload == {"accepted": "down", "address": second.address}
            # The operator asked one node; the decision reaches the other as
            # gossip, so the downed member leaves the live membership of both.
            # Its exact status is Down or the Removed the leader promotes it to,
            # so the test watches it leave rather than pinning one of the two.
            await eventually(
                lambda: (
                    second.address not in [m.address for m in first.cluster.members]
                ),
                within=5.0,
            )
            await eventually(
                lambda: (
                    second.address not in [m.address for m in second.cluster.members]
                ),
                within=5.0,
            )


async def test_leave_walks_the_member_out():
    with assert_no_leaked_tasks():
        async with cluster_of(2, management=MANAGED) as nodes:
            await _joined(nodes)
            first, second = nodes

            code, _ = await _request(
                _port(first), "POST", "/leave", body={"address": second.address}
            )

            assert code == 202
            await eventually(
                lambda: (
                    second.address not in [m.address for m in first.cluster.members]
                ),
                within=5.0,
            )


async def test_a_token_is_required_when_one_is_configured():
    with assert_no_leaked_tasks():
        async with cluster_of(1, management=GUARDED) as nodes:
            await _joined(nodes)
            port = _port(nodes[0])

            missing, _ = await _request(port, "GET", "/status")
            wrong, _ = await _request(port, "GET", "/status", token="nope")
            right, payload = await _request(port, "GET", "/status", token="s3cret")

            assert missing == 401
            assert wrong == 401
            assert right == 200
            assert payload["address"] == nodes[0].address


async def test_a_non_ascii_bearer_token_is_answered_not_crashed():
    # Headers are decoded as latin-1, so a bearer value may hold non-ASCII
    # characters. hmac.compare_digest on str raises TypeError for those, which
    # escaped the handler before the fix: the connection was dropped instead of
    # answered, so this request never gets a 401 back.
    with assert_no_leaked_tasks():
        async with cluster_of(1, management=GUARDED) as nodes:
            await _joined(nodes)
            port = _port(nodes[0])

            # "\xc3\xa9" is the UTF-8 encoding of "e-acute" seen as two latin-1
            # characters, which is what a real client sending UTF-8 puts on the
            # wire.
            code, _ = await _request(port, "GET", "/status", token="\xc3\xa9")

            assert code == 401


async def test_malformed_requests_answer_with_the_right_code():
    with assert_no_leaked_tasks():
        async with cluster_of(1, management=MANAGED) as nodes:
            await _joined(nodes)
            port = _port(nodes[0])
            member = nodes[0].address

            unknown_path, _ = await _request(port, "GET", "/nope")
            wrong_method, _ = await _request(port, "POST", "/status")
            no_address, _ = await _request(port, "POST", "/down", body={})
            bad_address, _ = await _request(
                port, "POST", "/leave", body={"address": "tapio://ghost"}
            )
            not_a_member, _ = await _request(
                port,
                "POST",
                "/down",
                body={"address": "tapio://node9@127.0.0.1:1"},
            )
            downing_self, _ = await _request(
                port, "POST", "/down", body={"address": member}
            )

            assert unknown_path == 404
            assert wrong_method == 405
            assert no_address == 400
            assert bad_address == 400
            assert not_a_member == 404
            # A member the node does know is accepted, even when it is itself.
            assert downing_self == 202


async def test_a_stalled_request_is_timed_out_not_parked(monkeypatch):
    # A short deadline so the test does not wait the real one out. A client that
    # opens a connection and never ends its headers is answered 408 and closed,
    # rather than holding a connection task for as long as it stays open;
    # assert_no_leaked_tasks would fail if the task were left parked.
    monkeypatch.setattr("tapio.cluster.management._REQUEST_TIMEOUT", 0.2)
    with assert_no_leaked_tasks():
        async with cluster_of(1, management=MANAGED) as nodes:
            await _joined(nodes)
            port = _port(nodes[0])

            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"GET /status HTTP/1.1\r\n")  # no blank line ever follows
            await writer.drain()
            raw = await asyncio.wait_for(reader.read(), timeout=5.0)
            writer.close()
            await writer.wait_closed()

    assert b"408" in raw


async def _answers(port: int, code: int, *, within: float = 5.0) -> None:
    """Wait until a `GET /status` on this port answers with a given code.

    This polls instead of asking once. The endpoint counts a connection only
    after it has accepted it, and releases one only after the handler has
    finished. Both happen on the endpoint's loop, a turn or two after the client
    opens or closes its side.
    """
    try:
        async with asyncio.timeout(within):
            while True:
                answered, _ = await _request(port, "GET", "/status")
                if answered == code:
                    return
    except TimeoutError:
        msg = f"the port never answered {code} within {within}s"
        raise AssertionError(msg) from None


async def test_the_port_refuses_connections_past_its_cap():
    # The cap exists because this port shares a loop with the cluster daemon.
    # Opening connections and sending nothing is the cheap way to starve it.
    with assert_no_leaked_tasks():
        async with cluster_of(1, management=MANAGED) as nodes:
            await _joined(nodes)
            port = _port(nodes[0])

            held = [
                await asyncio.open_connection("127.0.0.1", port)
                for _ in range(_MAX_CONNECTIONS)
            ]
            try:
                await _answers(port, 503)

                refused, payload = await _request(port, "GET", "/status")

                assert refused == 503
                assert payload == {"error": "too many connections"}

                # Closing one frees a slot. The cap bounds how many
                # connections are open at once. It does not wedge the port.
                _, writer = held.pop()
                writer.close()
                await writer.wait_closed()

                await _answers(port, 200)
            finally:
                for _, writer in held:
                    writer.close()
                    with contextlib.suppress(ConnectionResetError, BrokenPipeError):
                        await writer.wait_closed()


async def test_headers_that_never_end_answer_413():
    with assert_no_leaked_tasks():
        async with cluster_of(1, management=MANAGED) as nodes:
            await _joined(nodes)
            port = _port(nodes[0])

            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            # A header far past the read buffer, with no blank line to end it.
            writer.write(b"GET /status HTTP/1.1\r\nX-Big: " + b"a" * (32 * 1024))
            with contextlib.suppress(ConnectionResetError, BrokenPipeError):
                await writer.drain()
            raw = await asyncio.wait_for(reader.read(), timeout=5.0)
            writer.close()
            with contextlib.suppress(ConnectionResetError, BrokenPipeError):
                await writer.wait_closed()

    assert b"413" in raw


def test_binding_beyond_loopback_without_a_token_is_refused():
    beyond = IsolatedManagementSettings(bind_host="0.0.0.0")
    with pytest.raises(InsecureRemoteConfig):
        verify_management_security(beyond)


def test_binding_beyond_loopback_with_a_token_is_allowed():
    beyond = IsolatedManagementSettings(
        bind_host="0.0.0.0",
        token="s3cret",  # type: ignore[arg-type]
    )
    # Does not raise: a token is what the bind beyond loopback was missing.
    verify_management_security(beyond)


def test_mutual_tls_satisfies_the_beyond_loopback_rule():
    # A cafile means the port requires a client certificate, which
    # authenticates the operator the way a token does. The files are not read
    # here, only the policy is checked, so the paths need not exist.
    mutual = IsolatedManagementSettings(
        bind_host="0.0.0.0",
        tls=IsolatedTLSSettings(certfile="s.pem", keyfile="s.key", cafile="ca.pem"),
    )
    verify_management_security(mutual)


def test_server_only_tls_does_not_satisfy_it():
    # No cafile: the port proves who it is but not who the caller is, so a
    # stranger who trusts the certificate can still down a member. That is not
    # enough to bind beyond loopback.
    server_only = IsolatedManagementSettings(
        bind_host="0.0.0.0",
        tls=IsolatedTLSSettings(certfile="s.pem", keyfile="s.key"),
    )
    with pytest.raises(InsecureRemoteConfig):
        verify_management_security(server_only)


async def test_management_is_off_unless_it_is_configured():
    with assert_no_leaked_tasks():
        async with cluster_of(1) as nodes:
            assert nodes[0].cluster.management_address is None


def _a_free_port() -> int:
    """Find a port nothing is listening on, and let go of it."""
    with contextlib.closing(socket.create_server(("127.0.0.1", 0))) as probe:
        port: int = probe.getsockname()[1]
    return port


async def test_a_cluster_that_fails_to_start_releases_its_management_port():
    # A fixed port, against the convention that a test binds port 0, because the
    # failure only shows up on a fixed one. A leaked listener on port 0 costs a
    # descriptor and nothing else; on the default 25530 it makes the next
    # attempt fail to bind, so an operator reads "address already in use"
    # instead of the spawn failure that actually happened.
    port = _a_free_port()

    with assert_no_leaked_tasks():
        async with ActorSystem("port-released", remoting()) as system:
            Cluster(system)  # takes the /system/cluster name

            # The port is bound before the daemon is spawned, and this spawn is
            # the thing that fails, so the socket is open when the constructor
            # raises and nothing else refers to it.
            with pytest.raises(ActorNameError):
                Cluster(
                    system,
                    management=IsolatedManagementSettings(
                        bind_port=port,
                    ),
                )

            # Free again, which is the whole claim. Two things can catch a
            # leak here, and they catch different ones. This binding raises
            # EADDRINUSE while something still holds the listener open. The
            # suite's `filterwarnings = ["error"]` catches the other case: a
            # listener nobody refers to any more is closed by its finalizer,
            # and the ResourceWarning that comes with it fails the test. Before
            # this fix the second one fires, naming the port bound above.
            with contextlib.closing(socket.create_server(("127.0.0.1", port))):
                pass
