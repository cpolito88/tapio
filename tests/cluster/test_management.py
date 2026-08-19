"""The management endpoint, exercised over a real socket against a live cluster.

Every test here starts a real system with a real management port and talks to it
the way an operator's command does: a socket, an HTTP request, a JSON answer. A
read is checked against what the cluster believes, and a leave or a down is
checked by watching the member it named actually move.
"""

import asyncio
import json

import pytest

from tapio.cluster import MemberStatus
from tapio.cluster.management import verify_management_security
from tapio.errors import InsecureRemoteConfig
from tapio.settings import ManagementSettings, TLSSettings
from tapio.testkit import assert_no_leaked_tasks
from tests.cluster.conftest import Node, cluster_of, seeds_of
from tests.failures import eventually

MANAGED = ManagementSettings(_env_file=None, bind_port=0)  # type: ignore[call-arg]
"""A management endpoint on a loopback port the OS picks, asking for no token."""

GUARDED = ManagementSettings(  # type: ignore[call-arg]
    _env_file=None,  # type: ignore[call-arg]
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
) -> tuple[int, dict[str, object]]:
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


def test_binding_beyond_loopback_without_a_token_is_refused():
    beyond = ManagementSettings(_env_file=None, bind_host="0.0.0.0")  # type: ignore[call-arg]
    with pytest.raises(InsecureRemoteConfig):
        verify_management_security(beyond)


def test_binding_beyond_loopback_with_a_token_is_allowed():
    beyond = ManagementSettings(  # type: ignore[call-arg]
        _env_file=None,  # type: ignore[call-arg]
        bind_host="0.0.0.0",
        token="s3cret",  # type: ignore[arg-type]
    )
    # Does not raise: a token is what the bind beyond loopback was missing.
    verify_management_security(beyond)


def test_mutual_tls_satisfies_the_beyond_loopback_rule():
    # A cafile means the port requires a client certificate, which
    # authenticates the operator the way a token does. The files are not read
    # here, only the policy is checked, so the paths need not exist.
    mutual = ManagementSettings(  # type: ignore[call-arg]
        _env_file=None,  # type: ignore[call-arg]
        bind_host="0.0.0.0",
        tls=TLSSettings(
            _env_file=None, certfile="s.pem", keyfile="s.key", cafile="ca.pem"
        ),  # type: ignore[call-arg]
    )
    verify_management_security(mutual)


def test_server_only_tls_does_not_satisfy_it():
    # No cafile: the port proves who it is but not who the caller is, so a
    # stranger who trusts the certificate can still down a member. That is not
    # enough to bind beyond loopback.
    server_only = ManagementSettings(  # type: ignore[call-arg]
        _env_file=None,  # type: ignore[call-arg]
        bind_host="0.0.0.0",
        tls=TLSSettings(_env_file=None, certfile="s.pem", keyfile="s.key"),  # type: ignore[call-arg]
    )
    with pytest.raises(InsecureRemoteConfig):
        verify_management_security(server_only)


async def test_management_is_off_unless_it_is_configured():
    with assert_no_leaked_tasks():
        async with cluster_of(1) as nodes:
            assert nodes[0].cluster.management_address is None
