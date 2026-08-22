"""The tapio-cluster command, run against a live management endpoint.

The command speaks blocking HTTP, so each invocation runs in a worker thread
while the system's loop keeps answering. What is checked is the operator's
experience: the exit status, what was printed, and that a down asked for on the
command line actually moves the member.
"""

import asyncio

from click.testing import Result
from typer.testing import CliRunner

from tapio.cluster.cli import _print_status, app
from tapio.settings import ManagementSettings, TLSSettings
from tapio.testkit import assert_no_leaked_tasks
from tests.cluster.conftest import Node, TlsCerts, cluster_of, seeds_of
from tests.failures import eventually

MANAGED = ManagementSettings(_env_file=None, bind_port=0)  # type: ignore[call-arg]

runner = CliRunner()


def _port(node: Node) -> str:
    """The management port a node bound, as the string the CLI takes."""
    address = node.cluster.management_address
    assert address is not None
    return address.rsplit(":", 1)[1]


async def _joined(nodes: tuple[Node, ...]) -> None:
    """Join every node to one cluster and wait for it to be Up."""
    seeds = seeds_of(nodes)
    await asyncio.gather(*(node.cluster.join_seed_nodes(seeds) for node in nodes))


async def _run(*args: str) -> Result:
    """Run the command in a thread, so its blocking client does not stall the loop."""
    return await asyncio.to_thread(runner.invoke, app, list(args))


async def test_status_prints_the_members():
    with assert_no_leaked_tasks():
        async with cluster_of(1, management=MANAGED) as nodes:
            await _joined(nodes)

            result = await _run("--port", _port(nodes[0]), "status")

            assert result.exit_code == 0
            assert nodes[0].address in result.stdout
            assert "up" in result.stdout


def test_status_table_tolerates_a_member_missing_fields(capsys):
    # A member record without address or status prints a placeholder rather
    # than raising a KeyError and dumping a traceback at the operator. The
    # payload is another node's answer, so a version skew must still leave a
    # readable table.
    _print_status(
        {
            "address": "tapio://a@127.0.0.1:2551",
            "leader": None,
            "converged": False,
            "members": [{"roles": ["web"]}],
        }
    )

    out = capsys.readouterr().out
    assert "?" in out


async def test_down_from_the_command_line_moves_the_member():
    with assert_no_leaked_tasks():
        async with cluster_of(2, management=MANAGED) as nodes:
            await _joined(nodes)
            first, second = nodes

            result = await _run("--port", _port(first), "down", second.address)

            assert result.exit_code == 0
            assert "accepted" in result.stdout
            await eventually(
                lambda: (
                    second.address not in [m.address for m in first.cluster.members]
                ),
                within=5.0,
            )


async def test_a_node_that_cannot_be_reached_exits_two():
    with assert_no_leaked_tasks():
        # Nothing is listening on this port, so the connection is refused.
        result = await _run("--port", "1", "status")

    assert result.exit_code == 2
    assert "could not reach" in result.stderr


async def test_a_refused_request_exits_one():
    with assert_no_leaked_tasks():
        async with cluster_of(1, management=MANAGED) as nodes:
            await _joined(nodes)

            result = await _run(
                "--port", _port(nodes[0]), "down", "tapio://node9@127.0.0.1:1"
            )

    assert result.exit_code == 1
    assert "error" in result.stderr


def _tls_managed(certs: TlsCerts) -> ManagementSettings:
    """A management endpoint that speaks mutual TLS with the given certificates."""
    return ManagementSettings(  # type: ignore[call-arg]
        _env_file=None,  # type: ignore[call-arg]
        bind_port=0,
        tls=TLSSettings(  # type: ignore[call-arg]
            _env_file=None,  # type: ignore[call-arg]
            certfile=certs.server_cert,
            keyfile=certs.server_key,
            cafile=certs.ca,
        ),
    )


async def test_status_over_mutual_tls(mutual_tls_certs: TlsCerts):
    with assert_no_leaked_tasks():
        async with cluster_of(1, management=_tls_managed(mutual_tls_certs)) as nodes:
            await _joined(nodes)

            result = await _run(
                "--port",
                _port(nodes[0]),
                "--cafile",
                mutual_tls_certs.ca,
                "--client-cert",
                mutual_tls_certs.client_cert,
                "--client-key",
                mutual_tls_certs.client_key,
                "status",
            )

            assert result.exit_code == 0
            assert nodes[0].address in result.stdout


async def test_mutual_tls_refuses_a_client_with_no_certificate(
    mutual_tls_certs: TlsCerts,
):
    with assert_no_leaked_tasks():
        async with cluster_of(1, management=_tls_managed(mutual_tls_certs)) as nodes:
            await _joined(nodes)

            # Trusts the server, but presents no client certificate, so the
            # server refuses the handshake and the node cannot be reached.
            result = await _run(
                "--port", _port(nodes[0]), "--cafile", mutual_tls_certs.ca, "status"
            )

    assert result.exit_code == 2
    assert "could not reach" in result.stderr
