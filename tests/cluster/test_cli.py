"""The tapio-cluster command, run against a live management endpoint.

The command speaks blocking HTTP, so each call runs in a worker thread while the
system's loop keeps answering. What is checked is the operator's experience: the
exit status, and that a down asked for on the command line actually moves the
member.
"""

import asyncio

from tapio.cluster import cli
from tapio.settings import ManagementSettings
from tapio.testkit import assert_no_leaked_tasks
from tests.cluster.conftest import Node, cluster_of, seeds_of
from tests.failures import eventually

MANAGED = ManagementSettings(_env_file=None, bind_port=0)  # type: ignore[call-arg]


def _port(node: Node) -> str:
    """The management port a node bound, as the string the CLI takes."""
    address = node.cluster.management_address
    assert address is not None
    return address.rsplit(":", 1)[1]


async def _joined(nodes: tuple[Node, ...]) -> None:
    """Join every node to one cluster and wait for it to be Up."""
    seeds = seeds_of(nodes)
    await asyncio.gather(*(node.cluster.join_seed_nodes(seeds) for node in nodes))


async def _run(*args: str) -> int:
    """Run the command in a thread, so its blocking client does not stall the loop."""
    return await asyncio.to_thread(cli.main, list(args))


async def test_status_prints_the_members(capsys):
    with assert_no_leaked_tasks():
        async with cluster_of(1, management=MANAGED) as nodes:
            await _joined(nodes)

            code = await _run("--port", _port(nodes[0]), "status")

            assert code == 0
            out = capsys.readouterr().out
            assert nodes[0].address in out
            assert "up" in out


async def test_down_from_the_command_line_moves_the_member(capsys):
    with assert_no_leaked_tasks():
        async with cluster_of(2, management=MANAGED) as nodes:
            await _joined(nodes)
            first, second = nodes

            code = await _run("--port", _port(first), "down", second.address)

            assert code == 0
            assert "accepted" in capsys.readouterr().out
            await eventually(
                lambda: (
                    second.address not in [m.address for m in first.cluster.members]
                ),
                within=5.0,
            )


async def test_a_node_that_cannot_be_reached_exits_two(capsys):
    with assert_no_leaked_tasks():
        # Nothing is listening on this port, so the connection is refused.
        code = await _run("--port", "1", "status")

    assert code == 2
    assert "could not reach" in capsys.readouterr().err


async def test_a_refused_request_exits_one(capsys):
    with assert_no_leaked_tasks():
        async with cluster_of(1, management=MANAGED) as nodes:
            await _joined(nodes)

            code = await _run(
                "--port", _port(nodes[0]), "down", "tapio://node9@127.0.0.1:1"
            )

    assert code == 1
    assert "error" in capsys.readouterr().err
