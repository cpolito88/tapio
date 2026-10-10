"""The package imports and the toolchain is wired up."""

import asyncio
import importlib
import subprocess
import sys
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as installed_version

import pytest

import tapio
from tapio import version


def test_the_version_comes_from_the_installed_distribution():
    # The distribution is `tapio-py` and the import package is `tapio`.
    # importlib.metadata takes the first, and asking it for the second raised
    # on every install, so every release reported the fallback and every
    # handshake carried it to its peer. Asserting truthiness did not catch it,
    # because the fallback is truthy.
    assert tapio.__version__ != version._UNKNOWN
    assert tapio.__version__ == installed_version(version._DISTRIBUTION)


def test_an_uninstalled_source_tree_reports_the_unknown_version(monkeypatch):
    # The fallback is reachable: a checkout that was never built or installed
    # has no metadata to read. It is tested rather than excluded from coverage,
    # since excluding it is what hid the bug above.
    def missing(name: str) -> str:
        raise PackageNotFoundError(name)

    monkeypatch.setattr(version, "_installed_version", missing)

    assert version._read_version() == version._UNKNOWN


def test_examples_package_importable():
    import tapio_examples

    assert tapio_examples.__all__ == []


def test_minimum_python_is_311():
    # 3.11 is a hard floor: asyncio.timeout(), typing.Self, add_note.
    assert sys.version_info >= (3, 11)
    assert hasattr(asyncio, "timeout")


async def test_asyncio_mode_auto_is_configured():
    # Guards the pytest-asyncio wiring the whole runtime suite will rely on.
    await asyncio.sleep(0)


def test_importing_the_library_does_not_pull_in_typer():
    # `typer` is the `cli` extra, so nothing reachable from `import tapio` may
    # depend on it. A stray import would make the extra mandatory in practice
    # while still being optional on paper, and the 4.3 MB it brings would be
    # back in every downstream install.
    #
    # Checked in a subprocess rather than against this process's `sys.modules`:
    # `tests/cluster/test_cli.py` imports the CLI, so in-process the answer
    # would depend on which tests ran first.
    probe = "import sys, tapio, tapio.cluster; raise SystemExit('typer' in sys.modules)"
    assert subprocess.run([sys.executable, "-c", probe], check=False).returncode == 0


ACTOR_SURFACE = {
    "AbstractBehavior",
    "ActorContext",
    "ActorPath",
    "ActorRef",
    "ActorSystem",
    "AdapterRef",
    "Backoff",
    "Behavior",
    "Behaviors",
    "ChildFailed",
    "DeadLetter",
    "DeadLetterReason",
    "Decision",
    "MailboxConfig",
    "OverflowStrategy",
    "PostStop",
    "PreRestart",
    "PromiseRef",
    "RoundRobin",
    "Routers",
    "RoutingStrategy",
    "Signal",
    "SignalHandler",
    "StashBuffer",
    "Subscription",
    "SupervisorStrategy",
    "Terminated",
    "TimerScheduler",
    "ask",
}
"""What `tapio.actor` promises. Every name here is a compatibility promise."""

CLUSTER_SURFACE = {
    "Cluster",
    "ClusterDowned",
    "ClusterError",
    "ClusterEvent",
    "ClusterSettings",
    "ClusterSingleton",
    "DownAll",
    "DownStrategy",
    "KeepMajority",
    "KeepOldest",
    "LeaderChanged",
    "Lease",
    "LeaseMajority",
    "LocalLease",
    "ManagementSettings",
    "Member",
    "MemberLeaving",
    "MemberRemoved",
    "MemberStatus",
    "MemberUp",
    "ReachableMember",
    "SelfDown",
    "StaticQuorum",
    "UnreachableMember",
    "group_router",
}
"""What `tapio.cluster` promises. The wire protocol and merge state are not."""


def test_the_actor_package_exports_its_surface_and_nothing_else():
    import tapio.actor

    assert set(tapio.actor.__all__) == ACTOR_SURFACE


def test_the_cluster_package_exports_its_surface_and_nothing_else():
    import tapio.cluster

    assert set(tapio.cluster.__all__) == CLUSTER_SURFACE


@pytest.mark.parametrize(
    ("package", "name"),
    [
        ("tapio.actor", "ActorCell"),
        ("tapio.actor", "LocalActorRef"),
        ("tapio.actor", "Mailbox"),
        ("tapio.actor", "DeadLetterOffice"),
        ("tapio.cluster", "Join"),
        ("tapio.cluster", "WireMessage"),
        ("tapio.cluster", "Gossip"),
    ],
)
def test_runtime_internals_are_not_importable_from_the_package(package: str, name: str):
    # Out of the namespace as well as `__all__`, so nobody comes to rely on
    # one by accident. Each is still importable from its own module.
    assert not hasattr(importlib.import_module(package), name)


@pytest.mark.parametrize(
    "name", ["ClusterError", "ClusterSettings", "ManagementSettings"]
)
def test_what_every_cluster_needs_is_exported_at_the_top(name: str):
    import tapio.cluster

    assert name in tapio.__all__
    assert getattr(tapio, name) is getattr(tapio.cluster, name)


def test_every_exported_name_exists():
    import tapio.actor
    import tapio.cluster
    import tapio.testkit

    for package in (tapio, tapio.actor, tapio.cluster, tapio.testkit):
        missing = [name for name in package.__all__ if not hasattr(package, name)]
        assert missing == [], package.__name__
