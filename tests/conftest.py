"""Fixtures and fakes shared by every test package.

`FakeContext` is for tests that exercise behaviors with no runtime. A test
that needs a live tree uses the `system` fixture, which terminates whatever
the test leaves running.
"""

import os
import shutil
import subprocess
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, TypeVar

import pytest

from tapio import Message
from tapio.actor import (
    ActorContext,
    ActorPath,
    ActorRef,
    ActorSystem,
    Behavior,
    MailboxConfig,
)
from tapio.logging import ActorLogAdapter, actor_logger
from tapio.settings import TapioSettings
from tapio.testkit import (
    IsolatedTapioSettings,
    assert_no_leaked_tasks,
)
from tapio.validation import MessageType
from tests.messages import Greeted

# The fake stands in for a real context, so its spawning methods stay generic
# in the child protocol the way `ActorContext` declares them. Pinning them to
# `Message` would make the fake accept calls the runtime would reject.
U = TypeVar("U", bound=Message)


class FakeContext(ActorContext[Message]):
    """A context with no cell behind it, for behaviors under test."""

    def __init__(self, path: ActorPath) -> None:
        """Bind the fake to a path."""
        self._path = path

    @property
    def path(self) -> ActorPath:
        return self._path

    @property
    def self_ref(self) -> ActorRef[Message]:
        return ActorRef(self._path)

    @property
    def log(self) -> ActorLogAdapter:
        return actor_logger(self._path)

    def spawn(
        self,
        behavior: Behavior[U],
        name: str,
        mailbox: MailboxConfig | None = None,
    ) -> ActorRef[U]:
        raise NotImplementedError

    def spawn_anonymous(
        self, behavior: Behavior[U], mailbox: MailboxConfig | None = None
    ) -> ActorRef[U]:
        raise NotImplementedError

    def message_adapter(
        self,
        adapt: Callable[[U], Message],
        msg_type: MessageType | None = None,
    ) -> ActorRef[U]:
        raise NotImplementedError

    async def run_blocking(
        self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any
    ) -> Any:
        raise NotImplementedError

    async def resolve(self, uri: str, *, expect: type[Any]) -> ActorRef[Any]:
        raise NotImplementedError

    def dead_letter(
        self,
        message: Message,
        recipient: ActorPath,
        reason: str,
        *,
        detail: str | None = None,
    ) -> None:
        raise NotImplementedError

    def watch(self, ref: ActorRef[Any]) -> None:
        raise NotImplementedError

    def unwatch(self, ref: ActorRef[Any]) -> None:
        raise NotImplementedError


@pytest.fixture(autouse=True, scope="session")
def _no_tapio_environment() -> Iterator[None]:
    """Keep `TAPIO_*` out of the whole suite, not just out of the fixtures.

    The fixtures and the testkit helpers build isolated settings, so they are
    covered on their own. What is not is a test that writes
    `ActorSystem("name")` with no settings: that takes `TapioSettings()`, which
    reads the environment on purpose, because that is how a real system is
    configured. There are dozens of those, and asking each one to remember
    would be a rule nobody can enforce.

    So the suite drops the variables once, here. A developer with
    `TAPIO_DEFAULT_MAILBOX_CAPACITY=2` exported then runs the same tests as
    everyone else, rather than a bounded-mailbox variant of them that fails in
    places that have nothing to do with what they were changing.

    Yields:
        Nothing. The variables are restored when the session ends.
    """
    with pytest.MonkeyPatch.context() as env:
        for name in [name for name in os.environ if name.startswith("TAPIO_")]:
            env.delenv(name)
        yield


@pytest.fixture
def path() -> ActorPath:
    return ActorPath.root("sys").child("user").child("greeter", uid=42)


@pytest.fixture
def ref(path: ActorPath) -> ActorRef[Greeted]:
    return ActorRef(path)


@pytest.fixture
def ctx(path: ActorPath) -> FakeContext:
    return FakeContext(path)


@pytest.fixture
def settings() -> TapioSettings:
    """Settings that do not depend on the developer's environment.

    Isolated rather than merely dotenv-free: `_env_file=None` reads as if it
    keeps `TAPIO_*` out and does not, since the environment arrives through a
    source of its own.
    """
    return IsolatedTapioSettings()


@pytest.fixture
async def system(settings: TapioSettings) -> AsyncIterator[ActorSystem]:
    """A running system, terminated however the test ends, and checked for leaks.

    Every task the test left running fails it at teardown, as AGENTS.md asks of
    anything that starts a system. Threads are not checked here: a test that
    uses `asyncio.to_thread` leaves the loop's default executor running, which
    is not the system's to stop. `assert_no_leaked_threads` checks those that
    are.
    """
    async with running_system(settings) as running:
        yield running


@asynccontextmanager
async def running_system(settings: TapioSettings) -> AsyncIterator[ActorSystem]:
    """Run a system for the length of a block, and fail if it leaked a task.

    The `system` fixture is this. It is a plain context manager so a test can
    show that the check fails when a task is left behind.
    """
    with assert_no_leaked_tasks():
        running = ActorSystem("test", settings)
        try:
            yield running
        finally:
            await running.terminate()


@dataclass(frozen=True, slots=True)
class TlsCerts:
    """Paths to a CA and the certificates it signed."""

    ca: str
    server_cert: str
    server_key: str
    client_cert: str
    client_key: str
    node_cert: str
    """Good for both ends of a remoting link: serverAuth and clientAuth."""
    node_key: str
    elsewhere_cert: str
    """Signed by the same CA for `elsewhere.example`, not for loopback."""
    elsewhere_key: str


@pytest.fixture(scope="session")
def mutual_tls_certs(tmp_path_factory: pytest.TempPathFactory) -> TlsCerts:
    """Generate a CA and a server and client certificate it signed, once.

    Real certificates rather than mocks, so a test drives the actual TLS
    handshake. The server certificate names the loopback address so a client
    checking the hostname is satisfied, and every certificate carries the key
    usage extensions a strict verifier requires: Python 3.13 and later turn on
    strict X.509 checking by default, where a CA with no keyUsage extension is
    refused. Built with openssl, since neither cryptography nor trustme is a
    dependency; a machine without openssl skips the tests that ask for this.
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

    def leaf(name: str, subject: str, *extensions: str) -> None:
        """Issue `{name}.pem` and `{name}.key`, signed by the test CA."""
        added = [arg for extension in extensions for arg in ("-addext", extension)]
        openssl(
            "req",
            "-newkey",
            "rsa:2048",
            "-keyout",
            path(f"{name}.key"),
            "-out",
            path(f"{name}.csr"),
            "-nodes",
            "-subj",
            subject,
            *added,
        )
        openssl(
            "x509",
            "-req",
            "-in",
            path(f"{name}.csr"),
            "-CA",
            path("ca.pem"),
            "-CAkey",
            path("ca.key"),
            "-CAcreateserial",
            "-out",
            path(f"{name}.pem"),
            "-days",
            "1",
            "-copy_extensions",
            "copy",
        )

    leaf(
        "server",
        "/CN=127.0.0.1",
        "subjectAltName=IP:127.0.0.1",
        "keyUsage=critical,digitalSignature,keyEncipherment",
        "extendedKeyUsage=serverAuth",
    )
    leaf(
        "client",
        "/CN=operator",
        "keyUsage=critical,digitalSignature",
        "extendedKeyUsage=clientAuth",
    )
    # A remoting node both listens and dials, so its certificate serves both.
    leaf(
        "node",
        "/CN=127.0.0.1",
        "subjectAltName=IP:127.0.0.1",
        "keyUsage=critical,digitalSignature,keyEncipherment",
        "extendedKeyUsage=serverAuth,clientAuth",
    )
    # The same CA vouches for it, but for another host, so only a dialler that
    # checks the hostname refuses it.
    leaf(
        "elsewhere",
        "/CN=elsewhere.example",
        "subjectAltName=DNS:elsewhere.example",
        "keyUsage=critical,digitalSignature,keyEncipherment",
        "extendedKeyUsage=serverAuth,clientAuth",
    )
    return TlsCerts(
        ca=path("ca.pem"),
        server_cert=path("server.pem"),
        server_key=path("server.key"),
        client_cert=path("client.pem"),
        client_key=path("client.key"),
        node_cert=path("node.pem"),
        node_key=path("node.key"),
        elsewhere_cert=path("elsewhere.pem"),
        elsewhere_key=path("elsewhere.key"),
    )
