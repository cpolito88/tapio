"""Settings a test builds from its own arguments, and from nothing else.

A test asserts against the settings it asked for. If the environment can reach
them, a developer with `TAPIO_VALIDATE_ON_TELL=0` exported runs a different
suite from everyone else and nothing says so: the checks that would have failed
are the ones that stopped running.

Keeping the environment out is harder than it looks, which is why this module
exists rather than a keyword at each call site. `TapioSettings(_env_file=None)`
reads as if it does the job and does not: `_env_file` disables the **dotenv**
source, while environment variables arrive through a separate source that is
still active. Three places in this project said "the environment is switched
off" in a docstring and passed that argument.

The reliable way to say it is to name the sources. `settings_customise_sources`
returns the init source alone, so nothing but the arguments a caller passed can
contribute, and there is no second mechanism to remember.

These are for tests. A running system reads `TAPIO_*` on purpose, which is the
documented way to configure one.
"""

from pydantic_settings import BaseSettings, PydanticBaseSettingsSource

from tapio.settings import (
    ClusterSettings,
    ManagementSettings,
    RemoteSettings,
    TapioSettings,
    TLSSettings,
)

__all__ = [
    "IsolatedClusterSettings",
    "IsolatedManagementSettings",
    "IsolatedRemoteSettings",
    "IsolatedTLSSettings",
    "IsolatedTapioSettings",
]


class _InitOnly(BaseSettings):
    """Read settings from the arguments passed, and from no other source."""

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Return the init source alone.

        Dropping `env_settings` is the point. Dropping the dotenv and secrets
        sources with it costs nothing and means there is one rule here rather
        than a list of exceptions.

        Args:
            settings_cls: The settings class being built.
            init_settings: The values the caller passed.
            env_settings: Environment variables, deliberately dropped.
            dotenv_settings: A dotenv file, deliberately dropped.
            file_secret_settings: A secrets directory, deliberately dropped.

        Returns:
            The init source, alone.
        """
        return (init_settings,)


class IsolatedTapioSettings(_InitOnly, TapioSettings):
    """[TapioSettings][tapio.settings.TapioSettings] with the environment out.

    What the `actor_system` fixture builds. Override the `tapio_settings`
    fixture with one of these to change what a test runs against, and a
    `TAPIO_*` variable in the shell still cannot reach it.
    """


class IsolatedRemoteSettings(_InitOnly, RemoteSettings):
    """[RemoteSettings][tapio.settings.RemoteSettings] with the environment out.

    Built separately from the system's settings, so it needs its own isolation:
    `TAPIO_REMOTE_BIND_HOST` reaches a nested model that was constructed by a
    caller, exactly as `TAPIO_ASK_TIMEOUT` reaches the outer one.
    """


class IsolatedClusterSettings(_InitOnly, ClusterSettings):
    """[ClusterSettings][tapio.settings.ClusterSettings] with the environment out.

    A cluster is handed its settings rather than reading them at construction,
    so these are usually built once as a module constant and copied per test.
    `model_copy` keeps the isolation, since it keeps the class.
    """


class IsolatedManagementSettings(_InitOnly, ManagementSettings):
    """[ManagementSettings][tapio.settings.ManagementSettings] with the environment out.

    Built separately from the system's settings, like the cluster's, so it
    needs its own isolation. It matters more here than elsewhere: these
    settings decide what a management port binds to and what proves an
    operator is one, so a test that asserts "this configuration is refused"
    has to be sure the configuration under test is the one it wrote.
    """


class IsolatedTLSSettings(_InitOnly, TLSSettings):
    """[TLSSettings][tapio.settings.TLSSettings] with the environment out.

    A nested model a caller constructs, so `TAPIO_REMOTE_TLS_CAFILE` reaches
    it even when the settings around it were passed in. A test that builds
    one is usually checking which certificates are demanded, and an exported
    variable would change the answer.
    """
