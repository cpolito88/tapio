"""Settings that could only fail later are refused where they are written."""

from datetime import timedelta
from typing import Any

import pytest
from pydantic import ValidationError

from tapio.testkit import IsolatedRemoteSettings, IsolatedTapioSettings


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("blocking_pool_size", 0),
        ("default_mailbox_capacity", 0),
        ("dead_letter_log_first", -1),
        ("ask_timeout", timedelta(0)),
        ("shutdown_timeout", timedelta(seconds=-1)),
        ("dead_letter_summary_interval", timedelta(0)),
    ],
)
def test_a_value_no_system_could_run_with_is_refused(field: str, value: Any):
    with pytest.raises(ValidationError, match=field):
        IsolatedTapioSettings(**{field: value})


def test_the_smallest_sensible_values_are_accepted():
    settings = IsolatedTapioSettings(
        blocking_pool_size=1,
        default_mailbox_capacity=1,
        dead_letter_log_first=0,
        ask_timeout=timedelta(milliseconds=1),
    )

    assert settings.blocking_pool_size == 1
    assert settings.default_mailbox_capacity == 1
    assert settings.dead_letter_log_first == 0


def test_an_unbounded_mailbox_is_still_the_default():
    assert IsolatedTapioSettings().default_mailbox_capacity is None


@pytest.mark.parametrize("handshake", [timedelta(seconds=5), timedelta(seconds=10)])
def test_a_dial_allowed_as_long_as_the_silence_window_is_refused(
    handshake: timedelta,
):
    # The silence is counted from the association's creation, so a dial that
    # outlasts the window is judged as silence and quarantines the peer.
    with pytest.raises(ValidationError, match="must be shorter than unreachable_after"):
        IsolatedRemoteSettings(
            handshake_timeout=handshake, unreachable_after=timedelta(seconds=5)
        )


def test_the_default_remote_timings_satisfy_each_other():
    settings = IsolatedRemoteSettings()

    assert settings.handshake_timeout < settings.unreachable_after
