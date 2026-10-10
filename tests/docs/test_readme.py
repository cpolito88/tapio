"""What the README says about settings matches what the settings accept."""

from pathlib import Path

import pytest

from tapio.actor.mailbox import OverflowStrategy
from tapio.testkit import IsolatedTapioSettings

README = (Path(__file__).parents[2] / "README.md").read_text()


@pytest.mark.parametrize("strategy", list(OverflowStrategy))
def test_the_readme_spells_each_overflow_strategy_as_the_setting_reads_it(
    strategy: OverflowStrategy,
):
    # The README is where a reader copies an environment variable's value from.
    assert f"`{strategy.value}`" in README
    settings = IsolatedTapioSettings.model_validate(
        {"default_mailbox_overflow": strategy.value}
    )
    assert settings.default_mailbox_overflow is strategy
