<!--
Keep this short. The diff says what changed; this says why, and what a
reviewer should look at hardest. Delete any section that does not apply.
-->

## What and why

<!-- One or two sentences. If it closes an issue: Closes #NNN. -->

## Design notes

<!--
Anything a reviewer would otherwise have to reverse-engineer: a tradeoff you
made, an alternative you rejected and the reason, a semantic that is
observable and therefore load-bearing. If a decision here contradicts the
design doc, say so explicitly. That is a decision, not an oversight.
-->

## Verification

<!--
How you know it works, beyond "CI is green". Name the tests that would fail
if this regressed.
-->

- [ ] `make check` passes locally (lint, mypy strict, tests)
- [ ] New behaviour has a test that fails without the change
- [ ] Public API changes are reflected in the docs

## Risk

<!--
Breaking changes, new failure modes, anything that only shows up under
concurrency or shutdown. "None" is a fine answer, so say it rather than
leaving this blank.
-->

## Not in this PR

<!-- Deliberate follow-ups, so they read as scoping rather than omissions. -->
