"""Stage the assets the browser playground needs into `docs/playground/dist`.

The playground runs the library in the browser under Pyodide. Two things have
to be reachable from the published site for that to work: the wheel, so the
browser can install `tapio`, and the source of every example the playground
offers, so the editor can seed itself from the same files the test suite runs.

The wheel is built by `make playground` before this script runs, so this
script only stages what is already on disk. It copies the browser-runnable
examples, then writes a manifest naming the wheel and listing those examples
with a human label each. The page reads the manifest and needs nothing else
hard-coded about the build.

Examples that open a socket or use a thread pool cannot run under Pyodide, so
they are left out on purpose: the browser has no sockets, and the blocking
pool needs threads the runtime does not offer. Those examples stay in the
documentation, where they belong, and the playground says so.
"""

import json
import shutil
from pathlib import Path

# The examples the playground can actually run, mapped to the label the picker
# shows. Every one here is local: it opens no socket and offloads to no thread,
# so it runs on the browser event loop unchanged. The order is the order the
# picker lists them, easiest first.
RUNNABLE: dict[str, str] = {
    "hello_world": "Hello, world",
    "ping_pong": "Two actors talking",
    "counter": "An actor with state",
    "ask_timeout": "Ask, with a deadline",
    "state_machine": "A state machine",
    "stash_on_startup": "Stashing until ready",
    "worker_pool": "A pool of workers",
    "rate_limiter": "A rate limiter",
    "dead_letters": "Dead letters",
    "death_watch": "Watching for a stop",
    "supervision_backoff": "Restart with backoff",
    "escalation": "Escalating a failure",
    "chat_sessions": "One actor per user",
    "order_saga": "A saga across actors",
    "graceful_shutdown": "A graceful shutdown",
}

# Where things live, all relative to the repository root this script runs from.
REPO = Path(__file__).resolve().parent.parent
EXAMPLES = REPO / "examples" / "tapio_examples"
DIST = REPO / "docs" / "playground" / "dist"


def stage() -> None:
    """Copy the runnable examples and write the manifest the page reads.

    Raises:
        FileNotFoundError: If the wheel is missing, meaning `make playground`
            did not build it first, or an example named above does not exist.
    """
    wheels = sorted(DIST.glob("tapio_py-*.whl"))
    if not wheels:
        raise FileNotFoundError(
            f"no wheel in {DIST}; run `make playground`, which builds it first"
        )
    # The build leaves exactly one wheel because the Makefile clears the
    # directory first, so the last by name is the one just built.
    wheel = wheels[-1]

    example_dir = DIST / "examples"
    example_dir.mkdir(parents=True, exist_ok=True)
    for stale in example_dir.glob("*.py"):
        stale.unlink()

    examples: list[dict[str, str]] = []
    for name, label in RUNNABLE.items():
        source = EXAMPLES / f"{name}.py"
        if not source.exists():
            raise FileNotFoundError(f"example {name} is listed but {source} is missing")
        shutil.copyfile(source, example_dir / f"{name}.py")
        examples.append({"name": name, "label": label, "file": f"examples/{name}.py"})

    manifest = {"wheel": wheel.name, "examples": examples}
    (DIST / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"playground: staged {wheel.name} and {len(examples)} examples into {DIST}")


if __name__ == "__main__":
    stage()
