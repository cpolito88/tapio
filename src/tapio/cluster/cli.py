"""The `tapio-cluster` command: read a cluster, and move a member.

An operator points this at the management port of any one node, and asks that
node what it believes or asks it to act. There are three things to ask:

```bash
tapio-cluster status
tapio-cluster leave tapio://orders@10.0.0.2:2551
tapio-cluster down  tapio://orders@10.0.0.3:2551
```

It speaks plain HTTP to
[ManagementSettings][tapio.settings.ManagementSettings], so the only thing it
needs at runtime is a `typer` for its own argument parsing and the standard
library for the requests, and the same calls are a `curl` away for anyone who
would rather script them. What it reports is one node's view, which is the truth
once the cluster has converged and that node's best guess until then, the same
caveat every gossip-based answer carries.

A leave or a down is answered the moment the node has been asked, not once the
member has gone: the decision travels as gossip, so the command prints that the
node accepted it and the next `status` shows it taking effect.
"""

import http.client
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Annotated, Any

import typer

__all__ = ["app"]

_DEFAULT_HOST = "127.0.0.1"
_DEFAULT_PORT = 25530

app = typer.Typer(
    name="tapio-cluster",
    help="Read a tapio cluster and move a member through its management port.",
    add_completion=False,
    no_args_is_help=True,
)


@dataclass(frozen=True, slots=True)
class _Target:
    """Which node to reach, and how to talk to it.

    Built once from the global options and carried on the Typer context, so
    each command reads the same connection details rather than repeating them.
    """

    host: str
    port: int
    token: str | None
    as_json: bool


@app.callback()
def _configure(
    ctx: typer.Context,
    host: Annotated[
        str, typer.Option(help="The management host to reach.")
    ] = _DEFAULT_HOST,
    port: Annotated[
        int, typer.Option(help="The management port to reach.")
    ] = _DEFAULT_PORT,
    token: Annotated[
        str | None,
        typer.Option(help="The bearer token to present, if the node requires one."),
    ] = None,
    as_json: Annotated[
        bool,
        typer.Option(
            "--json", help="Print the node's raw JSON answer rather than a table."
        ),
    ] = False,
) -> None:
    """Read a tapio cluster and move a member through its management port."""
    ctx.obj = _Target(host=host, port=port, token=token, as_json=as_json)


@app.command()
def status(ctx: typer.Context) -> None:
    """Show the members, the leader, and reachability."""
    target: _Target = ctx.obj
    code, payload = _request(target, "GET", "/status")
    if code != 200:
        _fail(payload)
    if target.as_json:
        typer.echo(json.dumps(payload, indent=2))
    else:
        _print_status(payload)


@app.command()
def leave(
    ctx: typer.Context,
    address: Annotated[str, typer.Argument(help="The member's canonical address.")],
) -> None:
    """Ask a member to leave the cluster gracefully."""
    _move(ctx.obj, "leave", address)


@app.command()
def down(
    ctx: typer.Context,
    address: Annotated[str, typer.Argument(help="The member's canonical address.")],
) -> None:
    """Down a member an operator judges gone."""
    _move(ctx.obj, "down", address)


def _move(target: _Target, action: str, address: str) -> None:
    """Ask the node to let a member leave or to down one, and report the answer."""
    code, payload = _request(target, "POST", f"/{action}", {"address": address})
    if code != 202:
        _fail(payload)
    if target.as_json:
        typer.echo(json.dumps(payload, indent=2))
    else:
        typer.echo(f"{target.host}:{target.port} accepted: {action} {address}")


def _request(
    target: _Target,
    method: str,
    path: str,
    body: Mapping[str, Any] | None = None,
) -> tuple[int, dict[str, Any]]:
    """Make one request to the management port, or exit if it cannot be reached.

    A node that cannot be reached at all is a different failure from one that
    answered with an error, so it exits `2` here rather than returning a status
    a caller would have to tell apart from a real answer.

    Args:
        target: Which node to reach, and how.
        method: The HTTP method.
        path: The path to request.
        body: The JSON body to send, or `None` for a request with no body.

    Returns:
        The status code and the parsed JSON answer. An answer that is not JSON
        becomes an empty mapping, so a caller only has to reason about the code.

    Raises:
        typer.Exit: With code `2`, if the node could not be reached.
    """
    connection = http.client.HTTPConnection(target.host, target.port, timeout=10)
    headers = {"Content-Type": "application/json"}
    if target.token:
        headers["Authorization"] = f"Bearer {target.token}"
    encoded = json.dumps(body).encode("utf-8") if body is not None else None
    try:
        connection.request(method, path, body=encoded, headers=headers)
        response = connection.getresponse()
        raw = response.read()
        code = response.status
    except (OSError, http.client.HTTPException) as error:
        typer.echo(f"could not reach {target.host}:{target.port}: {error}", err=True)
        raise typer.Exit(2) from error
    finally:
        connection.close()
    try:
        parsed = json.loads(raw) if raw else {}
    except ValueError:
        parsed = {}
    return code, parsed if isinstance(parsed, dict) else {}


def _fail(payload: Mapping[str, Any]) -> None:
    """Print the error a node sent back, and exit non-zero.

    Args:
        payload: The node's JSON answer.

    Raises:
        typer.Exit: With code `1`, always: this is called only for an answer
            that was not the success the command wanted.
    """
    detail = payload.get("error", "the node refused the request")
    typer.echo(f"error: {detail}", err=True)
    raise typer.Exit(1)


def _print_status(payload: Mapping[str, Any]) -> None:
    """Print a node's view as a short table an operator can read at a glance."""
    leader = payload.get("leader") or "(none)"
    converged = "yes" if payload.get("converged") else "no"
    typer.echo(f"node:      {payload.get('address')}")
    typer.echo(f"leader:    {leader}")
    typer.echo(f"converged: {converged}")
    members = payload.get("members", ())
    if not members:
        typer.echo("members:   (none)")
        return
    width = max(max(len(member["address"]) for member in members), len("ADDRESS"))
    typer.echo(f"\n{'ADDRESS':<{width}}  {'STATUS':<8}  {'REACH':<11}  ROLES")
    for member in members:
        reach = "reachable" if member.get("reachable", True) else "unreachable"
        roles = ", ".join(member.get("roles", ())) or "-"
        typer.echo(
            f"{member['address']:<{width}}  {member['status']:<8}  {reach:<11}  {roles}"
        )


if __name__ == "__main__":  # pragma: no cover - exercised as a console script
    app()
