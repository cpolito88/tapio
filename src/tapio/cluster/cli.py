"""The `tapio-cluster` command: read a cluster, and move a member.

An operator points this at the management port of any one node, and asks that
node what it believes or asks it to act. There are three things to ask:

```bash
tapio-cluster status
tapio-cluster leave tapio://orders@10.0.0.2:2551
tapio-cluster down  tapio://orders@10.0.0.3:2551
```

It speaks plain HTTP to
[ManagementSettings][tapio.settings.ManagementSettings], so it needs nothing
from tapio at runtime beyond the standard library, and the same calls are a
`curl` away for anyone who would rather script them. What it reports is one
node's view, which is the truth once the cluster has converged and that node's
best guess until then, the same caveat every gossip-based answer carries.

A leave or a down is answered the moment the node has been asked, not once the
member has gone: the decision travels as gossip, so the command prints that the
node accepted it and the next `status` shows it taking effect.
"""

import argparse
import http.client
import json
import sys
from collections.abc import Mapping, Sequence
from typing import Any

__all__ = ["main"]

_DEFAULT_HOST = "127.0.0.1"
_DEFAULT_PORT = 25530


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command, and return the exit status.

    Args:
        argv: The arguments to parse, or `None` to read `sys.argv`.

    Returns:
        `0` when the call succeeded, `1` when the node answered with an error,
        and `2` when the node could not be reached. Argument errors exit `2`
        through argparse before this returns.
    """
    args = _parser().parse_args(argv)
    try:
        if args.command == "status":
            return _status(args)
        return _transition(args)
    except (OSError, http.client.HTTPException) as error:
        print(f"could not reach {args.host}:{args.port}: {error}", file=sys.stderr)
        return 2


def _parser() -> argparse.ArgumentParser:
    """Build the argument parser, with the three subcommands and their options."""
    parser = argparse.ArgumentParser(
        prog="tapio-cluster",
        description="Read a tapio cluster and move a member through its "
        "management port.",
    )
    parser.add_argument(
        "--host",
        default=_DEFAULT_HOST,
        help=f"the management host to reach (default {_DEFAULT_HOST})",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=_DEFAULT_PORT,
        help=f"the management port to reach (default {_DEFAULT_PORT})",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="the bearer token to present, if the node requires one",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="print the node's raw JSON answer rather than a table",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status", help="show the members, the leader, and reachability")
    leave = sub.add_parser("leave", help="ask a member to leave gracefully")
    leave.add_argument("address", help="the member's canonical address")
    down = sub.add_parser("down", help="down a member an operator judges gone")
    down.add_argument("address", help="the member's canonical address")
    return parser


def _status(args: argparse.Namespace) -> int:
    """Fetch the node's view and print it, as a table or as raw JSON."""
    status, payload = _call(args, "GET", "/status")
    if status != 200:
        return _report_error(payload)
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        _print_status(payload)
    return 0


def _transition(args: argparse.Namespace) -> int:
    """Ask the node to let a member leave or to down one, and report the answer."""
    status, payload = _call(args, "POST", f"/{args.command}", {"address": args.address})
    if status != 202:
        return _report_error(payload)
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"{args.host}:{args.port} accepted: {args.command} {args.address}")
    return 0


def _call(
    args: argparse.Namespace,
    method: str,
    path: str,
    body: Mapping[str, Any] | None = None,
) -> tuple[int, dict[str, Any]]:
    """Make one request to the management port and read its JSON answer.

    Args:
        args: The parsed arguments, for the host, port and token.
        method: The HTTP method.
        path: The path to request.
        body: The JSON body to send, or `None` for a request with no body.

    Returns:
        The status code and the parsed JSON answer. An answer that is not JSON
        becomes an empty mapping, so a caller only has to reason about the code.
    """
    connection = http.client.HTTPConnection(args.host, args.port, timeout=10)
    headers = {"Content-Type": "application/json"}
    if args.token:
        headers["Authorization"] = f"Bearer {args.token}"
    encoded = json.dumps(body).encode("utf-8") if body is not None else None
    try:
        connection.request(method, path, body=encoded, headers=headers)
        response = connection.getresponse()
        raw = response.read()
        status = response.status
    finally:
        connection.close()
    try:
        parsed = json.loads(raw) if raw else {}
    except ValueError:
        parsed = {}
    return status, parsed if isinstance(parsed, dict) else {}


def _report_error(payload: Mapping[str, Any]) -> int:
    """Print the error a node sent back, and return the failure status."""
    detail = payload.get("error", "the node refused the request")
    print(f"error: {detail}", file=sys.stderr)
    return 1


def _print_status(payload: Mapping[str, Any]) -> None:
    """Print a node's view as a short table an operator can read at a glance."""
    leader = payload.get("leader") or "(none)"
    converged = "yes" if payload.get("converged") else "no"
    print(f"node:      {payload.get('address')}")
    print(f"leader:    {leader}")
    print(f"converged: {converged}")
    members = payload.get("members", ())
    if not members:
        print("members:   (none)")
        return
    address_width = max(len(member["address"]) for member in members)
    address_width = max(address_width, len("ADDRESS"))
    print(f"\n{'ADDRESS':<{address_width}}  {'STATUS':<8}  {'REACH':<11}  ROLES")
    for member in members:
        reach = "reachable" if member.get("reachable", True) else "unreachable"
        roles = ", ".join(member.get("roles", ())) or "-"
        print(
            f"{member['address']:<{address_width}}  "
            f"{member['status']:<8}  {reach:<11}  {roles}"
        )


if __name__ == "__main__":  # pragma: no cover - exercised as a console script
    raise SystemExit(main())
