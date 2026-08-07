"""The version of the wire contract, which is not the version of the library.

Two nodes have to agree about the shape of what crosses between them: the
frame layout, the fields of a handshake, and what a link frame means. That
agreement is this number, and it changes when the contract changes.

**It is deliberately not `tapio.__version__`.** The package version moves for
a fixed docstring, a faster mailbox and a new supervisor strategy, none of
which a peer can observe. Pinning a link to it would make every release a flag
day: during any rolling deploy, half the nodes would refuse the other half,
and a patch release would be undeployable without stopping the fleet. So the
handshake checks this number, the hellos carry the package version as a
diagnostic, and 0.1.1 talks to 0.1.0 exactly as long as neither changed the
wire.

Equality is still required rather than negotiated. A wire format that half
matches corrupts a session instead of refusing one, and a peer speaking a
protocol this node has never seen cannot be reasoned about. What changed is
which number gets that treatment.

Raising it is a decision with a deployment cost attached, so it deserves a
sentence in the pull request that does it. Adding an optional field to a
frame does not change the contract, because a reader that does not know the
field ignores it. Removing a field, changing what one means, or adding one
the reader must understand does.
"""

from typing import Final

__all__ = ["PROTOCOL_VERSION"]

PROTOCOL_VERSION: Final = 1
"""What this node speaks, on the wire and in a handshake.

It appears in every frame as `v`, and in both hellos, and a peer that answers
with a different number is refused before anything else is read.
"""
