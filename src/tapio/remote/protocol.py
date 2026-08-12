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

**One contract change has happened without raising this**, deliberately, and it
is recorded here because the rule above says it should have. The handshake
stopped volunteering a system's identity to anything that could open a
connection: the name, address, incarnation uid and release moved out of the
server-hello and into the welcome. By the rule that is a contract change, since
a reader has to know where those fields now are. The number stayed at 1 because
nothing was deployed to be incompatible with, so the bump would have announced
a break to nobody.

The cost is that two nodes on either side of that change both say 1 and fail at
the frame rather than at the number: a `malformed server-hello` or a
`malformed welcome`, instead of a protocol mismatch naming both versions. If
you are reading this while debugging exactly that, the answer is that one end
predates the change. The next contract change raises the number.
"""
