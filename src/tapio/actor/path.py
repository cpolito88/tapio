"""Actor paths: the stable, printable identity of a place in the tree."""

import re
from dataclasses import dataclass
from typing import Final, Self, final

__all__ = ["ActorPath"]

SCHEME: Final = "tapio"

# Deliberately narrow. A path element appears in log lines and in the string
# form below, so characters that would make either ambiguous are out: "/" and
# "#" are structural, whitespace is unreadable, and a leading "$" is reserved
# for generated names (spawn_anonymous).
_ELEMENT_RE: Final = re.compile(r"\A\$?[A-Za-z0-9][A-Za-z0-9._-]*\Z")


@final
@dataclass(frozen=True, slots=True)
class ActorPath:
    """An immutable position in one actor system's tree.

    The string form is `tapio://system/user/greeter#42`, where the fragment is
    the incarnation uid. That uid distinguishes a restarted actor's ref from a
    differently spawned one at the same path: a restart keeps both path and
    uid, while a stop-then-respawn under the same name gets a new uid, so a
    stale ref cannot silently address the newcomer.
    """

    system: str
    elements: tuple[str, ...] = ()
    uid: int = 0

    def __post_init__(self) -> None:
        """Reject names that would make the string form ambiguous."""
        if not _ELEMENT_RE.match(self.system):
            msg = f"invalid actor system name: {self.system!r}"
            raise ValueError(msg)
        for element in self.elements:
            if not _ELEMENT_RE.match(element):
                msg = (
                    f"invalid actor name {element!r}: names must start with a "
                    "letter or digit (or '$' for generated names) and contain "
                    "only letters, digits, '.', '_' and '-'"
                )
                raise ValueError(msg)
        if self.uid < 0:
            msg = f"invalid incarnation uid: {self.uid!r}"
            raise ValueError(msg)

    @classmethod
    def root(cls, system: str) -> Self:
        """Return the root path of the named system."""
        return cls(system=system)

    @property
    def is_root(self) -> bool:
        """Whether this is the system's root path."""
        return not self.elements

    @property
    def name(self) -> str:
        """The last element of the path, or `/` at the root."""
        return self.elements[-1] if self.elements else "/"

    @property
    def parent(self) -> Self:
        """The enclosing path. The root is its own parent.

        The uid is dropped, since it identifies an incarnation of *this* actor
        and says nothing about the parent's.
        """
        if self.is_root:
            return self
        return ActorPath(system=self.system, elements=self.elements[:-1])

    def child(self, name: str, uid: int = 0) -> Self:
        """Return the path of a child of this actor."""
        return ActorPath(system=self.system, elements=(*self.elements, name), uid=uid)

    def with_uid(self, uid: int) -> Self:
        """Return this path stamped with an incarnation uid."""
        return ActorPath(system=self.system, elements=self.elements, uid=uid)

    def __str__(self) -> str:
        """Render as `tapio://system/user/greeter#42`."""
        body = "/".join(self.elements)
        fragment = f"#{self.uid}" if self.uid else ""
        return f"{SCHEME}://{self.system}/{body}{fragment}"

    def __repr__(self) -> str:
        """Render as the string form, which is what a reader wants to see."""
        return f"ActorPath({str(self)!r})"
