"""The package version, in one place both the package and the wire can read.

It lives in its own module rather than in `tapio/__init__.py` because the
handshake needs it. A link refuses a peer running a different version, and the
module that decides is imported by the actor package, so reading the version
from the top-level package would create an import cycle.
"""

__all__ = ["__version__"]

__version__ = "0.1.0"
