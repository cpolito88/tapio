"""The package version, in one place both the package and the wire can read.

It lives in its own module rather than in `tapio/__init__.py` because the
handshake needs it. A link reports its version to the peer, and the module
that does is imported by the actor package, so reading the version from the
top-level package would create an import cycle.

Nothing here is a literal. **The version is the git tag**, written into the
distribution at build time and read back from the installed metadata, so there
is no number in this repository that can disagree with the release it claims
to be. A release is a tag and nothing else, which is also what keeps the
release job from having to push to a protected branch.

A source tree that was never built or installed has no tag to read and says
so, with a version that is obviously not a release rather than a plausible
number that happens to be wrong.
"""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _installed_version

__all__ = ["__version__"]

_UNKNOWN = "0.0.0+unknown"
"""What an uninstalled source tree reports.

Chosen to be unmistakable. A peer that sees this in a handshake is talking to
something somebody is running out of a checkout, which is worth knowing.
"""

_DISTRIBUTION = "tapio-py"
"""The name the metadata is filed under, which is not the import package.

PyPI refuses `tapio`, so the distribution is `tapio-py` while the package you
import is still `tapio`. `importlib.metadata` takes the first of those. Asking
it for the second raises on every install, which is what left every release
reporting the fallback above and every handshake reporting it to its peer.
"""


def _read_version() -> str:
    """Read the version this distribution was built with.

    Returns:
        The version from the installed metadata, or `_UNKNOWN` for a source
        tree that was never built or installed and so has no tag to read.
    """
    try:
        return _installed_version(_DISTRIBUTION)
    except PackageNotFoundError:
        return _UNKNOWN


__version__ = _read_version()
