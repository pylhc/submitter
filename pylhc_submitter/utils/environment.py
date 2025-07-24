"""
Environment Tools
-----------------

Some tools that depend on the environment of the run.
"""

from __future__ import annotations

import sys


def on_windows() -> bool:
    return sys.platform.startswith("win")


def on_linux() -> bool:
    return sys.platform == "linux"
