"""
External Paths
--------------

Specific constants relating to external paths to be used,
to help with consistency.
"""
from pathlib import Path

AFS_CERN: Path = Path("/", "afs", "cern.ch")
LINTRACK: Path = AFS_CERN / "eng" / "sl" / "lintrack"

# Binary Files -----------------------------------------------------------------
MADX_BIN: Path = AFS_CERN / "user" / "m" / "mad" / "bin" / "madx"
PYTHON3_BIN: Path = LINTRACK / "omc_python3" / "bin" / "python"
PYTHON2_BIN: Path = LINTRACK / "miniconda2" / "bin" / "python"
SIXDESK_UTILS: Path = AFS_CERN / "project" / "sixtrack" / "SixDesk_utilities" / "pro"
