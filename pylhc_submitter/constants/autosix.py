"""
Constants: Autosix
----------------------------------

Collections of constants and paths used in autosix.

:module: constants.autosix
:author: jdilly

"""
from dataclasses import dataclass, fields, MISSING
from pathlib import Path
from typing import Union

from pylhc_submitter.constants.external_paths import MADX_BIN, SIXDESK_UTILS

# Program Paths ----------------------------------------------------------------
UTILITIES_DIR = Path("utilities")
BASH_DIR = UTILITIES_DIR / "bash"
SETENV_SH = BASH_DIR / "set_env.sh"
MAD_TO_SIXTRACK_SH = BASH_DIR / "mad6t.sh"
RUNSIX_SH = BASH_DIR / "run_six.sh"
RUNSTATUS_SH = BASH_DIR / "run_status"
DOT_PROFILE = BASH_DIR / "dot_profile"
SIXDB = UTILITIES_DIR / "externals" / "SixDeskDB" / "sixdb"
SIXDESKLOCKFILE = "sixdesklock"

# Constants and Requirements ---------------------------------------------------

HEADER_BASEDIR = "BASEDIR"

# AutoSix Environment (also defines defaults) ---
@dataclass
class AutoSixEnvironment:
    mask_text: str
    working_directory: Path
    executable: Union[str, Path] = MADX_BIN
    python2: Union[str, Path] = None
    python3: Union[str, Path] = "python3"
    da_turnstep: int = 100
    sixdesk_directory: Path = SIXDESK_UTILS
    unlock: bool = False
    max_stage: 'Stage' = None
    ssh: str = None
    stop_workspace_init: bool = False
    apply_mad6t_hacks: bool = False
    resubmit: bool = False


# Sixenv ---
@dataclass
class SixDeskEnvironment:
    TURNS: int
    AMPMIN: int
    AMPMAX: int
    AMPSTEP: int
    ANGLES: int
    JOBNAME: str = None  # set dynamically
    WORKSPACE: str = None  # set dynamically
    BASEDIR: str = None  # set dynamically
    SCRATCHDIR: str = None  # set dynamically
    TURNSPOWER: int = None  # set dynamically
    RESUBMISSION: int = 0  # 0: never, 1: if fort.10, 2: always
    PLATFORM: str = "HTCondor"
    LOGLEVEL: int = 0  # 0: basic + errors , 1: + info, >=2: + debug
    FIRSTSEED: int = 1
    LASTSEED: int = 60
    ENERGY: str = "col"  # 'col' or 'inj'
    NPAIRS: int = 30  # 1-32 particle pairs
    EMITTANCE: float = 3.75  # normalized emittance
    DIMENSIONS: int = 6  # Phase-Space dimensions
    WRITEBINS: int = 500


SIXENV_REQUIRED = [f.name for f in fields(SixDeskEnvironment) if (f.default is MISSING)]  # required by user
SIXENV_OPTIONAL = [f.name for f in fields(SixDeskEnvironment) if (f.default is MISSING) or (f.default is not None)]
SEED_KEYS = ["FIRSTSEED", "LASTSEED"]

# SixDB and Postprocess ---

SIXTRACK_OUTPUT_FILES = "fort.10.gz", "Sixout.zip"
SIXTRACK_INPUT_CHECK_FILES = "JOB_NOT_YET_STARTED", "JOB_NOT_YET_COMPLETED"

HEADER_NTOTAL, HEADER_INFO, HEADER_HINT = "NTOTAL", "INFO", "HINT"
MEAN, STD, MIN, MAX, N = "MEAN", "STD", "MIN", "MAX", "N"
SEED, ANGLE, ALOST1, ALOST2, AMP = "SEED", "ANGLE", "ALOST1", "ALOST2", "A"


# Errors ---


class StageSkip(Exception):
    """ Indicates that the stage was not completed or skipped entirely.
    This can be due to an error or on purpose (e.g. user interaction before
    restart). """
    pass


class StageStop(Exception):
    """ A signal sent at the end of a Stage indicating, that it has succeeded
    and that any iteration should be stopped after this Stage as the jobs have
    been submitted and the user needs to wait for them to finish. """
    pass


# Workspace Paths --------------------------------------------------------------

# Input ---
def get_workspace_path(jobname: str, basedir: Path) -> Path:
    return basedir / f"workspace-{jobname}"


def get_scratch_path(basedir: Path) -> Path:
    return basedir / f"scratch-0"


def get_sixjobs_path(jobname: str, basedir: Path) -> Path:
    return get_workspace_path(jobname, basedir) / "sixjobs"


def get_sixdeskenv_path(jobname: str, basedir: Path) -> Path:
    return get_sixjobs_path(jobname, basedir) / "sixdeskenv"


def get_sysenv_path(jobname: str, basedir: Path) -> Path:
    return get_sixjobs_path(jobname, basedir) / "sysenv"


def get_masks_path(jobname: str, basedir: Path) -> Path:
    return get_sixjobs_path(jobname, basedir) / "mask"


def get_track_path(jobname: str, basedir: Path) -> Path:
    return get_sixjobs_path(jobname, basedir) / "track"


def get_database_path(jobname: str, basedir: Path) -> Path:
    return get_sixjobs_path(jobname, basedir) / f"{jobname}.db"


def get_sixtrack_input_path(jobname: str, basedir: Path) -> Path:
    return get_sixjobs_path(jobname, basedir) / "sixtrack_input"


def get_mad6t_mask_path(jobname: str, basedir: Path) -> Path:
    return get_sixtrack_input_path(jobname, basedir) / "mad6t.sh"


def get_mad6t1_mask_path(jobname: str, basedir: Path) -> Path:
    return get_sixtrack_input_path(jobname, basedir) / "mad6t1.sh"


# Output ---


def get_autosix_results_path(jobname: str, basedir: Path) -> Path:
    return get_sixjobs_path(jobname, basedir) / "autosix_output"


def get_stagefile_path(jobname: str, basedir: Path) -> Path:
    return get_autosix_results_path(jobname, basedir) / "stages_completed.txt"


def get_tfs_da_path(jobname: str, basedir: Path) -> Path:
    return get_autosix_results_path(jobname, basedir) / f"{jobname}_da.tfs"


def get_tfs_da_seed_stats_path(jobname: str, basedir: Path) -> Path:
    return get_autosix_results_path(jobname, basedir) / f"{jobname}_da_per_seed.tfs"


def get_tfs_da_angle_stats_path(jobname: str, basedir: Path) -> Path:
    return get_autosix_results_path(jobname, basedir) / f"{jobname}_da_per_angle.tfs"

