"""
Constants: Autosix
----------------------------------

Collections of constants and paths used in autosix.

:module: constants.autosix
:author: jdilly

"""
from pathlib import Path

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

# Defaults ---

DEFAULTS = dict(
    python2=None,
    python3="python3",
    da_turnstep=100,
    executable=MADX_BIN,
    sixdesk_directory=SIXDESK_UTILS
)

# Sixenv ---
SIXENV_REQUIRED = ["TURNS", "AMPMIN", "AMPMAX", "AMPSTEP", "ANGLES"]
SIXENV_DEFAULT = dict(
    RESUBMISSION=0,  # 0: never, 1: if fort.10, 2: always
    PLATFORM="HTCondor",
    LOGLEVEL=0,  # 0: basic + errors , 1: + info, >=2: + debug
    FIRSTSEED=1,
    LASTSEED=60,
    ENERGY="col",  # 'col' or 'inj'
    NPAIRS=30,  # 1-32 particle pairs
    EMITTANCE=3.75,  # normalized emittance
    DIMENSIONS=6,  # Phase-Space dimensions
    WRITEBINS=500,
)
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

