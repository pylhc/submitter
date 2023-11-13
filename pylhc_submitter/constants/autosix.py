"""
Autosix
-------

Collections of constants and paths used in autosix.

:module: constants.autosix
:author: jdilly

"""
from dataclasses import dataclass, fields, MISSING
from pathlib import Path
from typing import Union

import logging
from pylhc_submitter.constants.external_paths import MADX_BIN, SIXDESK_UTILS
from pylhc_submitter.constants.general import PMASS

LOG = logging.getLogger(__name__)

# Program Paths ----------------------------------------------------------------
UTILITIES_DIR: Path = Path("utilities")
BASH_DIR: Path = UTILITIES_DIR / "bash"
SETENV_SH: Path = BASH_DIR / "set_env.sh"
MAD_TO_SIXTRACK_SH: Path = BASH_DIR / "mad6t.sh"
RUNSIX_SH: Path = BASH_DIR / "run_six.sh"
RUNSTATUS_SH: Path = BASH_DIR / "run_status"
DOT_PROFILE: Path = BASH_DIR / "dot_profile"
SIXDB: Path = UTILITIES_DIR / "externals" / "SixDeskDB" / "sixdb"
SIXDESKLOCKFILE: Path = "sixdesklock"

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
    max_materialize: int = None


# Sixenv ---
class _SetDynamicallyType:
    pass
SetDynamically = _SetDynamicallyType()
"""Sentinel value defining a parameter that is set later dynamically."""

@dataclass
class SixDeskEnvironment:
    TURNS: int
    AMPMIN: int
    AMPMAX: int
    AMPSTEP: int
    ANGLES: int
    JOBNAME: str = SetDynamically
    WORKSPACE: str = SetDynamically
    BASEDIR: str = SetDynamically
    SCRATCHDIR: str = SetDynamically
    TURNSPOWER: int = SetDynamically
    RESUBMISSION: int = 0  # 0: never, 1: if fort.10, 2: always
    PLATFORM: str = "HTCondor"
    LOGLEVEL: int = 0  # 0: basic + errors , 1: + info, >=2: + debug
    FIRSTSEED: int = 1
    LASTSEED: int = 60
    RUNTYPE: str = "col"  # 'col' or 'inj'
    NPAIRS: int = 30  # 1-32 particle pairs
    EMITTANCE: float = 3.75  # normalized emittance
    DIMENSIONS: int = 6  # Phase-Space dimensions
    WRITEBINS: int = 500
    ENERGY: float = None
    GAMMA: float = SetDynamically  # calculated from ENERGY, needs to be here so asdict() converts it

    def __post_init__(self):
        # Check Runtype, Energy and Gamma ---
        energy_map = {'inj': 450_000., 'col': 7000_000.}
        if self.RUNTYPE not in energy_map.keys():
            raise ValueError(f"RUNTYPE needs to be one of : {list(energy_map.keys())}")

        if self.ENERGY is None:
            energy = energy_map[self.RUNTYPE]
            LOG.debug(f"Energy for '{self.RUNTYPE}' defaults to {energy}")
            self.ENERGY = energy

        if self.ENERGY >= 6500_000 and self.RUNTYPE != list(energy_map.keys())[1]:
            LOG.warning(f"Runtype is {self.RUNTYPE}, yet energy is set to {self.ENERGY}. Are you sure?")

        self.GAMMA = self.ENERGY / 1000 / PMASS

        # Check Seeds ---
        if any(getattr(self, key) is None for key in SEED_KEYS):  # set by user to None
            for key in SEED_KEYS:
                setattr(self, key, 0)

        # the following checks are limits of SixDesk in 2020
        # and might be fixed upstream in the future
        if self.AMPMAX < self.AMPMIN:
            raise ValueError("Given AMPMAX is smaller than AMPMIN.")

        if (self.AMPMAX - self.AMPMIN) % self.AMPSTEP:
            raise ValueError("The amplitude range need to be dividable by the amplitude steps!")

        if not self.ANGLES % 2:
            raise ValueError("The number of angles needs to be an uneven one.")


SIXENV_REQUIRED = [f.name for f in fields(SixDeskEnvironment) if (f.default is MISSING)]  # required by user
SIXENV_OPTIONAL = [f.name for f in fields(SixDeskEnvironment) if (f.default is not MISSING) and (f.default is not SetDynamically)]
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

