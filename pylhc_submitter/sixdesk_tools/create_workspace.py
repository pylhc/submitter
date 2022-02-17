"""
Create SixDesk Workspace
-----------------------------------

Tools to setup the workspace for sixdesk.
"""
import re
import shutil
from dataclasses import asdict
from pathlib import Path
from typing import Union

import numpy as np
import logging

from generic_parser import DotDict

from pylhc_submitter.constants.autosix import (
    SETENV_SH,
    SIXENV_REQUIRED,
    SEED_KEYS,
    get_workspace_path,
    get_scratch_path,
    get_sixjobs_path,
    get_masks_path,
    get_mad6t_mask_path,
    get_mad6t1_mask_path,
    get_autosix_results_path,
    get_sysenv_path,
    get_sixdeskenv_path,
    SixDeskEnvironment, SIXENV_OPTIONAL,
)
from pylhc_submitter.constants.external_paths import SIXDESK_UTILS
from pylhc_submitter.sixdesk_tools.utils import start_subprocess

SYSENV_MASK = Path(__file__).parent / "mask_sysenv"
SIXDESKENV_MASK = Path(__file__).parent / "mask_sixdeskenv"

LOG = logging.getLogger(__name__)


# Main -------------------------------------------------------------------------


def create_job(jobname: str, basedir: Path, executable: Union[Path, str], mask_text: str,
               sixdesk: Path = SIXDESK_UTILS, ssh: str = None, **kwargs):
    """ Create environment and individual jobs/masks for SixDesk to send to HTC.

    Keyword Args:
        Need to contain all replacements for sixdeskenv and the mask.
    """
    _create_workspace(jobname, basedir, sixdesk=sixdesk, ssh=ssh)
    _create_sysenv(jobname, basedir, binary_path=executable)
    _create_sixdeskenv(jobname, basedir, **kwargs)
    _write_mask(jobname, basedir, mask_text, **kwargs)
    LOG.info("Workspace prepared.")


def init_workspace(jobname: str, basedir: Path, sixdesk: Path = SIXDESK_UTILS, ssh: str = None):
    """ Initializes the workspace with sixdeskenv and sysenv. """
    sixjobs_path = get_sixjobs_path(jobname, basedir)
    start_subprocess([sixdesk / SETENV_SH, "-s"], cwd=sixjobs_path, ssh=ssh)
    LOG.info("Workspace initialized.")


def remove_twiss_fail_check(jobname: str, basedir: Path):
    """ Comments out the "Twiss fail" check from mad6t.sh """
    LOG.info("Applying twiss-fail hack.")
    for mad6t_path in (
        get_mad6t_mask_path(jobname, basedir),
        get_mad6t1_mask_path(jobname, basedir),
    ):
        with open(mad6t_path, "r") as f:
            lines = f.readlines()

        check_started = False
        for idx, line in enumerate(lines):
            if line.startswith('grep -i "TWISS fail"'):
                check_started = True

            if check_started:
                lines[idx] = f"# {line}"
                if line.startswith("fi"):
                    break
        else:
            LOG.info(f"'TWISS fail' not found in {mad6t_path.name}")
            continue

        with open(mad6t_path, "w") as f:
            f.writelines(lines)


def fix_pythonfile_call(jobname: str, basedir: Path):
    """ Removes '<' in the `binary file` line in mad6t.sh so __file__ works. """
    LOG.info("Applying python-file call fix.")
    for mad6t_path in (
            get_mad6t_mask_path(jobname, basedir),
            get_mad6t1_mask_path(jobname, basedir),
    ):
        with open(mad6t_path, "r") as f:
            lines = f.readlines()

        for idx, line in enumerate(lines):
            if line.startswith('$MADX_PATH/$MADX'):
                lines[idx] = f'$MADX_PATH/$MADX $junktmp/$filejob."$i" > $filejob.out."$i"\n'
                break
        else:
            raise IOError(f"'$MADX_PATH/$MADX' line not found in {mad6t_path.name}")

        with open(mad6t_path, "w") as f:
            f.writelines(lines)


def set_max_materialize(sixdesk: Path, max_materialize: int = None):
    """ Adds the ``max_materialize`` option into the htcondor sixtrack
    submission-file."""
    if max_materialize is None:
        return

    LOG.info(f"Setting max_materialize for SixTrack to {max_materialize}.")
    sub_path = sixdesk / "utilities" / "templates" / "htcondor" / "htcondor_run_six.sub"
    sub_content = sub_path.read_text()

    # Remove whole max_materialize line if present
    if max_materialize == 0:
        if "max_materialize" in sub_content:
            LOG.info("'max_materialize' already set. Removing.")
            sub_content = re.sub(r"max_materialize\s*=\s*\d+\s*", "", sub_content)
        else:
            LOG.debug("'max_materialize' is already not present (as desired).")

    # Set/replace with number
    else:
        max_materialize_str = f"max_materialize = {max_materialize:d}"
        if "max_materialize" in sub_content:
            LOG.info("max_materialize already set. Replacing it with new number.")
            sub_content = re.sub(r"max_materialize\s*=\s*\d+", max_materialize_str, sub_content)
        else:
            sub_content = sub_content.replace("\nqueue", f"\n{max_materialize_str}\nqueue")

    # Write out
    try:
        sub_path.write_text(sub_content)
    except IOError as e:
        raise IOError(f"Could not write to {sub_path!s}. `max_materialization` could not be set.\n"
                      f"Remove option or use a SixDesk with writing rights.") from e


# Helper -----------------------------------------------------------------------


def _create_workspace(jobname: str,  basedir: Path, sixdesk: Path = SIXDESK_UTILS, ssh: str = None):
    """ Create workspace structure (with default files). """
    workspace_path = get_workspace_path(jobname, basedir)
    scratch_path = get_scratch_path(basedir)
    LOG.info(f'Creating new workspace in "{str(workspace_path)}"')

    if workspace_path.exists():
        LOG.warning(f'Workspace in "{str(workspace_path)}" already exists. ')
        LOG.info("Do you want to delete the old workspace? [y/N]")
        user_answer = input()
        if user_answer.lower().startswith("y"):
            shutil.rmtree(workspace_path)
            try:
                shutil.rmtree(scratch_path)
            except FileNotFoundError:
                pass
        else:
            LOG.warning("Keeping Workspace as-is.")
            return

    scratch_path.mkdir(parents=True, exist_ok=True)

    # create environment with all necessary files
    # _start_subprocess(['git', 'clone', GIT_REPO, basedir])
    start_subprocess([sixdesk / SETENV_SH, "-N", workspace_path.name], cwd=basedir, ssh=ssh)

    # create autosix results folder.
    # Needs to be done after above command (as it crashes if folder exists)
    # but before end of this stage (as it needs to write the stagefile)
    get_autosix_results_path(jobname, basedir).mkdir(exist_ok=True, parents=True)


def _create_sixdeskenv(jobname: str, basedir: Path, **kwargs):
    """ Fills sixdeskenv mask and copies it to workspace """
    workspace_path = get_workspace_path(jobname, basedir)
    scratch_path = get_scratch_path(basedir)
    sixdeskenv_path = get_sixdeskenv_path(jobname, basedir)

    missing = [key for key in SIXENV_REQUIRED if key not in kwargs.keys()]
    if len(missing):
        raise ValueError(f"The following keys are required but missing {missing}.")

    sixenv_variables = SixDeskEnvironment(
        JOBNAME=jobname,
        WORKSPACE=workspace_path.name,
        BASEDIR=str(basedir),
        SCRATCHDIR=str(scratch_path),
        TURNSPOWER=np.log10(kwargs["TURNS"]),
        **{k: v for k, v in kwargs.items() if k in SIXENV_REQUIRED + SIXENV_OPTIONAL},
    )

    sixenv_text = SIXDESKENV_MASK.read_text()
    sixdeskenv_path.write_text(sixenv_text % asdict(sixenv_variables))
    LOG.debug("sixdeskenv written.")


def _create_sysenv(jobname: str, basedir: Path, binary_path: Path):
    """ Fills sysenv mask and copies it to workspace """
    LOG.info(f"Chosen binary for mask '{str(binary_path)}'")
    sysenv_path = get_sysenv_path(jobname, basedir)
    sysenv_replace = dict(
        MADXPATH=str(binary_path.parent),
        MADXBIN=binary_path.name,
    )
    sysenv_text = SYSENV_MASK.read_text()
    sysenv_path.write_text(sysenv_text % sysenv_replace)
    LOG.debug("sysenv written.")


def _write_mask(jobname: str, basedir: Path, mask_text: str, **kwargs):
    """ Fills mask with arguments and writes it out. """
    masks_path = get_masks_path(jobname, basedir)
    seed_range = [kwargs.get(key, getattr(SixDeskEnvironment, key)) for key in SEED_KEYS]

    if seed_range.count(None) == 1:
        raise ValueError(
            "First- or Lastseed is set, but the other one is deactivated. " "Set or unset both."
        )

    if ("%SEEDRAN" not in mask_text) and ("%SEEDRAN" not in kwargs.values()) and any(seed_range):
        raise ValueError(
            "First- and Lastseed are set, but no seed-variable '%SEEDRAN' found in mask."
        )

    mask_text = mask_text.replace("%SEEDRAN", "#!#SEEDRAN")  # otherwise next line will complain
    mask_filled = mask_text % kwargs
    mask_filled = mask_filled.replace(
        "#!#SEEDRAN", "%SEEDRAN"
    )  # bring seedran back for sixdesk seed-loop

    with open(masks_path / f"{jobname}.mask", "w") as mask_out:
        mask_out.write(mask_filled)
