"""
SixDesk Submission Utils
---------------------------

Individual functions to call SixDesk functionality.
"""
import logging
from pathlib import Path
from typing import Union

from pylhc_submitter.constants.autosix import (
    MAD_TO_SIXTRACK_SH,
    RUNSIX_SH,
    RUNSTATUS_SH,
    SIXDB,
    get_sixjobs_path, StageSkip,
)
from pylhc_submitter.constants.external_paths import SIXDESK_UTILS
from pylhc_submitter.sixdesk_tools.utils import start_subprocess

LOG = logging.getLogger(__name__)


def submit_mask(jobname: str, basedir: Path, sixdesk: Path = SIXDESK_UTILS, ssh: str = None):
    """ Run the mask (probably Madx) and generate sixtrack input files.

    Args:
        jobname (str): Name of the Job
        basedir (Path): SixDesk Basefolder Location
        sixdesk (Path): Path to the SixDesk installation/repo
    """
    LOG.info("Submitting mask to run for sixtrack input generation.")
    sixjobs_path = get_sixjobs_path(jobname, basedir)
    start_subprocess([sixdesk / MAD_TO_SIXTRACK_SH, "-s"], cwd=sixjobs_path, ssh=ssh)
    LOG.info("Submitted mask-jobs.")


def check_sixtrack_input(jobname: str, basedir: Path, sixdesk: Path = SIXDESK_UTILS, ssh: str = None, resubmit: bool = False):
    """ Checks the generated input files needed by sixtrack and resubmits, if requested. """
    LOG.info("Checking if input files are present.")
    sixjobs_path = get_sixjobs_path(jobname, basedir)
    try:
        start_subprocess([sixdesk /MAD_TO_SIXTRACK_SH, "-c"], cwd=sixjobs_path, ssh=ssh)
    except OSError as e:
        if resubmit:
            LOG.info("Resubmitting mask to run wrong seeds for sixtrack input generation.")
            start_subprocess([sixdesk / MAD_TO_SIXTRACK_SH, "-w"], cwd=sixjobs_path, ssh=ssh)
            raise StageSkip("Resubmitted input generation jobs "
                            "(Not really an error, but the run is now interrupted).")
        else:
            raise StageSkip(
                "Checking input files failed. Check (debug-) logs. "
                "Maybe restart with 'resubmit' flag."
            ) from e
    else:
        LOG.info("Check for input files was successful.")


def submit_sixtrack(jobname: str, basedir: Path, python: Path = None,
                    sixdesk: Path = SIXDESK_UTILS, ssh: str = None, resubmit: bool = False):
    """ Generate simulation files and check if runnable and submit. """
    re_str = "Re-" if resubmit else ""
    LOG.info(f"{re_str}Submitting to sixtrack.")
    sixjobs_path = get_sixjobs_path(jobname, basedir)
    args = ["-i"] if resubmit else ["-a"]
    if python is not None:
        if not python.is_dir():
            python = python.parent
        args += ["-P", str(python)]
    try:
        start_subprocess([sixdesk / RUNSIX_SH] + args, cwd=sixjobs_path, ssh=ssh, check_log='exit status: 1')
    except OSError as e:
        raise StageSkip(
            f"{re_str}Submit to sixtrack for {jobname} ended in error."
            f" Input generation possibly not finished. Check your Scheduler."
        ) from e
    else:
        LOG.info(f"{re_str}Submitted jobs to Sixtrack")


def check_sixtrack_output(jobname: str, basedir: Path, python: Union[Path, str],
                          sixdesk: Path = SIXDESK_UTILS, ssh: str = None, resubmit: bool = False):
    """ Checks if the sixtrack output is all there. """
    LOG.info("Checking if sixtrack has finished.")
    sixjobs_path = get_sixjobs_path(jobname, basedir)
    try:
        start_subprocess([sixdesk / RUNSTATUS_SH], cwd=sixjobs_path, ssh=ssh)
    except OSError as e:
        if resubmit:
            submit_sixtrack(jobname, basedir, python=python, ssh=ssh, resubmit=True)
            raise StageSkip(
                f"Sixtrack for {jobname} seems to be incomplete."
                " Resubmitted incomplete sixtrack jobs."
                " Wait until they have finished and run again."
            )
        else:
            raise StageSkip(
                f"Sixtrack for {jobname} seems to be incomplete."
                f" Run possibly not finished. Check (debug-) log or your Scheduler."
            ) from e
    else:
        LOG.info("Sixtrack results are all present.")


def sixdb_load(jobname: str, basedir: Path, python: Union[Path, str],
               sixdesk: Path = SIXDESK_UTILS, ssh: str = None):
    """ Creates sixdb database and loads the study results into it. """
    LOG.info("Loading study into database.")
    sixjobs_path = get_sixjobs_path(jobname, basedir)
    try:
        start_subprocess([python, sixdesk / SIXDB, ".", "load_dir"], cwd=sixjobs_path, ssh=ssh)
    except OSError as e:
        raise StageSkip(f"Sixdb loading for {jobname} failed. Check (debug-) log.") from e
    else:
        LOG.info("Created database for study.")


def sixdb_cmd(jobname: str, basedir: Path, python: Union[Path, str], cmd: list,
              sixdesk: Path = SIXDESK_UTILS, ssh: str = None):
    """ Performs analysis on the sixdb database. """
    cmd_str = " ".join(cmd)
    LOG.info(f"Performing sixdb command `{cmd_str}`.")
    sixjobs_path = get_sixjobs_path(jobname, basedir)
    try:
        start_subprocess([python, sixdesk / SIXDB, jobname] + cmd, cwd=sixjobs_path, ssh=ssh)
    except OSError as e:
        raise StageSkip(f"SixBD command {cmd_str} for {jobname} failed. Check (debug-) log.") from e
    else:
        LOG.info(f"SixDB command '{cmd_str}' successfully run.")
