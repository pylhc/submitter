"""
SixDesk Utilities
--------------------

Helper Utilities for Autosix.
"""
import logging
import subprocess
from pathlib import Path

from pylhc_submitter.constants.autosix import SIXDESKLOCKFILE, get_workspace_path
from pylhc_submitter.constants.external_paths import SIXDESK_UTILS
from pylhc_submitter.submitter.mask import find_named_variables_in_mask

LOG = logging.getLogger(__name__)


# Checks  ----------------------------------------------------------------------


def check_mask(mask_text: str, replace_args: dict):
    """ Checks validity/compatibility of the mask and replacement dict. """
    dict_keys = set(replace_args.keys())
    mask_keys = find_named_variables_in_mask(mask_text)
    not_in_dict = mask_keys - dict_keys

    if len(not_in_dict):
        raise KeyError(
            "The following keys in the mask were not found for replacement: "
            f"{str(not_in_dict).strip('{}')}"
        )


# Locks ------------------------------------------------------------------------


def is_locked(jobname: str, basedir: Path, unlock: bool = False):
    """ Checks for sixdesklock-files """
    workspace_path = get_workspace_path(jobname, basedir)
    locks = list(workspace_path.glob(f"**/{SIXDESKLOCKFILE}"))  # list() for repeated usage

    if locks:
        LOG.info("The following folders are locked:")
        for lock in locks:
            LOG.info(f"{str(lock.parent)}")

            with open(lock, "r") as f:
                txt = f.read()
            txt = txt.replace(str(SIXDESK_UTILS), "$SIXUTILS").strip("\n")
            if txt:
                LOG.debug(f" -> locked by: {txt}")

        if unlock:
            for lock in locks:
                LOG.debug(f"Removing lock {str(lock)}")
                lock.unlink()
            return False
        return True
    return False


# Commandline ------------------------------------------------------------------


def start_subprocess(command, cwd=None, ssh: str = None, check_log: str = None):
    if isinstance(command, str):
        command = [command]

    # convert Paths
    command = [str(c) if isinstance(c, Path) else c for c in command]

    if ssh:
        # Send command to remote machine
        command = " ".join(command)
        if cwd:
            command = f'cd "{cwd}" && {command}'
        LOG.debug(f"Executing command '{command}' on {ssh}")
        process = subprocess.Popen(
            ["ssh", ssh, command], shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=cwd,
        )

    else:
        # Execute command locally
        LOG.debug(f"Executing command '{' '.join(command)}'")
        process = subprocess.Popen(
            command, shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=cwd
        )

    # Log output
    for line in process.stdout:
        decoded = line.decode("utf-8").strip()
        if decoded:
            LOG.debug(decoded)
            if check_log is not None and check_log in decoded:
                raise OSError(
                    f"'{check_log}' found in last logging message. "
                    "Something went wrong with the last command. Check (debug-)log."
                )

    # Wait for finish and check result
    if process.wait() != 0:
        raise OSError("Something went wrong with the last command. Check (debug-)log.")
