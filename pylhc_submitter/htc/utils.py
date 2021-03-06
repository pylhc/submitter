"""
HTCondor Utilities
------------------

This module provides functionality to create HTCondor jobs and submit them to ``HTCondor``.

``write_bash`` creates bash scripts executing either a python or madx script.
Takes as input `Dataframe`, job type, and optional additional commandline arguments for the script.
A shell script is created in each job directory in the dataframe.

``make_subfile`` takes the job dataframe and creates the **.sub** files required for submissions to
``HTCondor``. The **.sub** file will be put in the working directory. The maximum runtime of one
job can be specified, standard is 8h.
"""
import logging
import subprocess
from pathlib import Path
from typing import Union

from pandas import DataFrame

from pylhc_submitter.utils.environment_tools import on_windows

try:
    import htcondor
except ImportError:  # will be handled by job_submitter
    pass

from pylhc_submitter.constants.external_paths import MADX_BIN, PYTHON2_BIN, PYTHON3_BIN

LOG = logging.getLogger(__name__)


SHEBANG = "#!/bin/bash"
SUBFILE = "queuehtc.sub"
BASH_FILENAME = "Job"

HTCONDOR_JOBLIMIT = 100000

EXECUTEABLEPATH = {
    "madx": MADX_BIN,
    "python3": PYTHON3_BIN,
    "python2": PYTHON2_BIN,
}


CMD_SUBMIT = "condor_submit"
JOBFLAVOURS = (
    "espresso",  # 20 min
    "microcentury",  # 1 h
    "longlunch",  # 2 h
    "workday",  # 8 h
    "tomorrow",  # 1 d
    "testmatch",  # 3 d
    "nextweek",  # 1 w
)

NOTIFICATIONS = ("always", "complete", "error", "never")


COLUMN_SHELL_SCRIPT = "ShellScript"
COLUMN_JOB_DIRECTORY = "JobDirectory"
COLUMN_JOB_FILE = "JobFile"


# Subprocess Methods ###########################################################


def create_subfile_from_job(cwd: Path, job: str):
    """Write file to submit to ``HTCondor``."""
    subfile = cwd / SUBFILE
    LOG.debug(f"Writing sub-file '{str(subfile)}'.")
    with subfile.open("w") as f:
        f.write(str(job))
    return subfile


def submit_jobfile(jobfile: Path, ssh: str):
    """Submit subfile to ``HTCondor`` via subprocess."""
    proc_args = [CMD_SUBMIT, jobfile]
    if ssh:
        proc_args = ["ssh", ssh] + proc_args
    status = _start_subprocess(proc_args)
    if status:
        raise RuntimeError("Submit to HTCondor was not successful!")
    else:
        LOG.info("Jobs successfully submitted.")


def _start_subprocess(command):
    LOG.debug(f"Executing command '{command}'")
    process = subprocess.Popen(
        command, shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    for line in process.stdout:
        htc_line = line.decode("utf-8").strip()
        if htc_line:
            LOG.debug(f"{htc_line} (from HTCondor)")
    return process.wait()


# Job Creation #################################################################


def create_multijob_for_bashfiles(job_df: DataFrame, **kwargs):
    """
    Function to create an ``HTCondor`` job assuming n_files bash-files.

    Keyword Args:
        output_dir (str): output directory that will be transferred. Defaults to ``None``.
        duration (str): max duration of the job. Needs to be one of the ``HTCondor`` Jobflavours.
            Defaults to ``workday``.
        group (str): force use of accounting group. Defaults to ``None``.
        retries (int): maximum amount of retries. Default to ``3``.
        notification (str): Notify under certain conditions. Defaults to ``error``.
        priority (int): Priority to order your jobs. Defaults to ``None``.
    """
    submit_dict = {
        "MyId": "htcondor",
        "universe": "vanilla",
        "arguments": "$(ClusterId) $(ProcId)",
        "output": Path("$(initialdir)", "$(MyId).$(ClusterId).$(ProcId).out"),
        "error": Path("$(initialdir)", "$(MyId).$(ClusterId).$(ProcId).err"),
        "log": Path("$(initialdir)", "$(MyId).$(ClusterId).$(ProcId).log"),
        "on_exit_remove": "(ExitBySignal == False) && (ExitCode == 0)",
        "requirements": "Machine =!= LastRemoteHost",
    }
    submit_dict.update(_map_kwargs(kwargs))

    job = htcondor.Submit(submit_dict)

    # add the multiple bash files
    scripts = [
        str(Path(*parts))
        for parts in zip(job_df[COLUMN_JOB_DIRECTORY], job_df[COLUMN_SHELL_SCRIPT])
    ]
    args = [",".join(parts) for parts in zip(scripts, job_df[COLUMN_JOB_DIRECTORY])]
    queueArgs = ["queue executable, initialdir from (", *args, ")"]

    # ugly but job.setQArgs doesn't take string containing \n
    # job.setQArgs("\n".join(queueArgs))
    job = str(job) + "\n".join(queueArgs)
    LOG.debug(f"Created HTCondor subfile with content: \n{job}")
    return job


# Main functions ###############################################################


def make_subfile(cwd: Path, job_df: DataFrame, **kwargs):
    """
    Creates submit-file for ``HTCondor``.
    For kwargs, see ``create_multijob_for_bashfiles``.
    """
    job = create_multijob_for_bashfiles(job_df, **kwargs)
    return create_subfile_from_job(cwd, job)


def write_bash(
    job_df: DataFrame,
    output_dir: Path = None,
    executable: str = "madx",
    cmdline_arguments: dict = None,
    mask: Union[str, Path] = None,
) -> DataFrame:
    """Write the bash-files to be called by ``HTCondor``."""
    if len(job_df.index) > HTCONDOR_JOBLIMIT:
        raise AttributeError("Submitting too many jobs for HTCONDOR")

    cmds = ""
    if cmdline_arguments is not None:
        cmds = f" {' '.join([f'{param} {val}' for param, val in cmdline_arguments.items()])}"

    if executable is None:
        exec_path = ''
    else:
        exec_path = f"{str(EXECUTEABLEPATH.get(executable, executable))} "

    shell_scripts = [None] * len(job_df.index)
    for idx, (jobid, job) in enumerate(job_df.iterrows()):
        job_dir = Path(job[COLUMN_JOB_DIRECTORY])
        bash_file_name = f"{BASH_FILENAME}.{jobid}.{'bat' if on_windows() else 'sh'}"
        jobfile = job_dir / bash_file_name
        LOG.debug(f"Writing bash-file {idx:d} '{jobfile}'.")
        with open(jobfile, "w") as f:
            if not on_windows():
                f.write(f"{SHEBANG}\n") 
            if output_dir is not None:
                f.write(f"mkdir {str(output_dir)}\n")
            f.write(exec_path)

            if is_mask_file(mask):
                f.write(str(job_dir / job[COLUMN_JOB_FILE]))
            else:
                replace_columns = [column for column in job.index.tolist() if column not in [COLUMN_SHELL_SCRIPT, COLUMN_JOB_DIRECTORY, COLUMN_JOB_FILE]]
                f.write(mask % dict(zip(replace_columns, job[replace_columns])))
            f.write(cmds)
            f.write("\n")
        shell_scripts[idx] = bash_file_name
    job_df[COLUMN_SHELL_SCRIPT] = shell_scripts
    return job_df


# Helper #######################################################################


def _map_kwargs(add_dict):
    """
    Maps the kwargs for the job-file. Some arguments have pre-defined choices and defaults,
    the remaining ones are just passed on.
    """
    new = {}

    # Predefined ones
    htc_map = {
        "duration": ("+JobFlavour", JOBFLAVOURS, "workday"),
        "output_dir": ("transfer_output_files", None, None),
        "accounting_group": ("+AccountingGroup", None, None),
        "max_retries": ("max_retries", None, 3),
        "notification": ("notification", NOTIFICATIONS, "error"),
    }
    for key, (mapped, choices, default) in htc_map.items():
        try:
            value = add_dict.pop(key)
        except KeyError:
            if default is not None:
                new[mapped] = default
        else:
            if choices is not None and value not in choices:
                raise TypeError(
                    f"{key} needs to be one of '{str(choices).strip('[]')}' but "
                    f"instead was '{value}'"
                )
            new[mapped] = _maybe_put_in_quotes(mapped, value)

    # Pass-Through Arguments
    LOG.debug(f"Remaining arguments to be added: '{str(add_dict).strip('{}'):s}'")
    new.update(add_dict)
    return new


def _maybe_put_in_quotes(key, value):
    if key.startswith("+"):
        return f'"{value}"'
    return value


def is_mask_file(mask):
    try:
        return Path(mask).is_file()
    except OSError:
        return False

def is_mask_string(mask):
    return not is_mask_file(mask)

# Script Mode ##################################################################


if __name__ == "__main__":
    raise EnvironmentError(f"{__file__} is not supposed to run as main.")
