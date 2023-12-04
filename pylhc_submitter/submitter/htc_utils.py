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
from typing import Any, Dict, List, Union

from pandas import DataFrame

from pylhc_submitter.constants.htcondor import (BASH_FILENAME, CMD_SUBMIT, HTCONDOR_JOBLIMIT,
                                                JOBFLAVOURS, NOTIFICATIONS, SHEBANG, SUBFILE)
from pylhc_submitter.constants.job_submitter import (COLUMN_DEST_DIRECTORY, COLUMN_JOB_DIRECTORY,
                                                     COLUMN_JOB_FILE, COLUMN_SHELL_SCRIPT,
                                                     EXECUTEABLEPATH, NON_PARAMETER_COLUMNS)
from pylhc_submitter.submitter import iotools
from pylhc_submitter.submitter.mask import is_mask_file
from pylhc_submitter.utils.environment import on_windows

try:
    import htcondor
except ImportError:  # will be handled by job_submitter
    class htcondor:
        """Dummy HTCondor module. To satisfy the typing. """
        Submit: Any = None


LOG = logging.getLogger(__name__)


# Subprocess Methods ###########################################################


def create_subfile_from_job(cwd: Path, submission: Union[str, htcondor.Submit]) -> Path:
    """
    Write file to submit to ``HTCondor``.
    
    Args:
        cwd (Path): working directory
        submission (str, htcondor.Submit): HTCondor submission definition (i.e. content of the file)

    Returns:
        Path: path to sub-file
    """
    subfile = cwd / SUBFILE
    LOG.debug(f"Writing sub-file '{str(subfile)}'.")
    with subfile.open("w") as f:
        f.write(str(submission))
    return subfile


def submit_jobfile(jobfile: Path, ssh: str) -> None:
    """Submit subfile to ``HTCondor`` via subprocess.
    
    Args:
        jobfile (Path): path to sub-file
        ssh (str): ssh target
    """
    proc_args = [CMD_SUBMIT, jobfile]
    if ssh:
        proc_args = ["ssh", ssh] + proc_args
    status = _start_subprocess(proc_args)
    if status:
        raise RuntimeError("Submit to HTCondor was not successful!")
    else:
        LOG.info("Jobs successfully submitted.")


def _start_subprocess(command: List[str]) -> int:
    """ Start subprocess and log output. 
    
    Args:
        command (List[str]): command to execute

    Returns:
        int: return code of the process
    """
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


def create_multijob_for_bashfiles(job_df: DataFrame, **kwargs) -> str:
    """
    Function to create an ``HTCondor`` submission content for all job-scripts, 
    i.e. bash-files, in the job_df.

    Keyword Args:
        output_dir (str): output directory that will be transferred. Defaults to ``None``.
        jobflavour (str): max duration of the job. Needs to be one of the ``HTCondor`` Jobflavours.
            Defaults to ``workday``.
        group (str): force use of accounting group. Defaults to ``None``.
        retries (int): maximum amount of retries. Default to ``3``.
        notification (str): Notify under certain conditions. Defaults to ``error``.
        priority (int): Priority to order your jobs. Defaults to ``None``.

    Returns:
        str: HTCondor submission definition.
    """
    # Pre-defined HTCondor arguments for our jobs
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
    submit_dict.update(map_kwargs(kwargs))
    
    # Let the htcondor create the submit-file
    submission = htcondor.Submit(submit_dict)

    # add the multiple bash files
    scripts = [
        str(Path(*parts))
        for parts in zip(job_df[COLUMN_JOB_DIRECTORY], job_df[COLUMN_SHELL_SCRIPT])
    ]
    args = [",".join(parts) for parts in zip(scripts, job_df[COLUMN_JOB_DIRECTORY])]
    queueArgs = ["queue executable, initialdir from (", *args, ")"]

    # ugly but submission.setQArgs doesn't take string containing '\n':
    # submission.setQArgs("\n".join(queueArgs))  # doesn't work
    submission = str(submission) + "\n".join(queueArgs)
    LOG.debug(f"Created HTCondor subfile with content: \n{submission}")
    return submission


# Main functions ###############################################################


def make_subfile(cwd: Path, job_df: DataFrame, **kwargs) -> Path:
    """
    Creates submit-file for ``HTCondor``.
    For kwargs, see ``create_multijob_for_bashfiles``.

    Args:
        cwd (Path): working directory
        job_df (DataFrame): DataFrame containing all the job-information

    Returns:
        Path: path to the submit-file
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
    """
    Write the bash-files to be called by ``HTCondor``, which in turn call the executable.
    Takes as input `Dataframe`, job type, and optional additional commandline arguments for the script.
    A shell script is created in each job directory in the dataframe.

    Args:
        job_df (DataFrame): DataFrame containing all the job-information
        output_dir (str): output directory that will be transferred. Defaults to ``None``.
        executable (str): name of the executable. Defaults to ``madx``.
        cmdline_arguments (dict): additional commandline arguments for the executable
        mask (Union[str, Path]): string or path to the mask-file. Defaults to ``None``.

    Returns:
        DataFrame: The provided ``job_df`` but with added path to the scripts.
    """
    if len(job_df.index) > HTCONDOR_JOBLIMIT:
        raise AttributeError("Submitting too many jobs for HTCONDOR")

    exec_path = f"{str(EXECUTEABLEPATH.get(executable, executable))} " if executable else ''
    cmds = f" {' '.join([f'{param} {val}' for param, val in cmdline_arguments.items()])}" if cmdline_arguments else ''

    shell_scripts = [None] * len(job_df.index)
    for idx, (jobid, job) in enumerate(job_df.iterrows()):
        job_dir = Path(job[COLUMN_JOB_DIRECTORY])
        bash_file_name = f"{BASH_FILENAME}.{jobid}.{'bat' if on_windows() else 'sh'}"
        jobfile = job_dir / bash_file_name

        LOG.debug(f"Writing bash-file {idx:d} '{jobfile}'.")
        with open(jobfile, "w") as f:
            # Preparation ---
            if not on_windows():
                f.write(f"{SHEBANG}\n")
   
            if output_dir is not None:
                f.write(f"mkdir {str(output_dir)}\n")

            # The actual job execution ---
            f.write(exec_path)

            # Call the mask-file or the filled-template string
            if is_mask_file(mask):
                f.write(str(job_dir / job[COLUMN_JOB_FILE]))
            else:
                replace_columns = [column for column in job.index.tolist() if column not in NON_PARAMETER_COLUMNS]
                f.write(mask % dict(zip(replace_columns, job[replace_columns])))

            # Additional commands for the mask/string
            f.write(cmds)
            f.write("\n")

            # Manually copy output (if needed) ---
            dest_dir = job.get(COLUMN_DEST_DIRECTORY) 
            if output_dir and dest_dir and output_dir != dest_dir:
                if iotools.is_eos_uri(dest_dir):
                    # Note: eos-cp needs `/` at the end of both, source and target, dirs...
                    cp_command =  f'eos cp -r {_str_ending_with_slash(output_dir)} {_str_ending_with_slash(dest_dir)}'  
                else:
                    # ...but '/' at the end of source dir copies only the content on macOS.
                    cp_command =  f'cp -r {output_dir} {_str_ending_with_slash(dest_dir)}'  
                    
                f.write(f'{cp_command}\n')

        shell_scripts[idx] = bash_file_name

    job_df[COLUMN_SHELL_SCRIPT] = shell_scripts
    return job_df


def map_kwargs(add_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Maps the kwargs for the job-file. 
    Some arguments have pre-defined choices and defaults, the remaining ones are just passed on.

    Args:
        add_dict (Dict[str, Any]): additional kwargs to add to the defaults.

    Returns:
        Dict[str, Any]: The mapped kwargs.
    """
    new = {}

    # Predefined mappings 
    htc_map = { # name: mapped_name, choices, default
        "jobflavour": ("+JobFlavour", JOBFLAVOURS, "workday"),
        "output_dir": ("transfer_output_files", None, '""'),
        "accounting_group": ("+AccountingGroup", None, None),
        "max_retries": ("max_retries", None, 3),
        "notification": ("notification", NOTIFICATIONS, "error"),
    }
    for key, (mapped, choices, default) in htc_map.items():
        try:
            value = add_dict.pop(key)
        except KeyError:
            value = default  # could be `None`
        else:
            if choices is not None and value not in choices:
                raise TypeError(
                    f"{key} needs to be one of '{str(choices).strip('[]')}' but "
                    f"instead was '{value}'"
                )
        if value is not None:
            new[mapped] = _maybe_put_in_quotes(mapped, value)

    # Pass-Through Arguments
    LOG.debug(f"Remaining arguments to be added: '{str(add_dict).strip('{}'):s}'")
    new.update(add_dict)
    return new


# Helper #######################################################################

def _maybe_put_in_quotes(key: str, value: Any) -> Any:
    """ Put value in quoted strings if key starts with '+' """
    if key.startswith("+"):
        return f'"{value}"'
    return value


def _str_ending_with_slash(s: Union[Path, str]) -> str:
    """ Add a slash at the end of a path if not present. """
    s  = str(s)
    if s.endswith("/"):
        return s
    return f"{s}/"


# Script Mode ##################################################################


if __name__ == "__main__":
    raise EnvironmentError(f"{__file__} is not supposed to run as main.")
