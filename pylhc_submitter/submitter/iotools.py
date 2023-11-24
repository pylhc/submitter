""" 
Job Submitter IO-Tools
----------------------

Tools for input and output for the job-submitter.
"""
import itertools
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple, Union

import numpy as np
import pandas as pd
import tfs

from pylhc_submitter.constants.htcondor import HTCONDOR_JOBLIMIT
from pylhc_submitter.constants.job_submitter import (COLUMN_DEST_DIRECTORY, COLUMN_JOB_DIRECTORY,
                                                     COLUMN_JOBID, JOBDIRECTORY_PREFIX,
                                                     JOBSUMMARY_FILE, SCRIPT_EXTENSIONS)
from pylhc_submitter.submitter import htc_utils
from pylhc_submitter.submitter.mask import (create_job_scripts_from_mask, generate_jobdf_index,
                                            is_mask_file)

LOG = logging.getLogger(__name__)


JobNamesType = Sequence[Union[str, int]]


@dataclass
class CreationOpts:
    """ Options for creating jobs. """
    working_directory: Path         # Path to working directory (afs)
    mask: Union[Path, str]          # Path to mask file or mask-string
    jobid_mask: str                 # Mask for jobid
    replace_dict: Dict[str, Any]    # Replace-dict
    output_dir: Path                # Path to local output directory
    output_destination: Union[Path, str]  # Path or URI to remote output directory (e.g. eos)
    append_jobs: bool               # Append jobs to existing jobs
    resume_jobs: bool               # Resume jobs that have already run/failed/got interrupted
    executable: str                 # Name of executable to call the script (from mask)
    check_files: Sequence[str]      # List of output files to check for success
    script_arguments: Dict[str, Any]  # Arguments to pass to script
    script_extension: str           # Extension of the script to run

    def should_drop_jobs(self) -> bool:
        """ Check if jobs should be dropped after creating the whole parameter space, 
        e.g. because they already exist. """
        return self.append_jobs or self.resume_jobs



def create_jobs(opt: CreationOpts) -> tfs.TfsDataFrame:
    """Main function to prepare all the jobs and folder structure.
    This greates the value-grid based on the replace-dict and
    checks for existing jobs (if so desired).
    A job-dataframe is created - and written out - containing all the information and
    its values are used to generate the job-scripts.
    It also creates bash-scripts to call the executable for the job-scripts. 

    Args:
        opt (CreationOpts): Options for creating jobs 

    Returns:
        tfs.TfsDataFrame: The job-dataframe containing information for all jobs. 
    """
    LOG.debug("Creating Jobs.")

    # Generate product of replace-dict and compare to existing jobs  ---
    parameters, values_grid, prev_job_df = _generate_parameter_space(
        replace_dict=opt.replace_dict,
        append_jobs=opt.append_jobs,
        cwd=opt.working_directory,
    )

    # Check new jobs ---
    njobs = len(values_grid)
    if njobs == 0:
        raise ValueError(f"No (new) jobs found!")

    if njobs > HTCONDOR_JOBLIMIT:
        LOG.warning(
            f"You are attempting to submit an important number of jobs ({njobs})."
            "This can be a high stress on your system, make sure you know what you are doing."
        )

    LOG.debug(f"Initial number of jobs: {njobs:d}")

    # Generate new job-dataframe ---
    job_df = tfs.TfsDataFrame(
        index=generate_jobdf_index(prev_job_df, opt.jobid_mask, parameters, values_grid),
        columns=parameters,
        data=values_grid,
    )
    job_df = tfs.concat([prev_job_df, job_df], sort=False, how_headers='left')

    # Setup folders ---
    job_df = create_folders(job_df, opt.working_directory, opt.output_destination)

    # Create scripts ---
    if is_mask_file(opt.mask):
        LOG.debug("Creating all jobs from mask.")
        script_extension = _get_script_extension(opt.script_extension, opt.executable, opt.mask)
        job_df = create_job_scripts_from_mask(
            job_df, opt.mask, parameters, script_extension
        )

    LOG.debug("Creating shell scripts.")
    job_df = htc_utils.write_bash(
        job_df,
        output_dir=opt.output_dir,
        executable=opt.executable,
        cmdline_arguments=opt.script_arguments,
        mask=opt.mask,
    )

    # Convert paths to strings and write df to file ---
    job_df[COLUMN_JOB_DIRECTORY] = job_df[COLUMN_JOB_DIRECTORY].apply(str)
    if COLUMN_DEST_DIRECTORY in job_df.columns:
        job_df[COLUMN_DEST_DIRECTORY] = job_df[COLUMN_DEST_DIRECTORY].apply(str)

    tfs.write(str(opt.working_directory / JOBSUMMARY_FILE), job_df, save_index=COLUMN_JOBID)
    
    # Drop already run jobs ---
    dropped_jobs = []
    if opt.should_drop_jobs():
        job_df, dropped_jobs = _drop_already_run_jobs(
            job_df, opt.output_dir, opt.check_files
        )
    return job_df, dropped_jobs


def create_folders(job_df: tfs.TfsDataFrame, working_directory: Path, 
                   destination_directory: Union[Path, str] = None) -> tfs.TfsDataFrame:
    """Create the folder-structure in the given working directory and the 
    destination directory if given.
    This creates a folder per job in which then the job-scripts and bash-scripts
    can be stored later.

    Args:
        job_df (tfs.TfsDataFrame): DataFrame containing all the job-information
        working_directory (Path): Path to the working directory
        destination_directory (Path, optional): Path to the destination directory, 
        i.e. the directory to copy the outputs to manually. Defaults to None.

    Returns:
        tfs.TfsDataFrame: The job-dataframe again, but with the added paths to the job-dirs.
    """
    LOG.debug("Setting up folders: ")
    
    jobname = f"{JOBDIRECTORY_PREFIX}.{{0}}"
    job_df[COLUMN_JOB_DIRECTORY] = [working_directory / jobname.format(id_) for id_ in job_df.index]

    for job_dir in job_df[COLUMN_JOB_DIRECTORY]:
        job_dir.mkdir(exist_ok=True)
        LOG.debug(f"   created '{job_dir}'.")

    if destination_directory:
        dest_path = uri_to_path(destination_directory)
        dest_path.mkdir(parents=True, exist_ok=True)

        server = get_server_from_uri(destination_directory)
        job_df[COLUMN_DEST_DIRECTORY] = [f"{server}{dest_path / jobname.format(id_)}" for id_ in job_df.index]

        # Make some symlinks for easy navigation---
        # Output directory -> Working Directory
        sym_submission = dest_path / Path('SUBMISSION_DIR')
        sym_submission.unlink(missing_ok=True)
        sym_submission.symlink_to(working_directory.resolve(), target_is_directory=True)

        # Working Directory -> Output Directory
        sym_destination = working_directory / Path('OUTPUT_DIR')
        sym_destination.unlink(missing_ok=True)
        sym_destination.symlink_to(dest_path.resolve(), target_is_directory=True)

        # Create output dirs per job ---
        for job_dest_dir in job_df[COLUMN_DEST_DIRECTORY]:
            uri_to_path(job_dest_dir).mkdir(exist_ok=True)
            LOG.debug(f"   created '{job_dest_dir}'.")

    return job_df


def is_eos_uri(path: Union[Path, str, None]) -> bool:
    """ Check if the given path is an EOS-URI as `eos cp` only works with those.
    E.g.: root://eosuser.cern.ch//eos/user/a/anabramo/banana.txt
    
    This function does not check the double slashes,
    to avoid having the user pass a malformed path by accident and then 
    assuming it is just a path. This is tested for in 
    :func:`pylhc_submitter.job_submitter.check_opts`.
    """
    if path is None:
        return False

    parts = Path(path).parts 
    return (
        len(parts) >= 3  # at least root:, server, path
        and
        parts[0].endswith(':')
        and
        parts[2] == 'eos'
    )


def uri_to_path(path: Union[Path, str]) -> Path:
    """ Strip EOS path information from a path.
    EOS paths for HTCondor can be given as URI. Strip for direct writing.
    E.g.: root://eosuser.cern.ch//eos/user/a/anabramo/banana.txt
    """
    path = Path(path)
    parts = path.parts
    if parts[0].endswith(':'):
        # the first two parts are host info, e.g `file: //host/path`
        return Path('/', *parts[2:])
    return path 


def get_server_from_uri(path: Union[Path, str]) -> str:
    """ Get server information from a path.
    E.g.: root://eosuser.cern.ch//eos/user/a/ -> root://eosuser.cern.ch/
    """
    path_part = uri_to_path(path)
    if path_part == Path(path):
        return ""
    
    server_part = str(path).replace(str(path_part), '')
    if server_part.endswith("//"):
        server_part = server_part[:-1]
    return server_part


def print_stats(new_jobs: JobNamesType, finished_jobs: JobNamesType):
    """Print some quick statistics."""
    text = [
        "\n------------- QUICK STATS ----------------"
        f"Jobs total:{len(new_jobs) + len(finished_jobs):d}",
        f"Jobs to run: {len(new_jobs):d}",
        f"Jobs already finished: {len(finished_jobs):d}",
        "---------- JOBS TO RUN: NAMES -------------"
    ]
    text += [str(job_name) for job_name in new_jobs]
    text += ["--------- JOBS FINISHED: NAMES ------------"]
    text += [str(job_name) for job_name in finished_jobs]
    LOG.info("\n".join(text))


def _generate_parameter_space(
        replace_dict: Dict[str, Any], append_jobs: bool, cwd: Path
    ) -> Tuple[List[str], np.ndarray, tfs.TfsDataFrame]:
    """ Generate parameter space from replace-dict, check for existing jobs. """
    LOG.debug("Generating parameter space from replace-dict.")
    parameters = list(replace_dict.keys())
    values_grid = _generate_values_grid(replace_dict)
    if not append_jobs:
        return parameters, values_grid, tfs.TfsDataFrame()

    jobfile_path = cwd / JOBSUMMARY_FILE
    try:
        prev_job_df = tfs.read(str(jobfile_path.absolute()), index=COLUMN_JOBID)
    except FileNotFoundError as filerror:
        raise FileNotFoundError(
            "Cannot append jobs, as no previous jobfile was found at " f"'{jobfile_path}'"
        ) from filerror
    new_jobs_mask = [elem not in prev_job_df[parameters].values for elem in values_grid]
    values_grid = values_grid[new_jobs_mask]

    return parameters, values_grid, prev_job_df


def _generate_values_grid(replace_dict: Dict[str, Any]) -> np.ndarray:
    """ Creates an array of the inner-product of the replace-dict. """
    return np.array(list(itertools.product(*replace_dict.values())), dtype=object)


def _drop_already_run_jobs(
        job_df: tfs.TfsDataFrame, output_dir: str, check_files: str
    ) -> Tuple[tfs.TfsDataFrame, List[str]]:
    """ Check for jobs that have already been run and drop them from current job_df. """
    LOG.debug("Dropping already finished jobs.")
    finished_jobs = [
        idx
        for idx, row in job_df.iterrows()
        if _job_was_successful(row, output_dir, check_files)
    ]

    LOG.info(
        f"{len(finished_jobs):d} of {len(job_df.index):d}"
        " Jobs have already finished and will be skipped."
    )

    job_df = job_df.drop(index=finished_jobs)
    return job_df, finished_jobs


def _job_was_successful(job_row: pd.Series, output_dir: str, files: Sequence[str]) -> bool:
    """ Determines if the job was successful. 
    
    Args:
        job_row (pd.Series): row from the job_df
        output_dir (str): Name of the (local) output directory
        files (List[str]): list of files that should have been generated
    """
    job_dir = job_row.get(COLUMN_DEST_DIRECTORY) or job_row[COLUMN_JOB_DIRECTORY]
    output_dir = Path(job_dir, output_dir)
    success = output_dir.is_dir() and any(output_dir.iterdir())
    if success and files is not None and len(files):
        for f in files:
            success &= len(list(output_dir.glob(f))) > 0
    return success


def _get_script_extension(script_extension: str, executable: Path, mask: Path) -> str:
    """ Returns the extension of the script to run based on 
    either the given value, its executable or the mask. """
    if script_extension is not None:
        return script_extension
    return SCRIPT_EXTENSIONS.get(executable, mask.suffix)
