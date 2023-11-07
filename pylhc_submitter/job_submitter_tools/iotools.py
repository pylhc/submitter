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
import tfs

from pylhc_submitter.constants.job_submitter import (COLUMN_DEST_DIRECTORY, COLUMN_JOB_DIRECTORY,
                                                     COLUMN_JOBID, JOBDIRECTORY_PREFIX,
                                                     JOBSUMMARY_FILE, SCRIPT_EXTENSIONS)
from pylhc_submitter.job_submitter_tools import htc_utils
from pylhc_submitter.job_submitter_tools.mask import (create_job_scripts_from_mask,
                                                      generate_jobdf_index, is_mask_file)

LOG = logging.getLogger(__name__)


@dataclass
class CreationOpts:
    working_directory: Path
    mask: Union[Path, str]
    jobid_mask: str
    replace_dict: Dict[str, Any]
    output_dir: Path
    output_destination: Path
    append_jobs: bool
    resume_jobs: bool
    executable: str
    check_files: Sequence[str]
    script_arguments: Dict[str, Any]
    script_extension: str

    def should_drop_jobs(self) -> bool:
        return self.append_jobs or self.resume_jobs



def create_jobs(opt: CreationOpts) -> tfs.TfsDataFrame:
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

    if njobs > htc_utils.HTCONDOR_JOBLIMIT:
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
                   destination_directory: Path = None) -> tfs.TfsDataFrame:
    LOG.debug("Setting up folders: ")
    
    jobname = f"{JOBDIRECTORY_PREFIX}.{{0}}"
    job_df[COLUMN_JOB_DIRECTORY] = [working_directory / jobname.format(id_) for id_ in job_df.index]

    for job_dir in job_df[COLUMN_JOB_DIRECTORY]:
        job_dir.mkdir(exist_ok=True)
        LOG.debug(f"   created '{job_dir}'.")

    if destination_directory:
        job_df[COLUMN_DEST_DIRECTORY] = [destination_directory / jobname.format(id_) for id_ in job_df.index]

        strip_dest_dir = strip_eos_uri(destination_directory)
        strip_dest_dir.mkdir(parents=True, exist_ok=True)

        # Make some symlinks for easy navigation---
        # Output directory -> Working Directory
        sym_submission = destination_directory / Path('SUBMISSION_DIR')
        sym_submission.symlink_to(working_directory.resolve(), target_is_directory=True)

        # Working Directory -> Output Directory
        sym_destination = working_directory / Path('OUTPUT_DIR')
        sym_destination.symlink_to(destination_directory.resolve(), target_is_directory=True)

        # Create output dirs per job ---
        for job_dest_dir in job_df[COLUMN_DEST_DIRECTORY]:
            strip_eos_uri(job_dest_dir).mkdir(exist_ok=True)
            LOG.debug(f"   created '{job_dest_dir}'.")

    return job_df


def is_eos_path(path: Union[Path, str]) -> bool:
    """ Check if the given path leads to EOS."""
    strip_path_parts = strip_eos_uri(path).parts 
    return len(strip_path_parts) > 1 and strip_path_parts[1] == 'eos'


def strip_eos_uri(path: Union[Path, str]) -> Path:
    # EOS paths for HTCondor can be given as URI. Strip for direct writing.
    # E.g.: root://eosuser.cern.ch//eos/user/a/anabramo/banana.txt
    path = Path(path)
    parts = path.parts
    outpath = path
    if parts[0].endswith(':'):
        # the first two parts are host info, e.g `file: //host/path`
        outpath = Path('/', *parts[2:])
    return outpath


def print_stats(new_jobs, finished_jobs):
    """Print some quick statistics."""
    text = [
        "\n------------- QUICK STATS ----------------"
        f"Jobs total:{len(new_jobs) + len(finished_jobs):d}",
        f"Jobs to run: {len(new_jobs):d}",
        f"Jobs already finished: {len(finished_jobs):d}",
        "---------- JOBS TO RUN: NAMES -------------"
    ]
    for job_name in new_jobs:
        text.append(job_name)
    text += ["--------- JOBS FINISHED: NAMES ------------"]
    for job_name in finished_jobs:
        text.append(job_name)
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


def _job_was_successful(job_row, output_dir, files) -> bool:
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
