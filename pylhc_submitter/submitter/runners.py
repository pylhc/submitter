""" 
Job Submitter Runners
---------------------

Defines the methods to run the job-submitter, locally or on HTC.
"""
import logging
import multiprocessing
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import tfs

from pylhc_submitter.constants.job_submitter import (COLUMN_DEST_DIRECTORY, COLUMN_JOB_DIRECTORY,
                                                     COLUMN_SHELL_SCRIPT)
from pylhc_submitter.submitter import htc_utils
from pylhc_submitter.submitter.iotools import is_eos_uri
from pylhc_submitter.utils.environment import on_windows

LOG = logging.getLogger(__name__)


@dataclass
class RunnerOpts:
    """ Options for running the submission. """
    working_directory: Path           # Path to the working directory (e.g. afs)
    jobflavour: Optional[str] = None  # HTCondor job flavour (lengths of the job)
    output_dir: Optional[str] = None  # Name of the output directory, where jobs store data
    ssh: Optional[str] = None         # SSH command
    dryrun: Optional[bool] = False    # Perform only a dry-run, i.e. do all but submit to HTC
    htc_arguments: Optional[Dict[str, Any]] = field(default_factory=dict)  # Arguments to pass on to htc as keywords
    run_local: Optional[bool] = False # Run jobs locally
    num_processes: Optional[int] = 4  # Number of processes to run in parallel (locally)


def run_jobs(job_df: tfs.TfsDataFrame, opt: RunnerOpts) -> None:
    """Selects how to run the jobs.
    
    Args:
        job_df (tfs.TfsDataFrame): DataFrame containing all the job-information 
        opt (RunnerOpts): Parameters for the runner 
    """
    if opt.run_local: 
        run_local(job_df, opt)
    else:
        run_htc(job_df, opt)


def run_local(job_df: tfs.TfsDataFrame, opt: RunnerOpts) -> None:
    """Run all jobs locally.

    Args:
        job_df (tfs.TfsDataFrame): DataFrame containing all the job-information 
        opt (RunnerOpts): Parameters for the runner 
    """
    if opt.dryrun:
        LOG.info(f"Dry-run: Skipping local run.")
        return

    LOG.info(f"Running {len(job_df.index)} jobs locally in {opt.num_processes:d} processes.")
        
    pool = multiprocessing.Pool(processes=opt.num_processes)
    res = pool.map(_execute_shell, job_df.iterrows())
    if any(res):
        jobs_failed = [j for r, j in zip(res, job_df.index) if r]
        LOG.error(f"{len(jobs_failed)} of {len(job_df)} jobs have failed:\n {jobs_failed}")
        raise RuntimeError("At least one job has failed. Check output logs!")


def run_htc(job_df: tfs.TfsDataFrame, opt: RunnerOpts) -> None:
    """ Create submission file and submit the jobs to ``HTCondor``.

    Args:
        job_df (tfs.TfsDataFrame): DataFrame containing all the job-information
        opt (RunnerOpts): Parameters for the runner 
    """
    LOG.info(f"Submitting {len(job_df.index)} jobs on htcondor, flavour '{opt.jobflavour}'.")
    LOG.debug("Creating htcondor subfile.")

    subfile = htc_utils.make_subfile(
        opt.working_directory, job_df, 
        output_dir=opt.output_dir, 
        jobflavour=opt.jobflavour, 
        **opt.htc_arguments
    )

    if opt.dryrun:
        LOG.info("Dry run: submission file created, but not submitting jobs to htcondor.")
        return

    LOG.debug("Submitting jobs to htcondor.")
    htc_utils.submit_jobfile(subfile, opt.ssh)


# Helper #######################################################################

def _execute_shell(df_row: Tuple[Any, pd.Series]) -> int:
    """ Execute the shell script. 
    
    Args:
        df_row (Tuple[Any, pd.Series]): Row in the job-dataframe as coming from `iterrows()`, 
                                        i.e. a tuple of (index, series)
    
    Returns:
        int: return code of the process
    """
    _, column = df_row
    cmd = [] if on_windows() else ["sh"]

    with Path(column[COLUMN_JOB_DIRECTORY], "log.tmp").open("w") as logfile:
        process = subprocess.Popen(
            cmd + [column[COLUMN_SHELL_SCRIPT]],
            shell=on_windows(),
            stdout=logfile,
            stderr=subprocess.STDOUT,
            cwd=column[COLUMN_JOB_DIRECTORY],
        )
    return process.wait()
