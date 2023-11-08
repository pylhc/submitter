"""
Mask Resolver
-------------

This module provides functionality to resolve and write script masks for ``HTCondor`` jobs
submission.
"""
import logging
import re
from pathlib import Path
from typing import Iterable, List, Sequence, Set, Union

import pandas as pd
from numpy.typing import ArrayLike

from pylhc_submitter.constants.job_submitter import COLUMN_JOB_DIRECTORY, COLUMN_JOB_FILE

LOG = logging.getLogger(__name__)


def create_job_scripts_from_mask(
    job_df: pd.DataFrame, maskfile: Path, replace_keys: dict, file_ext: str
) -> pd.DataFrame:
    """
    Takes path to mask file, list of parameter to be replaced and pandas dataframe containg per job
    the job directory where processed mask is to be put, and columns containing the parameter values
    with column named like replace parameters. Job directories have to be created beforehand.
    Processed (madx) mask has the same filename as mask but with the given file extension.
    Input Dataframe is returned with additional column containing path to the processed script
    files.

    Args:
        job_df (pd.DataFrame): Job parameters as defined in description.
        maskfile: `Path` object to the mask file.
        replace_keys: keys to be replaced (must correspond to columns in ``job_df``).
        file_ext: file extention to use (defaults to **madx**).

    Returns:
        The provided ``job_df`` but with added path to the scripts.
    """
    with maskfile.open("r") as mfile:
        template = mfile.read()

    jobname = maskfile.with_suffix("").name
    jobs = [None] * len(job_df)
    for idx, (jobid, values) in enumerate(job_df.iterrows()):
        jobfile_fullpath = (Path(values[COLUMN_JOB_DIRECTORY]) / jobname).with_suffix(file_ext)

        with jobfile_fullpath.open("w") as job_file:
            job_file.write(template % dict(zip(replace_keys, values[list(replace_keys)])))
        jobs[idx] = jobfile_fullpath.name
    job_df[COLUMN_JOB_FILE] = jobs
    return job_df


def find_named_variables_in_mask(mask: str) -> Set[str]:
    """ Find all variable-names in the mask. """
    return set(re.findall(r"%\((\w+)\)", mask))


def check_percentage_signs_in_mask(mask: str) -> None:
    """ Checks for '%' in the mask, that are not replacement variables. """
    cleaned_mask = re.sub(r"%\((\w+)\)", "", mask)
    n_signs = cleaned_mask.count("%")
    if n_signs == 0:
        return

    # Help the user find the %
    for idx, line in enumerate(cleaned_mask.split("\n")):
        if "%" in line:
            positions = [str(i) for i, char in enumerate(line) if char == "%"]
            LOG.error(f"Problematic '%' sign(s) in line {idx}, pos {' ,'.join(positions)}.")
    raise KeyError(f"{n_signs} problematic '%' signs found in template. Please remove.")


def generate_jobdf_index(old_df: pd.DataFrame, jobid_mask: str, keys: Sequence[str], values: ArrayLike
    ) -> Union[List[str], Iterable[int]]:
    """ Generates index for jobdf from mask for job_id naming. 
    
    Args:
        old_df (pd.DataFrame): Existing jobdf.
        jobid_mask (str): Mask for naming the jobs.
        keys (Sequence[str]): Keys to be replaced in the mask.
        values (np.array_like): Values-Grid to be replaced in the mask.
    
    Returns:
        List[str]: Index for jobdf, either list of strings (the filled jobid_masks) or integer-range.
    """
    if not jobid_mask:
        # Use integer-range as index, if no mask is given
        # Start with last index if old_df is not None.
        nold = len(old_df.index) if old_df is not None else 0
        start = nold-1 if nold > 0 else 0
        return range(start, start + values.shape[0])

    # Fill job-id mask
    return [jobid_mask % dict(zip(keys, v)) for v in values]


def is_mask_file(mask: str) -> bool:
    """ Check if given string points to a file. """
    try:
        return Path(mask).is_file()
    except OSError:
        return False


def is_mask_string(mask: str) -> bool:
    """ Checks that given string does not point to a file. """
    return not is_mask_file(mask)


if __name__ == "__main__":
    raise EnvironmentError(f"{__file__} is not supposed to run as main.")
