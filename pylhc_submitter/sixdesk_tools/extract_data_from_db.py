"""
Extract Data From DataBase
-----------------------------

These functions operate on the SixDesk database and help to extract data
form it.

TODO: Implement extraction of data into ``.csv`` (and/or tfs?)
like fvanderv does.
"""
import logging
import sqlite3 as sql
from pathlib import Path
from typing import Any, Tuple, Iterable
from contextlib import contextmanager

import numpy as np
import pandas as pd
from generic_parser import DotDict
from matplotlib import pyplot as plt
from matplotlib import rcParams, lines as mlines
from scipy.interpolate import interp1d
from tfs import TfsDataFrame, write_tfs

from pylhc_submitter.constants.autosix import (
    get_database_path,
    get_tfs_da_path,
    get_tfs_da_seed_stats_path,
    get_tfs_da_angle_stats_path,
    get_autosix_results_path,
    HEADER_NTOTAL,
    HEADER_INFO,
    HEADER_HINT,
    MEAN,
    STD,
    MIN,
    MAX,
    N,
    SEED,
    ANGLE,
    ALOST1,
    ALOST2,
    AMP,
)

LOG = logging.getLogger(__name__)


def extract_da_data(jobname: str, basedir: Path) -> TfsDataFrame:
    """ Extract DA data directly from the database.

    Args:
        jobname (str): Name of the Job
        basedir (Path): SixDesk Basefolder Location
    """
    with _get_database(jobname, basedir) as db:
        df_da = pd.read_sql(
            "SELECT seed, angle, alost1, alost2, Amin, Amax FROM da_post ORDER BY seed, angle", db
        )

    df_da = df_da.rename(
        columns={
            "seed": SEED,
            "angle": ANGLE,
            "alost1": ALOST1,
            "alost2": ALOST2,
            "Amin": f"{MIN}{AMP}",
            "Amax": f"{MAX}{AMP}",
        }
    )
    return TfsDataFrame(df_da)


def extract_meta_data(jobname: str, basedir: Path) -> TfsDataFrame:
    """ Extract the meta-data directly from the database.

    Args:
        jobname (str): Name of the Job
        basedir (Path): SixDesk Basefolder Location
    """
    with _get_database(jobname, basedir) as db:
        df_da = pd.read_sql(
            "SELECT keyname, value FROM env", db
        )
    return TfsDataFrame(df_da)


@contextmanager
def _get_database(jobname, basedir):
    """Context to connect to DataBase and always close the connection afterwards.

    Args:
        jobname (str): Name of the Job
        basedir (Path): SixDesk Basefolder Location
    """
    db_path = get_database_path(jobname, basedir)
    db = sql.connect(db_path)
    yield db
    db.close()
