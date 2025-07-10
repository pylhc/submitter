"""
Extract Data From DataBase
-----------------------------

These functions operate on the SixDesk database and help to extract data
form it.

TODO: Implement extraction of data into ``.csv`` (and/or tfs?)
like fvanderv does.
"""

from __future__ import annotations

import logging
import sqlite3 as sql
from contextlib import contextmanager
from typing import TYPE_CHECKING

import pandas as pd
from tfs import TfsDataFrame

from pylhc_submitter.constants.autosix import (
    ALOST1,
    ALOST2,
    AMP,
    ANGLE,
    MAX,
    MIN,
    SEED,
    get_database_path,
)

if TYPE_CHECKING:
    from pathlib import Path

LOG = logging.getLogger(__name__)


def extract_da_data(jobname: str, basedir: Path) -> TfsDataFrame:
    """Extract DA data directly from the database.

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
    """Extract the meta-data directly from the database.

    Args:
        jobname (str): Name of the Job
        basedir (Path): SixDesk Basefolder Location
    """
    with _get_database(jobname, basedir) as db:
        df_da = pd.read_sql("SELECT keyname, value FROM env", db)
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
