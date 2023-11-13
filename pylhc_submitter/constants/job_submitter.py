
"""
Job Submitter
-------------

Collections of constants and paths used in the job-submitter.
"""
from pylhc_submitter.constants.external_paths import MADX_BIN, PYTHON2_BIN, PYTHON3_BIN

JOBSUMMARY_FILE = "Jobs.tfs"
JOBDIRECTORY_PREFIX = "Job"
CONFIG_FILE = "config.ini"

SCRIPT_EXTENSIONS = {
    "madx": ".madx",
    "python3": ".py",
    "python2": ".py",
}

EXECUTEABLEPATH = {
    "madx": MADX_BIN,
    "python3": PYTHON3_BIN,
    "python2": PYTHON2_BIN,
}


COLUMN_JOBID = "JobId"
COLUMN_SHELL_SCRIPT = "ShellScript"
COLUMN_JOB_DIRECTORY = "JobDirectory"
COLUMN_DEST_DIRECTORY = "DestDirectory"
COLUMN_JOB_FILE = "JobFile"

NON_PARAMETER_COLUMNS = (COLUMN_SHELL_SCRIPT, COLUMN_JOB_DIRECTORY, COLUMN_JOB_FILE, COLUMN_DEST_DIRECTORY)