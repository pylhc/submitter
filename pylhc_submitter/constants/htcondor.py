"""
HTCondor
--------

Constants for the HTCondor parameters.
"""
SHEBANG = "#!/bin/bash"
SUBFILE = "queuehtc.sub"
BASH_FILENAME = "Job"

HTCONDOR_JOBLIMIT = 100000

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
