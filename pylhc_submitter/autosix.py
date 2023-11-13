"""
AutoSix
-------

``AutoSix`` is a wrapper to automatically perform the necessary setup and steps needed for ``SixDesk`` use.

The functionality is similar to the ``pylhc_submitter.job_submitter`` in that the inner product of a ``replace_dict``
is used to automatically create a set of job-directories to gather the data.
To avoid conflicts, each of these job-directories is a ``SixDesk`` workspace,
meaning there can be only one study per directory.
Beware that the ``max_materialize`` limit is set for each of these workspaces
individually, not for all Jobs together (i.e. it should be <=MAX_USER_JOBS / NUMBER_OF_WORKSPACES).

The ``replace_dict`` contains variables for your mask as well as variables for the SixDesk environment.
See the description of ``replace_dict`` below.

In any other way, these *special* variables behave like normal variables and can also be inserted in your mask.
They are also looped over in the same manner as any other variable (if given as a list).

For additional information and guides, see the `AutoSix page
<https://pylhc.github.io/packages/pylhcsubmitter/autosix/>`_
on the ``OMC`` documentation site.

.. admonition:: Problems with *search.f90*
    :class: warning

    If you run into

    .. code-block:: bash

       ImportError: cannot import name 'search' from partially initialized module 'sixdeskdb'

    that means that the :file:`SixDesk/utilities/externals/SixDeskDB/sixdeskdb/search.f90`
    has not been compiled for your current python and OS version (indicated by XXX below).
    There are two ways to do this:

    **a)** Run

    .. code-block:: bash

        python -m numpy.f2py -c search.f90 -f search

    with your desired python version
    (a shortcut ``f2py`` might be available if you run in an activated venv).
    Copy (or symlink) the resulting :file:`search.cpython-XXX.so` file into the
    :file:`sixdeskdb` folder
    (if it is not already there because you ran from that folder)

    **b)** Run

    .. code-block:: bash

        python setup.py build_ext

    on the :file:`setup.py` in :file:`SixDesk/utilities/externals/SixDeskDB`.
    Then copy (or symlink) the resulting :file:`build/lib.XXX/sixdeskdb/search.cpython-XXX.so` into
    the :file:`sixdeskdb` folder (the name needs to stay as it is).


Arguments:

*--Required--*

- **mask** *(Path)*:

    Path to the program's mask to use.


- **replace_dict** *(DictAsString)*:

    Dict with keys of the strings to be replaced in the mask (required) as
    well as the mask_sixdeskenv and mask_sysenv files in the sixdesk_tools
    module. Required fields are TURNS, AMPMIN, AMPMAX, AMPSTEP, ANGLES.
    Optional fields are RESUBMISSION, PLATFORM, LOGLEVEL, FIRSTSEED,
    LASTSEED, RUNTYPE, NPAIRS, EMITTANCE, DIMENSIONS, WRITEBINS, ENERGY.
    These keys can also be used in the mask if needed. The values of this
    dict are lists of values to replace these or single entries.


- **working_directory** *(Path)*:

    Directory where data should be put into.


*--Optional--*

- **apply_mad6t_hacks**:

    Apply two hacks: Removes '<' in binary call and ignore the check for
    'Twiss fail' in the submission file. This is hack needed in case this
    check greps the wrong lines, e.g. in madx-comments. USE WITH CARE!!

    action: ``store_true``


- **da_turnstep** *(int)*:

    Step between turns used in DA-vs-Turns plot.

    default: ``100``


- **executable** *(PathOrStr)*:

    Path to executable or 'madx', 'python2', 'python3' to use the OMC
    default paths on AFS.Defaults to the latest MADX-Binary on AFS.

    default: ``/afs/cern.ch/user/m/mad/bin/madx``


- **jobid_mask** *(str)*:

    Mask-String to name jobs, with placeholders from the ``replace_dict``
    keys.


- **max_materialize** *(int)*:

    Maximum jobs to be materialized in scheduler (per SixDesk Workspace!)..
    Here: ``None`` leaves the settings as defined in the SixDesk
    htcondor_run_six.sub template and ``0`` removes it from the template.
    Warning: This setting modifies the template in the ``sixdesk_directory``
    permanently. For more details see the htcondor API.


- **max_stage** *(str)*:

    Last stage to be run. All following stages are skipped.


- **python2** *(PathOrStr)*:

    Path to python to use with run_six.sh (python2 with requirements
    installed). ONLY THE PATH TO THE DIRECTORY OF THE python BINARY IS
    NEEDED! And it can't be an Anaconda Distribution. If ``None`` the
    system's ``python`` is used (SixDesk internally).

    default: ``None``


- **python3** *(PathOrStr)*:

    Path to python to use with sixdb (python3 with requirements
    installed).Defaults to the system's ``python3``.

    default: ``python3``


- **resubmit**:

    Resubmits to HTCondor if needed (i.e. in case it finds errors with the
    previous run).

    action: ``store_true``


- **sixdesk_directory** *(Path)*:

    Path to the directory of SixDesk. Defaults to the PRO-version on AFS.
    If you are using your own SixDesk Environment and it does not run,
    check the AutoSix doc.

    default: ``/afs/cern.ch/project/sixtrack/SixDesk_utilities/pro``


- **ssh** *(str)*:

    Run htcondor from this machine via ssh (needs access to the
    ``working_directory``)


- **stop_workspace_init**:

    Stops the workspace creation before initialization, so one can make
    manual changes.

    action: ``store_true``


- **unlock**:

    Forces unlocking of folders (if they have been locked by Sixdesk).

    action: ``store_true``


:author: jdilly

"""
import itertools
import logging
from pathlib import Path

import numpy as np
import tfs
from generic_parser import EntryPointParameters, entrypoint
from generic_parser.entry_datatypes import DictAsString

from pylhc_submitter.constants.autosix import (HEADER_BASEDIR, SIXENV_OPTIONAL, SIXENV_REQUIRED,
                                               AutoSixEnvironment)
from pylhc_submitter.constants.job_submitter import COLUMN_JOBID, JOBSUMMARY_FILE
from pylhc_submitter.submitter.mask import generate_jobdf_index
from pylhc_submitter.sixdesk_tools.stages import STAGE_ORDER, Stage
from pylhc_submitter.sixdesk_tools.utils import check_mask, is_locked
from pylhc_submitter.utils.iotools import (PathOrStr, keys_to_path, make_replace_entries_iterable,
                                           save_config)
from pylhc_submitter.utils.logging_tools import log_setup

LOG = logging.getLogger(__name__)


def get_params():
    params = EntryPointParameters()
    params.add_parameter(
        name="mask",
        type=Path,
        required=True,
        help="Path to the program's mask to use.",
    )
    params.add_parameter(
        name="working_directory",
        type=Path,
        required=True,
        help="Directory where data should be put into.",
    )
    params.add_parameter(
        name="replace_dict",
        help=(
            "Dict with keys of the strings to be replaced in the mask (required) "
            "as well as the mask_sixdeskenv and mask_sysenv files "
            "in the sixdesk_tools module. "
            f"Required fields are {', '.join(SIXENV_REQUIRED)}. "
            f"Optional fields are {', '.join(SIXENV_OPTIONAL)}. "
            "These keys can also be used in the mask if needed. "
            "The values of this dict are lists of values to replace "
            "these or single entries."
        ),
        type=DictAsString,
        required=True,
    )
    params.add_parameter(
        name="sixdesk_directory",
        type=Path,
        default=AutoSixEnvironment.sixdesk_directory,
        help="Path to the directory of SixDesk. Defaults to the PRO-version on AFS."
             " If you are using your own SixDesk Environment and it does not run, "
             " check the AutoSix doc.",
    )
    params.add_parameter(
        name="executable",
        default=AutoSixEnvironment.executable,
        type=PathOrStr,
        help="Path to executable or 'madx', 'python2', 'python3' "
             "to use the OMC default paths on AFS."
             "Defaults to the latest MADX-Binary on AFS.",
    )
    params.add_parameter(
        name="python2",
        default=AutoSixEnvironment.python2,
        type=PathOrStr,
        help=("Path to python to use with run_six.sh (python2 with requirements installed)."
              " ONLY THE PATH TO THE DIRECTORY OF THE python BINARY IS NEEDED!"
              " And it can't be an Anaconda Distribution."
              " If ``None`` the system's ``python`` is used (SixDesk internally)."),
    )
    params.add_parameter(
        name="python3",
        default=AutoSixEnvironment.python3,
        type=PathOrStr,
        help="Path to python to use with sixdb (python3 with requirements installed)."
             "Defaults to the system's ``python3``.",
    )
    params.add_parameter(
        name="jobid_mask",
        help="Mask-String to name jobs, with placeholders from the ``replace_dict`` keys.",
        type=str,
    )
    params.add_parameter(
        name="ssh",
        help="Run htcondor from this machine via ssh (needs access to the ``working_directory``)",
        type=str,
    )
    params.add_parameter(
        name="unlock",
        help="Forces unlocking of folders (if they have been locked by Sixdesk).",
        action="store_true",
    )
    params.add_parameter(
        name="apply_mad6t_hacks",
        help=(
            "Apply two hacks: Removes '<' in binary call and "
            "ignore the check for 'Twiss fail' in the submission file. "
            "This is hack needed in case this check greps the wrong lines, "
            "e.g. in madx-comments. USE WITH CARE!!"
        ),
        action="store_true",
    )
    params.add_parameter(
        name="stop_workspace_init",
        help=(
            "Stops the workspace creation before initialization,"
            " so one can make manual changes."
        ),
        action="store_true",
    )
    params.add_parameter(
        name="resubmit",
        help="Resubmits to HTCondor if needed "
             "(i.e. in case it finds errors with the previous run).",
        action="store_true",
    )
    params.add_parameter(
        name="da_turnstep",
        type=int,
        help="Step between turns used in DA-vs-Turns plot.",
        default=AutoSixEnvironment.da_turnstep,
    )
    params.add_parameter(
        name="max_stage",
        type=str,
        help="Last stage to be run. All following stages are skipped.",
    )
    params.add_parameter(
        name="max_materialize",
        type=int,
        help="Maximum jobs to be materialized in scheduler (per SixDesk Workspace!). "
             "Here: ``None`` leaves the settings as defined in the SixDesk "
             "htcondor_run_six.sub template and ``0`` removes it from the "
             "template. Warning: This setting modifies the template in the "
             "``sixdesk_directory`` permanently. For more details see the "
             "htcondor API.",
    )
    return params


@entrypoint(get_params(), strict=True)
def main(opt):
    """ Loop to create jobs from replace dict product matrix. """
    LOG.info("Starting autosix.")
    opt = _check_opts(opt)
    save_config(opt.working_directory, opt, "autosix")

    jobdf = _generate_jobs(
        opt.working_directory,
        opt.pop('jobid_mask'),  # not needed anymore
        **opt.pop('replace_dict')  # not needed anymore
    )
    env = AutoSixEnvironment(**opt)  # basically checks that everything is there

    for jobname, jobargs in jobdf.iterrows():
        run_job(jobname=jobname, jobargs=jobargs, env=env)


def run_job(jobname: str, jobargs: dict, env: AutoSixEnvironment):
    """Main submitting procedure for single job.

    Args:
        jobname (str): Name of the job/study
        env (DotDict): The ensemble of autosix settings as an ``AutoSixEnvironment`` object.
        jobargs(dict): All Key=Values needed to fill the mask!
    """
    if is_locked(jobname, env.working_directory, unlock=env.unlock):
        LOG.info(f"{jobname} is locked. Try 'unlock' flag if this causes errors.")

    Stage.run_all_stages(jobname, jobargs, env)


# Helper  ----------------------------------------------------------------------


def _check_opts(opt):
    opt = keys_to_path(opt, "mask", "working_directory", "executable")

    opt.mask_text = opt.mask.read_text()
    check_mask(opt.mask_text, opt.replace_dict)
    del opt.mask

    opt.replace_dict = make_replace_entries_iterable(opt.replace_dict)
    if opt.max_stage is not None and not isinstance(opt.max_stage, Stage):
        opt.max_stage = STAGE_ORDER[opt.max_stage]
    return opt


def get_jobs_and_values(jobid_mask, **kwargs):
    values_grid = np.array(list(itertools.product(*kwargs.values())), dtype=object)
    job_names = generate_jobdf_index(None, jobid_mask, kwargs.keys(), values_grid)
    return job_names, values_grid


def _generate_jobs(basedir, jobid_mask, **kwargs) -> tfs.TfsDataFrame:
    """ Generates product matrix for job-values and stores it as TfsDataFrame. """
    LOG.debug("Creating Jobs")
    job_names, values_grid = get_jobs_and_values(jobid_mask, **kwargs)
    job_df = tfs.TfsDataFrame(
        headers={HEADER_BASEDIR: basedir},
        index=job_names,
        columns=list(kwargs.keys()),
        data=values_grid,
    )
    tfs.write(basedir / JOBSUMMARY_FILE, job_df, save_index=COLUMN_JOBID)
    return job_df


if __name__ == "__main__":
    log_setup()
    main()
