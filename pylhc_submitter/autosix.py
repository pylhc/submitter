"""
AutoSix
-------

``AutoSix`` is a wrapper to automatically perform the necessary setup and steps needed for ``SixDesk`` use.

The functionality is similar to the ``pylhc_submitter.job_submitter`` in that the inner product of a ``replace_dict``
is used to automatically create a set of job-directories to gather the data.
To avoid conflicts, each of these job-directories is a ``SixDesk`` workspace,
meaning there can be only one study per directory.

The ``replace_dict`` contains variables for your mask as well as variables for the SixDesk environment.
See the description of ``replace_dict`` below.

In any other way, these *special* variables behave like normal variables and can also be inserted in your mask.
They are also looped over in the same manner as any other variable (if given as a list).

For additional information and guides, see the `AutoSix page
<https://pylhc.github.io/packages/pylhcsubmitter/autosix/>`_ in the ``OMC`` documentation site.

Arguments:

*--Required--*

- **mask** *(PathOrStr)*:

    Program mask to use


- **replace_dict** *(DictAsString)*:

    Dict with keys of the strings to be replaced in the mask (required) as
    well as the mask_sixdeskenv and mask_sysenv files in the sixdesk_tools
    module. Required fields are TURNS, AMPMIN, AMPMAX, AMPSTEP, ANGLES.
    Optional fields are RESUBMISSION, PLATFORM, LOGLEVEL, FIRSTSEED,
    LASTSEED, ENERGY, NPAIRS, EMITTANCE, DIMENSIONS, WRITEBINS. These keys
    can also be used in the mask if needed. The values of this dict are
    lists of values to replace these or single entries.


- **working_directory** *(PathOrStr)*:

    Directory where data should be put


*--Optional--*

- **apply_mad6t_hacks**:

    Apply two hacks: Removes '<' in binary call andignore the check for
    'Twiss fail' in the submission file. This is hack needed in case this
    check greps the wrong lines, e.g. in madx-comments. USE WITH CARE!!

    action: ``store_true``


- **da_turnstep** *(int)*:

    Step between turns used in DA-vs-Turns plot.

    default: ``100``


- **executable** *(PathOrStr)*:

    Path to executable.

    default: ``/afs/cern.ch/user/m/mad/bin/madx``


- **jobid_mask** *(str)*:

    Mask to name jobs from replace_dict


- **max_materialize** *(int)*:

    Maximum jobs to be materialized in scheduler. Here: ``None`` leaves the
    settings as defined in the SixDesk htcondor_run_six.sub template and
    ``0`` removes it from the template. Warning: This setting modifies the
     template in the ``sixdesk_directory`` permanently.
     For more details htcondor API.


- **max_stage** *(str)*:

    Last stage to be run. All following stages are skipped.


- **python2** *(PathOrStr)*:

    Path to python to use with run_six.sh (python2 with requirements
    installed). ONLY THE PATH TO THE DIRECTORY OF THE python BINARY IS
    NEEDED! And it can't be an Anaconda Distribution.

    default: ``None``


- **python3** *(PathOrStr)*:

    Path to python to use with sixdb (python3 with requirements
    installed).

    default: ``python3``


- **resubmit**:

    Resubmits if needed.

    action: ``store_true``


- **ssh** *(str)*:

    Run htcondor from this machine via ssh (needs access to the
    ``working_directory``)


- **stop_workspace_init**:

    Stops the workspace creation before initialization, so one can make
    manual changes.

    action: ``store_true``


- **unlock**:

    Forces unlocking of folders.

    action: ``store_true``



:author: jdilly

"""
import itertools
import logging
from pathlib import Path

import numpy as np
import tfs
from generic_parser import EntryPointParameters, entrypoint, DotDict
from generic_parser.entry_datatypes import DictAsString

from pylhc_submitter.constants.autosix import (
    HEADER_BASEDIR,
    SIXENV_REQUIRED,
    SIXENV_OPTIONAL,
    AutoSixEnvironment,
)
from pylhc_submitter.htc.mask import generate_jobdf_index
from pylhc_submitter.job_submitter import (
    JOBSUMMARY_FILE,
    COLUMN_JOBID,
)
from pylhc_submitter.sixdesk_tools.create_workspace import (
    set_max_materialize
)
from pylhc_submitter.sixdesk_tools.stages import Stage, STAGE_ORDER
from pylhc_submitter.sixdesk_tools.utils import (
    is_locked,
    check_mask,
)
from pylhc_submitter.utils.iotools import (
    PathOrStr,
    save_config,
    make_replace_entries_iterable,
    keys_to_path
)
from pylhc_submitter.utils.logging_tools import log_setup

LOG = logging.getLogger(__name__)


def get_params():
    params = EntryPointParameters()
    params.add_parameter(
        name="mask",
        type=Path,
        required=True,
        help="Program mask to use",
    )
    params.add_parameter(
        name="working_directory",
        type=Path,
        required=True,
        help="Directory where data should be put",
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
        help="Directory of SixDesk. Default is pro version on AFS.",
    )
    params.add_parameter(
        name="executable",
        default=AutoSixEnvironment.executable,
        type=PathOrStr,
        help="Path to executable.",
    )
    params.add_parameter(
        name="python2",
        default=AutoSixEnvironment.python2,
        type=PathOrStr,
        help=("Path to python to use with run_six.sh (python2 with requirements installed)."
              " ONLY THE PATH TO THE DIRECTORY OF THE python BINARY IS NEEDED!"
              " And it can't be an Anaconda Distribution."),
    )
    params.add_parameter(
        name="python3",
        default=AutoSixEnvironment.python3,
        type=PathOrStr,
        help="Path to python to use with sixdb (python3 with requirements installed).",
    )
    params.add_parameter(
        name="jobid_mask",
        help="Mask to name jobs from replace_dict",
        type=str,
    )
    params.add_parameter(
        name="ssh",
        help="Run htcondor from this machine via ssh (needs access to the ``working_directory``)",
        type=str,
    )
    params.add_parameter(
        name="unlock",
        help="Forces unlocking of folders.",
        action="store_true",
    )
    params.add_parameter(
        name="apply_mad6t_hacks",
        help=(
            "Apply two hacks: Removes '<' in binary call and"
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
        help="Resubmits if needed.",
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
        help="Maximum jobs to be materialized in scheduler. "
             "Here: ``None`` leaves the settings as defined in the SixDesk "
             "htcondor_run_six.sub template and ``0`` removes it from the "
             "template. Warning: This setting modifies the template in the "
             "``sixdesk_directory`` permanently. For more details htcondor API.",
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
