"""
AutoSix
-------

``AutoSix`` is a wrapper to automatically perform the necessary setup and steps needed for ``SixDesk`` use.

The functionality is similar to the ``pylhc_submitter.job_submitter`` in that the inner product of a ``replace_dict`` is used to automatically create a set of job-directories to gather the data.
To avoid conflicts, each of these job-directories is a ``SixDesk`` workspace, meaning there can be only one study per directory.

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
    module. Required fields are TURNS, AMPMIN, AMPMAX, AMPSTEP,
    ANGLES. Optional fields are RESUBMISSION, PLATFORM, LOGLEVEL,
    FIRSTSEED, LASTSEED, ENERGY, NPAIRS, EMITTANCE, DIMENSIONS, WRITEBINS.
    These keys can also be used in the mask if needed. The values of this
    dict are lists of values to replace these or single entries.


- **working_directory** *(PathOrStr)*:

    Directory where data should be put


*--Optional--*

- **da_turnstep** *(int)*:

    Step between turns used in DA-vs-Turns plot.

    default: ``100``


- **executable** *(PathOrStr)*:

    Path to executable.

    default: ``/afs/cern.ch/user/m/mad/bin/madx``


- **apply_mad6t_hacks**:

    Apply two hacks: Removes '<' in binary call and
    ignore the check for 'Twiss fail' in the submission file.
    This hack is needed in case this check greps the wrong lines,
    e.g. in madx- comments. USE WITH CARE!!

    action: ``store_true``


- **jobid_mask** *(str)*:

    Mask to name jobs from replace_dict


- **python3** *(PathOrStr)*:

    Path to python to use with sixdb (python3 with requirements
    installed).

    default: ``python3``


- **python2** *(PathOrStr)*:

    Path to python to use with run_six.sh (python2 with requirements installed).
    ONLY THE PATH TO THE DIRECTORY OF THE python BINARY IS NEEDED!
    And it can't be an Anaconda Distribution.

    default: None (uses the first ``python`` in path)


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
from typing import Union

import numpy as np
import tfs
from generic_parser import EntryPointParameters, entrypoint
from generic_parser.entry_datatypes import DictAsString

from pylhc_submitter.constants.autosix import (
    Stage,
    HEADER_BASEDIR,
    get_stagefile_path,
    DEFAULTS,
    SIXENV_REQUIRED,
    SIXENV_DEFAULT,
)
from pylhc_submitter.htc.mask import generate_jobdf_index
from pylhc_submitter.job_submitter import (
    JOBSUMMARY_FILE,
    COLUMN_JOBID,
    check_replace_dict,
    keys_to_path,
)
from pylhc_submitter.sixdesk_tools.create_workspace import (
    create_job,
    remove_twiss_fail_check,
    init_workspace,
    fix_pythonfile_call
)
from pylhc_submitter.sixdesk_tools.post_process_da import post_process_da
from pylhc_submitter.sixdesk_tools.submit import (
    submit_mask,
    submit_sixtrack,
    check_sixtrack_input,
    check_sixtrack_output,
    sixdb_cmd,
    sixdb_load,
)
from pylhc_submitter.sixdesk_tools.utils import is_locked, check_mask, check_stage, StageSkip
from pylhc_submitter.utils.iotools import PathOrStr, save_config

LOG = logging.getLogger(__name__)


def get_params():
    params = EntryPointParameters()
    params.add_parameter(
        name="mask",
        type=PathOrStr,
        required=True,
        help="Program mask to use",
    )
    params.add_parameter(
        name="working_directory",
        type=PathOrStr,
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
            f"Optional fields are {', '.join(SIXENV_DEFAULT.keys())}. "
            "These keys can also be used in the mask if needed. "
            "The values of this dict are lists of values to replace "
            "these or single entries."
        ),
        type=DictAsString,
        required=True,
    )
    params.add_parameter(
        name="executable",
        default=DEFAULTS["executable"],
        type=PathOrStr,
        help="Path to executable.",
    )
    params.add_parameter(
        name="python2",
        default=DEFAULTS["python2"],
        type=PathOrStr,
        help=("Path to python to use with run_six.sh (python2 with requirements installed)."
              " ONLY THE PATH TO THE DIRECTORY OF THE python BINARY IS NEEDED!"
              " And it can't be an Anaconda Distribution."),
    )
    params.add_parameter(
        name="python3",
        default=DEFAULTS["python3"],
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
        default=DEFAULTS["da_turnstep"],
    )
    params.add_parameter(
        name="max_stage",
        type=str,
        help="Last stage to be run. All following stages are skipped.",
    )
    return params


@entrypoint(get_params(), strict=True)
def main(opt):
    """ Loop to create jobs from replace dict product matrix. """
    LOG.info("Starting autosix.")
    with open(opt.mask, "r") as mask_f:
        mask = mask_f.read()
    opt = _check_opts(mask, opt)
    save_config(opt.working_directory, opt, __file__)

    jobdf = _generate_jobs(opt.working_directory, opt.jobid_mask, **opt.replace_dict)
    for job_args in jobdf.iterrows():
        setup_and_run(
            jobname=job_args[0],
            basedir=opt.working_directory,
            # kwargs:
            ssh=opt.ssh,
            python2=opt.python2,
            python3=opt.python3,
            unlock=opt.unlock,
            resubmit=opt.resubmit,
            da_turnstep=opt.da_turnstep,
            apply_mad6t_hacks=opt.apply_mad6t_hacks,
            max_stage=opt.max_stage,
            # kwargs passed only to create_jobs:
            mask_text=mask,
            binary_path=opt.executable,
            stop_workspace_init=opt.stop_workspace_init,
            **job_args[1],
        )


def setup_and_run(jobname: str, basedir: Path, **kwargs):
    """Main submitting procedure for single job.

    Args:
        jobname (str): Name of the job/study
        basedir (Path): Working directory

    Keyword Args (optional):
        unlock (bool): unlock folder
        ssh (str): ssh-server to use
        python (str): python binary to use for sixDB
        resubmit(bool): Resubmit jobs if checks fail
        da_turnstep (int): Step in turns for DA
        ignore_twissfail_check (bool): Hack to ignore check for 'Twiss fail' after run
        max_stage (str): Last stage to run

    Keyword Args (needed for create jobs):
        mask_text (str): Content of the mask to use.
        binary_path (Path): path to binary to use in jobs
        All Key=Values needed to fill the mask!
        All Key=Values needed to fill the mask!

    """
    LOG.info(f"vv---------------- Job {jobname} -------------------vv")
    unlock: bool = kwargs.pop("unlock", False)
    ssh: str = kwargs.pop("ssh", None)
    python2: Union[Path, str] = kwargs.pop("python2", DEFAULTS["python2"])
    python3: Union[Path, str] = kwargs.pop("python3", DEFAULTS["python3"])
    resubmit: bool = kwargs.pop("resubmit", False)
    da_turnstep: int = kwargs.pop("da_turnstep", DEFAULTS["da_turnstep"])
    apply_mad6t_hacks: bool = kwargs.pop("apply_mad6t_hacks", False)
    stop_workspace_init: bool = kwargs.pop("stop_workspace_init", False)
    max_stage: str = kwargs.pop("max_stage", None)
    if max_stage is not None:
        max_stage = Stage[max_stage]

    if is_locked(jobname, basedir, unlock=unlock):
        LOG.info(f"{jobname} is locked. Try 'unlock' flag if this causes errors.")

    with check_stage(Stage.create_job, jobname, basedir, max_stage) as check_ok:
        """
        create workspace
        > cd $basedir
        > /afs/cern.ch/project/sixtrack/SixDesk_utilities/pro/utilities/bash/set_env.sh -N workspace-$jobname

        write sixdeskenv, sysenv, filled mask (manual)

        """
        if check_ok:
            create_job(jobname, basedir, ssh=ssh, **kwargs)

    with check_stage(Stage.initialize_workspace, jobname, basedir, max_stage) as check_ok:
        """
        initialize workspace
        > cd $basedir/workspace-$jobname/sixjobs
        > /afs/cern.ch/project/sixtrack/SixDesk_utilities/pro/utilities/bash/set_env.sh -s

        remove the twiss-fail check in sixtrack_input
        (manual)
        """
        if check_ok:
            if stop_workspace_init:
                LOG.info(
                    f"Workspace creation for job {jobname} interrupted."
                    " Check directory to manually adapt ``sixdeskenv``"
                    " and ``sysenv``. Remove 'stop_workspace_init' from input"
                    " parameters or set to 'False' to continue run."
                )
                raise StageSkip()

            init_workspace(jobname, basedir, ssh=ssh)
            if apply_mad6t_hacks:
                fix_pythonfile_call(jobname, basedir)  # removes "<" in call
                remove_twiss_fail_check(jobname, basedir)  # removes 'grep twiss fail'

    with check_stage(Stage.submit_mask, jobname, basedir, max_stage) as check_ok:
        """
        submit for input generation
        > cd $basedir/workspace-$jobname/sixjobs
        > /afs/cern.ch/project/sixtrack/SixDesk_utilities/pro/utilities/bash/mad6t.sh -s
        """
        if check_ok:
            submit_mask(jobname, basedir, ssh=ssh)
            return  # takes a while, so we interrupt here

    with check_stage(Stage.check_input, jobname, basedir, max_stage) as check_ok:
        """
        Check if input files have been generated properly
        > cd $basedir/workspace-$jobname/sixjobs
        > /afs/cern.ch/project/sixtrack/SixDesk_utilities/pro/utilities/bash/mad6t.sh -c

        If not, and resubmit is active
        > /afs/cern.ch/project/sixtrack/SixDesk_utilities/pro/utilities/bash/mad6t.sh -w
        """
        if check_ok:
            check_sixtrack_input(jobname, basedir, ssh=ssh, resubmit=resubmit)

    with check_stage(Stage.submit_sixtrack, jobname, basedir, max_stage) as check_ok:
        """
        Generate simulation files (-g) and check if runnable (-c) and submit (-s) (-g -c -s == -a).
        > cd $basedir/workspace-$jobname/sixjobs
        > /afs/cern.ch/project/sixtrack/SixDesk_utilities/pro/utilities/bash/run_six.sh -a
        """
        if check_ok:
            submit_sixtrack(jobname, basedir, python=python2, ssh=ssh)
            return  # takes even longer

    with check_stage(Stage.check_sixtrack_output, jobname, basedir, max_stage) as check_ok:
        """
        Checks sixtrack output via run_status. If this fails even though all
        jobs have finished on the scheduler, check the log-output (run_status
        messages are logged to debug).
        > cd $basedir/workspace-$jobname/sixjobs
        > /afs/cern.ch/project/sixtrack/SixDesk_utilities/pro/utilities/bash/run_status
        
        If not, and resubmit is active
        > cd $basedir/workspace-$jobname/sixjobs
        > /afs/cern.ch/project/sixtrack/SixDesk_utilities/pro/utilities/bash/run_six.sh -i
        """
        if check_ok:
            check_sixtrack_output(jobname, basedir, python=python2, ssh=ssh, resubmit=resubmit)

    with check_stage(Stage.sixdb_load, jobname, basedir, max_stage) as check_ok:
        """
        Gather results into database via sixdb.
        > cd $basedir/workspace-$jobname/sixjobs
        > python3 /afs/cern.ch/project/sixtrack/SixDesk_utilities/pro/utilities/externals/SixDeskDB/sixdb . load_dir
        """
        if check_ok:
            sixdb_load(jobname, basedir, python=python3, ssh=ssh)

    with check_stage(Stage.sixdb_cmd, jobname, basedir, max_stage) as check_ok:
        """
        Analysise results in database via sixdb.
        > cd $basedir/workspace-$jobname/sixjobs
        > python3 /afs/cern.ch/project/sixtrack/SixDesk_utilities/pro/utilities/externals/SixDeskDB/sixdb $jobname da

        when fixed:
        > python3 /afs/cern.ch/project/sixtrack/SixDesk_utilities/pro/utilities/externals/SixDeskDB/sixdb $jobname da_vs_turns -turnstep 100 -outfile
        > python3 /afs/cern.ch/project/sixtrack/SixDesk_utilities/pro/utilities/externals/SixDeskDB/sixdb $jobname plot_da_vs_turns
        """
        if check_ok:
            sixdb_cmd(jobname, basedir, cmd=["da"], python=python3, ssh=ssh)

            # da_vs_turns is broken at the moment (jdilly, 19.10.2020)
            # sixdb_cmd(jobname, basedir, cmd=['da_vs_turns', '-turnstep', str(da_turnstep), '-outfile'],
            #           python=python, ssh=ssh)
            # sixdb_cmd(jobname, basedir, cmd=['plot_da_vs_turns'], python=python, ssh=ssh)

    with check_stage(Stage.post_process, jobname, basedir, max_stage) as check_ok:
        """
        Extracts the analysed data in the database and writes them to three tfs files:

        - All DA values
        - Statistics over angles, listed per seed (+ Seed 0 as over seeds and angles)
        - Statistics over seeds, listed per angle

        The statistics over the seeds are then plotted in a polar plot.
        All files are outputted to the ``sixjobs/autosix_output`` folder in the job directory.
        """
        if check_ok:
            post_process_da(jobname, basedir)

    with check_stage(Stage.final, jobname, basedir, max_stage) as check_ok:
        """ Just info about finishing this script and where to check the stagefile. """
        if check_ok:
            stage_file = get_stagefile_path(jobname, basedir)
            LOG.info(
                f"All stages run. Check stagefile {str(stage_file)} "
                "in case you want to rerun some stages."
            )
            raise StageSkip()

    LOG.info(f"^^---------------- Job {jobname} -------------------^^")


# Helper for main --------------------------------------------------------------


def _check_opts(mask_text, opt):
    opt = keys_to_path(opt, "mask", "working_directory", "executable")
    check_mask(mask_text, opt.replace_dict)
    opt.replace_dict = check_replace_dict(opt.replace_dict)
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
