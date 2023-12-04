"""
Job Submitter
-------------


The ``job_submitter`` allows to execute a parametric study using a script mask and a `dictionary` of parameters to replace in this mask, from the command line.
These parameters must be present in the given mask in the ``%(PARAMETER)s`` format (other types apart from string are also allowed).

The type of script and executable is freely choosable, but defaults to ``madx``, for which this submitter was originally written.
When submitting to ``HTCondor``, data to be transferred back to the working directory must be written in a sub-folder defined by ``job_output_directory`` which defaults to **Outputdata**.

This script also allows to check if all ``HTCondor`` jobs finished successfully, for resubmissions with a different parameter grid, and for local execution.
A **Jobs.tfs** file is created in the working directory containing the Job Id, parameter per job
and job directory for further post processing.

For additional information and guides, see the `Job Submitter page
<https://pylhc.github.io/packages/pylhcsubmitter/job_submitter.html>`_ in the ``OMC`` documentation site.


*--Required--*

- **mask** *(PathOrStr)*:

    Program mask to use


- **replace_dict** *(DictAsString)*:

    Dict containing the str to replace as keys and values a list of
    parameters to replace


- **working_directory** *(PathOrStr)*:

    Directory where data should be put


*--Optional--*

- **append_jobs**:

    Flag to rerun job with finer/wider grid, already existing points will
    not be reexecuted.

    action: ``store_true``


- **check_files** *(str)*:

    List of files/file-name-masks expected to be in the 'job_output_dir'
    after a successful job (for appending/resuming). Uses the 'glob'
    function, so unix-wildcards (*) are allowed. If not given, only the
    presence of the folder itself is checked.


- **dryrun**:

    Flag to only prepare folders and scripts, but does not start/submit
    jobs. Together with `resume_jobs` this can be use to check which jobs
    succeeded and which failed.

    action: ``store_true``


- **executable** *(PathOrStr)*:

    Path to executable or job-type (of ['madx', 'python3', 'python2']) to
    use.

    default: ``madx``


- **htc_arguments** *(DictAsString)*:

    Additional arguments for htcondor, as Dict-String. For AccountingGroup
    please use 'accounting_group'. 'max_retries' and 'notification' have
    defaults (if not given). Others are just passed on.

    default: ``{}``


- **job_output_dir** *(str)*:

    The name of the output dir of the job. (Make sure your script puts its
    data there!)

    default: ``Outputdata``


- **jobflavour** *(str)*:

    Jobflavour to give rough estimate of runtime of one job

    choices: ``('espresso', 'microcentury', 'longlunch', 'workday', 'tomorrow', 'testmatch', 'nextweek')``

    default: ``workday``


- **jobid_mask** *(str)*:

    Mask to name jobs from replace_dict


- **num_processes** *(int)*:

    Number of processes to be used if run locally

    default: ``4``


- **output_destination** *(PathOrStr)*:

    Directory to copy the output of the jobs to, sorted into folders per job.
    Can be on EOS, preferrably via EOS-URI format ('root://eosuser.cern.ch//eos/...').


- **resume_jobs**:

    Only do jobs that did not work.

    action: ``store_true``


- **run_local**:

    Flag to run the jobs on the local machine. Not suggested.

    action: ``store_true``


- **script_arguments** *(DictAsString)*:

    Additional arguments to pass to the script, as dict in key-value pairs
    ('--' need to be included in the keys).

    default: ``{}``


- **script_extension** *(str)*:

    New extension for the scripts created from the masks. This is inferred
    automatically for ['madx', 'python3', 'python2']. Otherwise not
    changed.


- **ssh** *(str)*:

    Run htcondor from this machine via ssh (needs access to the
    `working_directory`)


"""
import logging
import sys
from dataclasses import fields
from pathlib import Path

from generic_parser import EntryPointParameters, entrypoint
from generic_parser.entry_datatypes import DictAsString
from generic_parser.tools import print_dict_tree

from pylhc_submitter.constants.htcondor import JOBFLAVOURS
from pylhc_submitter.constants.job_submitter import EXECUTEABLEPATH, SCRIPT_EXTENSIONS
from pylhc_submitter.submitter.iotools import CreationOpts, create_jobs, is_eos_uri, print_stats
from pylhc_submitter.submitter.mask import (check_percentage_signs_in_mask,
                                            find_named_variables_in_mask, is_mask_file)
from pylhc_submitter.submitter.runners import RunnerOpts, run_jobs
from pylhc_submitter.utils.iotools import (PathOrStr, keys_to_path, make_replace_entries_iterable,
                                           save_config)
from pylhc_submitter.utils.logging_tools import log_setup

LOG = logging.getLogger(__name__)


try:
    import htcondor
except ImportError:
    platform = "macOS" if sys.platform == "darwin" else "windows"
    LOG.warning(
        f"htcondor python bindings are linux-only. You can still use job_submitter on {platform}, "
        "but only for local runs."
    )
    htcondor = None


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
        name="executable",
        default="madx",
        type=PathOrStr,
        help=(
            "Path to executable or job-type " f"(of {str(list(EXECUTEABLEPATH.keys()))}) to use."
        ),
    )
    params.add_parameter(
        name="jobflavour",
        type=str,
        choices=JOBFLAVOURS,
        default="workday",
        help="Jobflavour to give rough estimate of runtime of one job ",
    )
    params.add_parameter(
        name="run_local",
        action="store_true",
        help="Flag to run the jobs on the local machine. Not suggested.",
    )
    params.add_parameter(
        name="resume_jobs",
        action="store_true",
        help="Only do jobs that did not work.",
    )
    params.add_parameter(
        name="append_jobs",
        action="store_true",
        help=(
            "Flag to rerun job with finer/wider grid, already existing points will not be "
            "reexecuted."
        ),
    )
    params.add_parameter(
        name="dryrun",
        action="store_true",
        help=(
            "Flag to only prepare folders and scripts, but does not start/submit jobs. "
            "Together with `resume_jobs` this can be use to check which jobs "
            "succeeded and which failed."
        ),
    )
    params.add_parameter(
        name="replace_dict",
        help=(
            "Dict containing the str to replace as keys and values a list of parameters to "
            "replace"
        ),
        type=DictAsString,
        required=True,
    )
    params.add_parameter(
        name="script_arguments",
        help=(
            "Additional arguments to pass to the script, as dict in key-value pairs "
            "('--' need to be included in the keys)."
        ),
        type=DictAsString,
        default={},
    )
    params.add_parameter(
        name="script_extension",
        help=(
            "New extension for the scripts created from the masks. This is inferred "
            f"automatically for {str(list(SCRIPT_EXTENSIONS.keys()))}. Otherwise not changed."
        ),
        type=str,
    )
    params.add_parameter(
        name="num_processes",
        help="Number of processes to be used if run locally",
        type=int,
        default=4,
    )
    params.add_parameter(
        name="check_files",
        help=(
            "List of files/file-name-masks expected to be in the "
            "'job_output_dir' after a successful job "
            "(for appending/resuming). Uses the 'glob' function, so "
            "unix-wildcards (*) are allowed. If not given, only the "
            "presence of the folder itself is checked."
        ),
        type=str,
        nargs="+",
    )
    params.add_parameter(
        name="jobid_mask",
        help="Mask to name jobs from replace_dict",
        type=str,
    )
    params.add_parameter(
        name="job_output_dir",
        help="The name of the output dir of the job. (Make sure your script puts its data there!)",
        type=str,
        default="Outputdata",
    )
    params.add_parameter(
        name="output_destination",
        help="Directory to copy the output of the jobs to, sorted into folders per job. "
             "Can be on EOS, preferrably via EOS-URI format ('root://eosuser.cern.ch//eos/...').",
        type=PathOrStr,
    )
    params.add_parameter(
        name="htc_arguments",
        help=(
            "Additional arguments for htcondor, as Dict-String. "
            "For AccountingGroup please use 'accounting_group'. "
            "'max_retries' and 'notification' have defaults (if not given). "
            "Others are just passed on. "
        ),
        type=DictAsString,
        default={},
    )
    params.add_parameter(
        name="ssh",
        help="Run htcondor from this machine via ssh (needs access to the `working_directory`)",
        type=str,
    )

    return params


@entrypoint(get_params(), strict=True)
def main(opt):
    if not opt.run_local:
        LOG.info("Starting HTCondor Job-submitter.")
        _check_htcondor_presence()
    else:
        LOG.info("Starting Job-submitter.")

    save_config(Path(opt.working_directory), opt, "job_submitter")
    creation_opt, runner_opt = check_opts(opt)

    job_df, dropped_jobs = create_jobs(creation_opt)

    run_jobs(job_df, runner_opt)

    print_stats(job_df.index, dropped_jobs)


def check_opts(opt):
    """ Checks options and sorts them into job-creation and running parameters. """
    LOG.debug("Checking options.")
    if opt.resume_jobs and opt.append_jobs:
        raise ValueError("Select either Resume jobs or Append jobs")

    # Paths ---
    opt = keys_to_path(opt, "working_directory", "executable")

    if str(opt.executable) in EXECUTEABLEPATH.keys():
        opt.executable = str(opt.executable)

    if is_mask_file(opt.mask):
        mask_content = Path(opt.mask).read_text()  # checks that mask and dir are there
        opt.mask = Path(opt.mask)
    else:
        mask_content = opt.mask
    
    if is_eos_uri(opt.output_destination) and not ("://" in opt.output_destination and "//eos/" in opt.output_destination):
        raise ValueError(
            "The 'output_destination' is an EOS-URI but missing '://' or '//eos' (double slashes?). "
        )
        

    # Replace dict ---
    dict_keys = set(opt.replace_dict.keys())
    mask_keys = find_named_variables_in_mask(mask_content)
    not_in_mask = dict_keys - mask_keys
    not_in_dict = mask_keys - dict_keys

    if len(not_in_dict):
        raise KeyError(
            "The following keys in the mask were not found in the given replace_dict: "
            f"{str(not_in_dict).strip('{}')}"
        )

    if len(not_in_mask):
        LOG.warning(
            "The following replace_dict keys were not found in the given mask: "
            f"{str(not_in_mask).strip('{}')}"
        )

        # remove all keys which are not present in mask (otherwise unnecessary jobs)
        [opt.replace_dict.pop(key) for key in not_in_mask]
        if len(opt.replace_dict) == 0:
            raise KeyError("Empty replace-dictionary")
    check_percentage_signs_in_mask(mask_content)

    print_dict_tree(opt, name="Input parameter", print_fun=LOG.debug)
    opt.replace_dict = make_replace_entries_iterable(opt.replace_dict)
    
    # Create new classes
    opt.output_dir = opt.job_output_dir  # renaming

    creation = CreationOpts(**{f.name: opt[f.name] for f in fields(CreationOpts)})
    runner = RunnerOpts(**{f.name: opt[f.name] for f in fields(RunnerOpts)})
    runner.output_dir = '""' if opt.output_destination else opt.output_dir  # empty string stops htc transfer of files
    return creation, runner


def _check_htcondor_presence() -> None:
    """ Raises an error if htcondor is not installed. """
    if htcondor is None:
        raise EnvironmentError("htcondor bindings are necessary to run this module.")


# Script Mode ------------------------------------------------------------------


if __name__ == "__main__":
    log_setup()
    main()
