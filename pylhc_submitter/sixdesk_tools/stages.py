"""
Stages
------

In this module the stages are organized.

"""
import logging
import re
from abc import ABC, abstractmethod, ABCMeta

from generic_parser import DotDict

from pylhc_submitter.constants.autosix import get_stagefile_path, StageSkip, StageStop, AutoSixEnvironment
from pylhc_submitter.sixdesk_tools.create_workspace import (
    create_job, init_workspace, fix_pythonfile_call,
    remove_twiss_fail_check, set_max_materialize
)
from pylhc_submitter.sixdesk_tools.post_process_da import post_process_da
from pylhc_submitter.sixdesk_tools.submit import (
    submit_mask, check_sixtrack_input, submit_sixtrack,
    check_sixtrack_output, sixdb_load, sixdb_cmd
)

LOG = logging.getLogger(__name__)

# Overwritten in StageMeta below and actual classes inserted
STAGE_ORDER = DotDict({
    "create_job": None,
    "initialize_workspace": None,
    "submit_mask": None,
    "check_input": None,
    "submit_sixtrack": None,
    "check_sixtrack_output": None,
    "sixdb_load": None,
    "sixdb_cmd": None,
    "post_process": None,
    "final": None,
})


class StageMeta(ABCMeta):
    """ Dynamically generate name and value from STAGE_ORDER """
    def __init__(cls, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # convert CamelCase to snake_case (https://stackoverflow.com/a/1176023/5609590)
        cls.name = re.sub(r'(?<!^)(?=[A-Z])', '_', cls.__name__).lower()

        if cls.name == "stage":
            # Base Class
            cls.value = None
            return

        # set value according to list (so we can compare order)
        cls.value = list(STAGE_ORDER.keys()).index(cls.name)

        # set class as entry in STAGE_ORDER
        STAGE_ORDER[cls.name] = cls
    
    def __str__(cls):
        return cls.name

    def __int__(cls):
        return cls.value

    def __add__(cls, other):
        new_idx = int(cls) + int(other)
        return list(STAGE_ORDER.values())[new_idx]

    def __sub__(cls, other):
        new_idx = int(cls) - int(other)
        if new_idx < 0:
            raise IndexError("Requested Stage order is negative. Stage not found.")
        return list(STAGE_ORDER.values())[new_idx]
    
    def __gt__(cls, other):
        try:
            return int(cls) > int(other)
        except TypeError:
            return False

    def __ge__(cls, other):
        try:
            return int(cls) >= int(other)
        except TypeError:
            return False

    def __lt__(cls, other):
        try:
            return int(cls) < int(other)
        except TypeError:
            return False

    def __le__(cls, other):
        try:
            return int(cls) <= int(other)
        except TypeError:
            return False

    def __eq__(cls, other):
        try:
            return int(cls) == int(other)
        except TypeError:
            return False

    def __hash__(cls):
        return hash((cls.value, cls.name))


class Stage(ABC, metaclass=StageMeta):
    """ Abstract Base Class for all Stages. Also provides the basic methods.
    The stages themselves only need to implement the _run() method. """
    @staticmethod
    def run_all_stages(jobname, jobargs, env):
        """ Run all stages in order. """
        LOG.info(f"vv---------------- Job {jobname} -------------------vv")
        for stage_name, stage_class in STAGE_ORDER.items():
            stage = stage_class(jobname, jobargs, env)
            try:
                stage.run()
            except StageSkip as e:
                if str(e):
                    LOG.error(e)
                # break  # stop here or always run to the end and show all skipped stages
            except StageStop:
                LOG.info(f"Stopping after Stage '{stage!s}' as the submitted jobs will now run. "
                         f"Check `condor_q` for their progress and restart autosix when they are done.")
                break
        LOG.info(f"^^---------------- Job {jobname} -------------------^^")

    def __init__(self, jobname: str, jobargs: dict, env: AutoSixEnvironment):
        self.jobname = jobname
        self.env = env
        self.jobargs = jobargs

        # Helper ---
        self.basedir = env.working_directory
        self.max_stage = env.max_stage
        self.stage_file = get_stagefile_path(self.jobname, self.basedir)

    def __str__(self):
        return self.name

    def __int__(self):
        return self.value

    def __sub__(self, other):
        return StageMeta.__sub__(self.__class__, other)(self.jobname, self.jobargs, self.env)

    def __add__(self, other):
        return StageMeta.__add__(self.__class__, other)(self.jobname, self.jobargs, self.env)

    def __gt__(self, other):
        return int(self) > int(other)

    def __ge__(self, other):
        return int(self) >= int(other)

    def __lt__(self, other):
        return int(self) < int(other)

    def __le__(self, other):
        return int(self) <= int(other)

    def __eq__(self, other):
        return int(self) == int(other)

    def should_run_stage(self):
        """ Checks if the stage should be run. """
        if not self.stage_file.exists():
            if self == 0:
                return True
            else:
                LOG.info(f"Stage '{self!s}' not run because previous stage(s) missing.")
                return False

        stage_file_txt = self.stage_file.read_text().split("\n")
        run_stages = [line.strip() for line in stage_file_txt if line.strip()]

        if self.name in run_stages:
            LOG.info(f"Stage '{self!s}' has already been run. Skipping.")
            return False

        if self == 0:
            return True

        # check if user requested a stop at a certain stage
        if (self.max_stage is not None) and (self > self.max_stage):
            LOG.info(f"Stage '{self!s}' would run after requested "
                     f"maximum stage '{self.max_stage!s}'. Skipping.")
            return False

        # check if last run stage is also the stage before current stage in stage order
        if run_stages[-1] == (self - 1).name:
            return True

        LOG.info(f"Stage '{self!s}' not run because previous stage(s) missing.")
        return False

    def stage_done(self):
        """ Append current stage name to stagefile. """
        with open(self.stage_file, "a+") as f:
            f.write(f"{self!s}\n")

    def run(self):
        """ Run the Stage. """
        if not self.should_run_stage():
            return

        try:
            self._run()
        except StageStop as e:
            # Stage indicates that it ran successfully,
            # but that there should be a stop in the loop.
            self.stage_done()
            raise e
        except StageSkip as e:
            # logged/handled outside
            raise e
        except Exception as e:
            # convert any exception to a StageSkip,
            # so the other jobs can continue running.
            LOG.exception(str(e))
            raise StageSkip(f"Stage {self!s} failed!") from e

        self.stage_done()

    @abstractmethod
    def _run(self):
        pass


# Actual Stages ----------------------------------------------------------------
# These Stages should actually have the function they call implemented directly
# and should be defined in the modules directly. But I like that you can read
# them one after another here and also see the command one would have to run if
# one would do this manually (which can be very helpful for fixing broken jobs).
# - jdilly 2020-08-04

class CreateJob(Stage):
    """
    create workspace
    > cd $basedir
    > /afs/cern.ch/project/sixtrack/SixDesk_utilities/pro/utilities/bash/set_env.sh -N workspace-$jobname

    write sixdeskenv, sysenv, filled mask (manual)
    """
    def _run(self):
        create_job(self.jobname, self.basedir,
                   executable=self.env.executable,
                   mask_text=self.env.mask_text,
                   sixdesk=self.env.sixdesk_directory,
                   ssh=self.env.ssh,
                   **self.jobargs)


class InitializeWorkspace(Stage):
    """
    initialize workspace
    > cd $basedir/workspace-$jobname/sixjobs
    > /afs/cern.ch/project/sixtrack/SixDesk_utilities/pro/utilities/bash/set_env.sh -s

    remove the twiss-fail check in sixtrack_input
    (manual)
    """
    def _run(self):
        if self.env.stop_workspace_init:
            LOG.info(
                f"Workspace creation for job {self.jobname} interrupted."
                " Check directory to manually adapt ``sixdeskenv``"
                " and ``sysenv``. Remove 'stop_workspace_init' from input"
                " parameters or set to 'False' to continue run."
            )
            raise StageSkip()

        init_workspace(self.jobname, self.basedir,
                       sixdesk=self.env.sixdesk_directory,
                       ssh=self.env.ssh)
        if self.env.apply_mad6t_hacks:
            fix_pythonfile_call(self.jobname, self.basedir)  # removes "<" in call
            remove_twiss_fail_check(self.jobname, self.basedir)  # removes 'grep twiss fail'


class SubmitMask(Stage):
    """
    submit for input generation
    > cd $basedir/workspace-$jobname/sixjobs
    > /afs/cern.ch/project/sixtrack/SixDesk_utilities/pro/utilities/bash/mad6t.sh -s
    """
    def _run(self):
        submit_mask(self.jobname, self.basedir,
                    sixdesk=self.env.sixdesk_directory,
                    ssh=self.env.ssh)
        raise StageStop()


class CheckInput(Stage):
    """
    Check if input files have been generated properly
    > cd $basedir/workspace-$jobname/sixjobs
    > /afs/cern.ch/project/sixtrack/SixDesk_utilities/pro/utilities/bash/mad6t.sh -c

    If not, and resubmit is active
    > /afs/cern.ch/project/sixtrack/SixDesk_utilities/pro/utilities/bash/mad6t.sh -w
    """
    def _run(self):
        check_sixtrack_input(self.jobname, self.basedir,
                             sixdesk=self.env.sixdesk_directory,
                             ssh=self.env.ssh,
                             resubmit=self.env.resubmit)


class SubmitSixtrack(Stage):
    """
    Generate simulation files (-g) and check if runnable (-c) and submit (-s) (-g -c -s == -a).
    > cd $basedir/workspace-$jobname/sixjobs
    > /afs/cern.ch/project/sixtrack/SixDesk_utilities/pro/utilities/bash/run_six.sh -a
    """
    def _run(self):
        # adds max_materialize to tracking sub-file template
        # might run into a race condition, this is why it's done here.
        # Also I assume we use the same value for all jobs anyway.
        set_max_materialize(self.env.sixdesk_directory, self.env.max_materialize)

        submit_sixtrack(self.jobname, self.basedir,
                        sixdesk=self.env.sixdesk_directory,
                        ssh=self.env.ssh,
                        python=self.env.python2)

        raise StageStop()


class CheckSixtrackOutput(Stage):
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
    def _run(self):
        check_sixtrack_output(self.jobname, self.basedir,
                              sixdesk=self.env.sixdesk_directory,
                              ssh=self.env.ssh,
                              python=self.env.python2,
                              resubmit=self.env.resubmit)


class SixdbLoad(Stage):
    """
    Gather results into database via sixdb.
    > cd $basedir/workspace-$jobname/sixjobs
    > python3 /afs/cern.ch/project/sixtrack/SixDesk_utilities/pro/utilities/externals/SixDeskDB/sixdb . load_dir
    """
    def _run(self):
        sixdb_load(self.jobname, self.basedir,
                   sixdesk=self.env.sixdesk_directory,
                   ssh=self.env.ssh,
                   python=self.env.python3)


class SixdbCmd(Stage):
    """
    Analysise results in database via sixdb.
    > cd $basedir/workspace-$jobname/sixjobs
    > python3 /afs/cern.ch/project/sixtrack/SixDesk_utilities/pro/utilities/externals/SixDeskDB/sixdb $jobname da

    when fixed:
    > python3 /afs/cern.ch/project/sixtrack/SixDesk_utilities/pro/utilities/externals/SixDeskDB/sixdb $jobname da_vs_turns -turnstep 100 -outfile
    > python3 /afs/cern.ch/project/sixtrack/SixDesk_utilities/pro/utilities/externals/SixDeskDB/sixdb $jobname plot_da_vs_turns
    """
    def _run(self):
        sixdb_cmd(self.jobname, self.basedir,
                  sixdesk=self.env.sixdesk_directory,
                  ssh=self.env.ssh,
                  cmd=["da"],
                  python=self.env.python3)

        # da_vs_turns is broken at the moment (jdilly, 19.10.2020)
        # sixdb_cmd(jobname, basedir, cmd=['da_vs_turns', '-turnstep', str(da_turnstep), '-outfile'],
        #           python=python, ssh=ssh)
        # sixdb_cmd(jobname, basedir, cmd=['plot_da_vs_turns'], python=python, ssh=ssh)


class PostProcess(Stage):
    """
    Extracts the analysed data in the database and writes them to three tfs files:

    - All DA values
    - Statistics over angles, listed per seed (+ Seed 0 as over seeds and angles)
    - Statistics over seeds, listed per angle

    The statistics over the seeds are then plotted in a polar plot.
    All files are outputted to the ``sixjobs/autosix_output`` folder in the job directory.
    """
    def _run(self):
        post_process_da(self.jobname, self.basedir)


class Final(Stage):
    """ Just info about finishing this script and where to check the stagefile. """
    def _run(self):
        stage_file = get_stagefile_path(self.jobname, self.basedir)
        LOG.info(
            f"All stages run. Check stagefile {str(stage_file)} "
            "in case you want to rerun some stages."
        )
        raise StageSkip()
