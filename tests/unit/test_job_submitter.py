import itertools
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from pylhc_submitter.job_submitter import main as job_submit
from pylhc_submitter.submitter.iotools import uri_to_path
from pylhc_submitter.utils.environment import on_linux, on_windows

SUBFILE = "queuehtc.sub"

run_only_on_linux = pytest.mark.skipif(
    not on_linux(), reason="htcondor python bindings from PyPI are only on linux"
)

run_if_not_linux = pytest.mark.skipif(on_linux(), reason="htcondor python bindings are present")


@pytest.mark.parametrize("maskfile", [True, False])
def test_job_creation_and_localrun(tmp_path, maskfile):
    """Tests that the jobs are created and can be run locally
    from mask-string and mask-file."""
    setup = InputParameters(working_directory=tmp_path, run_local=True)
    setup.create_mask(as_file=maskfile)
    job_submit(**asdict(setup))
    _test_output(setup)


def test_output_directory(tmp_path):
    """Tests that the output is copied to the output destination.
    As a by product it also tests that the jobs are created and can be run locally."""
    setup = InputParameters(
        working_directory=tmp_path,
        run_local=True,
        output_destination=tmp_path / "my_new_output" / "long_path",
    )
    setup.create_mask()
    job_submit(**asdict(setup))


def test_detects_wrong_uri(tmp_path):
    """Tests that wrong URI's are identified."""
    for test_uri in [
        "root:/eosuser.cern.ch//eos/my_new_output/",
        "root://eosuser.cern.ch/eos/my_new_output/",
        "root:/eosuser.cern.ch/eos/my_new_output/",
    ]:
        setup = InputParameters(
            working_directory=tmp_path,
            run_local=True,
            output_destination=test_uri,
        )
        setup.create_mask()
        with pytest.raises(ValueError) as e:
            job_submit(**asdict(setup))
        assert "EOS-URI" in str(e)


@run_only_on_linux
def test_job_creation_and_localrun_with_multiline_maskstring(tmp_path):
    """Tests that the jobs are created and can be run locally from a multiline mask-string."""
    mask = '123"" \nsleep 0.1 \n/bin/bash -c  "echo "%(PARAM1)s.%(PARAM2)s'
    setup = InputParameters(working_directory=tmp_path, run_local=True)
    setup.create_mask(content=mask, as_file=False)
    job_submit(**asdict(setup))
    _test_output(setup)


@run_only_on_linux
@pytest.mark.parametrize("maskfile", [True, False])
def test_job_creation_and_dryrun(tmp_path, maskfile):
    """Tests that the jobs are created as dry-run from mask-file and from mask-string."""
    setup = InputParameters(working_directory=tmp_path, dryrun=True)
    setup.create_mask(as_file=maskfile)
    job_submit(**asdict(setup))
    _test_subfile_content(setup)
    _test_output(setup, post_run=False)


@run_only_on_linux
@pytest.mark.parametrize("maskfile", [True, False])
@pytest.mark.parametrize("destination", [True, False])
def test_more_subfile_content(tmp_path, maskfile, destination):
    """Tests that the jobs are created as dry-run from mask-file and from mask-string."""
    setup = InputParameters(
        jobflavour="espresso",  # change from default
        working_directory=tmp_path,
        dryrun=True,
        output_destination=tmp_path / "my_new_output" / "long_path" if destination else None,
        job_output_dir="MyOutputDataHere",  # change from default
    )
    setup.create_mask(as_file=maskfile)
    job_submit(**asdict(setup))
    _test_subfile_content(setup)


@run_only_on_linux
@pytest.mark.parametrize("maskfile", [True, False])
def test_find_errorneous_percentage_signs(tmp_path, maskfile):
    """Tests that a key-error is raised on a mask-string with percentage signs,
    that are not part of the replacement parameters."""
    mask = "%(PARAM1)s.%(PARAM2)d\nsome stuff # should be 5%\nsome % more % stuff."
    setup = InputParameters(working_directory=tmp_path)
    setup.create_mask(content=mask, as_file=maskfile)
    with pytest.raises(KeyError) as e:
        job_submit(**asdict(setup))
    assert "problematic '%'" in str(e)


@run_only_on_linux
@pytest.mark.parametrize("maskfile", [True, False])
def test_missing_keys(tmp_path, maskfile):
    """Tests that a key-error is raised on a mask-string with missing keys in the replacement dict."""
    mask = "%(PARAM1)s.%(PARAM2)s.%(PARAM3)s"
    setup = InputParameters(working_directory=tmp_path)
    setup.create_mask(content=mask, as_file=maskfile)
    with pytest.raises(KeyError) as e:
        job_submit(**asdict(setup))
    assert "PARAM3" in str(e)


@run_if_not_linux
def test_htcondor_bindings_not_found_on_nonlinux_os(tmp_path):
    """Test that an error is raised if htcondor bindings are not found.
    If this tests fails, this might mean, that htcondor bindings are finally
    available for the other platforms."""
    setup = InputParameters(working_directory=tmp_path)
    setup.create_mask()
    with pytest.raises(EnvironmentError) as e:
        job_submit(**asdict(setup))
    assert "htcondor bindings" in str(e)


@run_only_on_linux
@pytest.mark.cern_network
@pytest.mark.parametrize("destination", [True, False])
@pytest.mark.parametrize("uri", [False, True])
def test_htc_submit(destination: bool, uri: bool):
    """This test is here for manual testing.
    It runs 3 scenarios and each submits 6 jobs to HTCondor.
    This means you need to be in the cern-network on a machine with afs and eos access
    and htcondor installed.
    You need to adapt the path to your user-name and delete the results afterwards manually.

    Scenarios:
        a) destination = False: Transfer output data back to afs
        b) destination = True, uri = False: Copy output data to EOS (via eos path)
        c) destination = True, uri = True: Copy output data to EOS (via eos uri)

    Run this test twice, manually changing `prerun` from "True" to "False" after the jobs are finished.
     -  `prerun = True`: create the folder structures and submit the jobs.
     -  `prerun = False`: check that the output data is present.
    """
    # MANUAL THINGS TO CHANGE ##############################################
    user = "mmustermann"  # set your username
    tmp_name = "htc_temp"  # name for the temporary folder (will be created)
    prerun = True
    # prerun = False       # switch here after jobs finished.

    # Uncomment to fix the kerberos ticket, in case htcondor doesn't find it.
    # Do a `klist` in terminal and adapt the path.
    # import os
    # os.environ["KRB5CCNAME"] = "/tmp/krb5cc_####"
    ########################################################################
    if uri and not destination:
        return  # only need to run one when destination is not set

    # set working_directory
    if destination:
        tmp_name = f"{tmp_name}_dest"
        if uri:
            tmp_name = f"{tmp_name}_uri"

    path = Path("/", "afs", "cern.ch", "user", user[0], user, tmp_name)
    path.mkdir(exist_ok=True)

    # set output_destination
    dest = None
    if destination:
        dest = f"/eos/user/{user[0]}/{user}/{tmp_name}"
        if uri:
            dest = f"root://eosuser.cern.ch/{dest}"

    # create setup
    setup = InputParameters(
        working_directory=path,
        output_destination=dest,
        # dryrun=True
    )
    setup.create_mask()

    if prerun:
        # submit jobs
        job_submit(**asdict(setup))
        _test_subfile_content(setup)
        _test_output(setup, post_run=False)
    else:
        # check output
        _test_output(setup, post_run=True)


# Helper -----------------------------------------------------------------------


@dataclass
class InputParameters:
    """job_submitter input parameters."""

    working_directory: Path
    executable: str | None = None if on_windows() else "/bin/bash"
    script_extension: str | None = ".bat" if on_windows() else ".sh"
    job_output_dir: str | None = "Outputdir"
    jobid_mask: str | None = "%(PARAM1)s.%(PARAM2)d"
    replace_dict: dict | None = field(
        default_factory=lambda: dict(PARAM1=["a", "b"], PARAM2=[1, 2, 3])
    )
    jobflavour: str | None = "workday"
    resume_jobs: bool | None = True
    check_files: Sequence | None = field(
        default_factory=lambda: [
            "out.txt",
        ]
    )
    dryrun: bool | None = False
    run_local: bool | None = False
    htc_arguments: dict | None = field(
        default_factory=lambda: {"max_retries": "4", "some_other_argument": "some_other_parameter"}
    )
    output_destination: Path | None = None
    mask: Path | str = None  # will be set in create_mask

    def create_mask(
        self, name: str = "test_script.mask", content: str = None, as_file: bool = False
    ):
        output_file = Path(self.job_output_dir, self.check_files[0])

        if content is None:
            content = self.jobid_mask

        if on_windows():
            mask_string = f'echo {content}> "{output_file!s}"'
        else:
            mask_string = f'echo "{content}" > "{output_file!s}"'
            if not as_file:
                mask_string = " ".join(['-c "', mask_string, '"'])

        mask_string = f"{mask_string}\n"

        if as_file:
            mask_path = self.working_directory / name
            with mask_path.open("w") as f:
                f.write(mask_string)
            self.mask = mask_path
        else:
            self.mask = mask_string


def _test_subfile_content(setup: InputParameters):
    """Checks some of the content of the subfile (queuehtc.sub)."""
    subfile = setup.working_directory / SUBFILE
    assert subfile.exists()
    with subfile.open("r") as sfile:
        filecontents = dict(line.rstrip().split(" = ") for line in sfile if " = " in line)
        assert (
            filecontents["MY.JobFlavour"].strip('"') == setup.jobflavour
        )  # flavour is saved with "" in .sub, and read in with them
        if setup.output_destination is None:
            assert filecontents["transfer_output_files"] == setup.job_output_dir
        else:
            assert filecontents["transfer_output_files"] == '""'

        for key in setup.htc_arguments.keys():
            assert filecontents[key] == setup.htc_arguments[key]


def _test_output(setup: InputParameters, post_run: bool = True):
    """Checks the validity of the output."""

    combinations = _generate_combinations(setup.replace_dict)
    assert len(combinations)
    assert len(combinations) == np.prod([len(v) for v in setup.replace_dict.values()])

    for combination_instance in combinations:
        current_id = setup.jobid_mask % combination_instance
        job_name = f"Job.{current_id}"

        if isinstance(setup.mask, Path):
            assert (
                (setup.working_directory / job_name / setup.mask.name)
                .with_suffix(setup.script_extension)
                .exists()
            )

        def _check_output_content(dir_path: Path, check_output: bool = True):
            # Check if the code created the folder structure ---
            job_path = uri_to_path(dir_path) / job_name

            assert job_path.exists()
            assert job_path.is_dir()

            if check_output:  # Check if the jobs created the files ---
                out_dir_path = job_path / setup.job_output_dir
                out_file_path = out_dir_path / setup.check_files[0]

                assert out_dir_path.is_dir()
                assert out_file_path.exists()
                assert out_file_path.is_file()

                with out_file_path.open("r") as f:
                    assert f.read().strip("\n") == current_id

        # Check local working directory ---
        _check_output_content(
            setup.working_directory, check_output=post_run and setup.output_destination is None
        )

        if setup.output_destination is not None:
            # Check copy at output destination ---
            _check_output_content(setup.output_destination, check_output=post_run)


def _generate_combinations(data: dict[str, Sequence]) -> list[dict[str, Any]]:
    """Creates all possible combinations of values in data as a list of dictionaries."""
    keys = list(data.keys())
    all_values = [data[key] for key in keys]

    return [
        {keys[i]: values[i] for i in range(len(keys))} for values in itertools.product(*all_values)
    ]
