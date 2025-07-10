import logging
from pathlib import Path

import pytest

from pylhc_submitter.submitter.iotools import (
    get_server_from_uri,
    is_eos_uri,
    print_stats,
    uri_to_path,
)
from pylhc_submitter.utils.environment import on_linux, on_windows


@pytest.mark.skipif(on_windows(), reason="Paths are not split on '/' on Windows.")
def test_eos_uri_manipulation_functions():
    """Unit-test for the EOS-URI parsing. (OH LOOK! An actual unit test!)"""
    server = "root://eosuser.cern.ch/"
    path = "/eos/user/m/mmustermann/"
    uri = f"{server}{path}"
    assert is_eos_uri(uri)
    assert not is_eos_uri(path)
    assert uri_to_path(uri) == Path(path)
    assert get_server_from_uri(uri) == server


def test_print_stats(caplog):
    """Checking that the stats are printed correctly."""
    new_jobs = ["a", 1]
    finished_jobs = [3, "d"]
    with caplog.at_level(logging.INFO):
        print_stats(new_jobs, finished_jobs)
    assert "run: 2" in caplog.text
    assert "finished: 2" in caplog.text
    assert "a\n1" in caplog.text
    assert "3\nd" in caplog.text
