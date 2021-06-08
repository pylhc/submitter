from functools import partial
from pathlib import Path

import pytest
from generic_parser import EntryPoint, EntryPointParameters
from generic_parser.entry_datatypes import DictAsString

from pylhc_submitter.utils.iotools import PathOrStr, save_config, keys_to_path, make_replace_entries_iterable


def test_escape_percentages(tmp_path):
    """Check if percentage-signs are escaped correctly in ini."""
    opt = entrypoint(mask="%(PARAM)d+%(PARAM)d")
    save_load_and_check(tmp_path, opt)


@pytest.mark.parametrize('pathparam', ['test', Path('test')], ids=['str', 'Path'])
def test_pathstr(tmp_path, pathparam):
    """Test PathOrStr datatype."""
    opt = entrypoint(pathstr=pathparam)
    assert isinstance(opt.pathstr, pathparam.__class__)

    opt = keys_to_path(opt, 'pathstr')
    assert isinstance(opt.pathstr, Path)

    save_load_and_check(tmp_path, opt)


def test_replace_dict(tmp_path):
    """Test conversion to make replace dict iterable."""
    opt = entrypoint(replace_dict={"A": 1, "B": [100, 200], "C": "ITERME"})
    assert isinstance(opt.replace_dict, dict)

    new_dict = make_replace_entries_iterable(opt.replace_dict.copy())
    assert new_dict["A"] == [opt.replace_dict["A"]]
    assert new_dict["C"] == [opt.replace_dict["C"]]
    assert new_dict["B"] == opt.replace_dict["B"]

    opt.replace_dict = new_dict
    save_load_and_check(tmp_path, opt)


def test_multiple_entries(tmp_path):
    """Test config file for multiple entries from above."""
    opt = entrypoint(replace_dict={"A": 1, "B": [100, 200], "C": "ITERME"},
                     pathstr=Path("test"),
                     mask="%(PARAM)d+%(PARAM)d")

    save_load_and_check(tmp_path, opt)


# Helper -----------------------------------------------------------------------


def save_load_and_check(path, opt):
    """Save opt, load as config file and runs basic tests."""
    save_config(path, opt, "test")
    cfg_file = next(path.glob("*.ini"))
    assert cfg_file.exists() and cfg_file.is_file()

    opt_load = entrypoint(entry_cfg=cfg_file)
    for key in opt:
        assert opt[key] == opt_load[key]


def entrypoint(**kwargs):
    """Creates an entrypoint and parses kwargs."""
    params = EntryPointParameters(
        pathstr=dict(
            type=PathOrStr,
        ),
        replace_dict=dict(
            type=DictAsString,
        ),
        mask=dict(
            type=str,
        )
    )
    entry = EntryPoint(params)
    return entry.parse(**kwargs)[0]