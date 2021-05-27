"""
IO-Tools
--------

Tools for input and output.
"""
from pathlib import Path
from datetime import datetime

from generic_parser.entry_datatypes import get_instance_faker_meta
from generic_parser.entrypoint_parser import save_options_to_config

from pylhc_submitter.constants.general import TIME


def save_config(output_dir: Path, opt: dict, script: str):
    """
    Quick wrapper for ``save_options_to_config``.

    Args:
        output_dir (Path): Path to the output directory (does not need to exist).
        opt (dict): opt-structure to be saved.
        script (str): path/name of the invoking script (becomes name of the .ini)
                      usually ``__file__``.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    opt = convert_paths_in_dict_to_strings(opt)
    time = datetime.utcnow().strftime(TIME)
    save_options_to_config(output_dir / f"{script:s}_{time:s}.ini",
                           dict(sorted(opt.items()))
                           )


def convert_paths_in_dict_to_strings(dict_: dict) -> dict:
    """Converts all Paths in the dict to strings, including those in iterables."""
    dict_ = dict_.copy()
    for key, value in dict_.items():
        if isinstance(value, Path):
            dict_[key] = str(value)
        else:
            try:
                list_ = list(value)
            except TypeError:
                pass
            else:
                has_changed = False
                for idx, item in enumerate(list_):
                    if isinstance(item, Path):
                        list_[idx] = str(item)
                        has_changed = True
                if has_changed:
                    dict_[key] = list_
    return dict_


class PathOrStr(metaclass=get_instance_faker_meta(Path, str)):
    """A class that behaves like a Path when possible, otherwise like a string."""
    def __new__(cls, value):
        if value is None:
            return None

        if isinstance(value, str):
            value = value.strip("\'\"")  # behavior like dict-parser, IMPORTANT FOR EVERY STRING-FAKER
        return Path(value)
