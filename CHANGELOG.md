# `pylhc-submitter` Changelog

## Version 1.0.1

Version `1.0.1` is a patch release.

- Fixed:
    - Fixed an issue where the `config.ini` file created during submission would be saved in the `site-packages` instead of the specified `working_directory` if the submitter was installed and called as a module (`python -m pylhc_submitter.job_submitter ...`) (https://github.com/pylhc/submitter/pull/16).
    - Fixed an issue where `%` characters in a config file used for submission would cause the parameter parsing to crash (https://github.com/pylhc/submitter/pull/16).
    - Stopped the use of `OrderedDict` which would be written down to the `config.ini` file and prevent further use of said file (https://github.com/pylhc/submitter/pull/16).

## Version 1.0.0

First stable version of `pylhc_submitter`.

Important notice: this release **does not** break backwards compatibility with previous versions of the submitter.

Studies that were parametrized and submitted with previous versions of `pylhc.job_submitter` can be ran again seamlessly with this tool from their `config.ini` file.

- Added:
    - The `job_submitter` now creates the HTCondor `.sub` file when given the `dryrun` flag.
    - The `job_submitter` now accepts the use of a mask string instead of a mask file.

- Removed:
    - Remove dependency on `omc3`.

- Changed:
    - License changed from  `PyLHC`'s GPLv3 to an MIT license.

## Version 0.0.1

Initial version extracted from the pylhc/PyLHC repository.
