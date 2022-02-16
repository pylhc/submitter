# `pylhc-submitter` Changelog

## Version 1.1.0

This release adds some changes the `autosix` module:

- New Features:
  - Added `sixdesk_directory` option, which allows the user to choose their own 
    sixdesk environment (default is PRO on AFS).
  - Added `max_materialize` option, which allows the user to specify the amount of jobs that
    materialize at once per SixDesk Workspace (i.e. one realization in the `replace_dict`).
    This enables the user to send more jobs to HTCondor than are allowed within their user limit.
    See the HTCondor API for details. This option requires writing rights in the `sixdesk_directory`.
  - Allow `ENERGY` and `EMMITANCE` to be set via `replace_dict`, which are then 
    passed to the `sixdeskenv` (`GAMMA` is calculated from the `EMMITANCE` automatically).
    
- Changes:
  - Big object-oriented restructuring of the Stages and increased use of Dataclasses.
  - Some other small changes to improve readablity.
  - Fixed the DA-Plot labels from `sigma [sigma]` to `DA [sigma]`

## Version 1.0.1

Version `1.0.1` is a patch release.

- Fixed:
    - Fixed an issue where the `config.ini` file created during submission would be saved in the `site-packages` instead of the specified `working_directory` if the submitter was installed and called as a module (`python -m pylhc_submitter.job_submitter ...`) ([pull/16](https://github.com/pylhc/submitter/pull/16)).
    - Fixed an issue where `%` characters in a config file used for submission would cause the parameter parsing to crash ([pull/16](https://github.com/pylhc/submitter/pull/16)).
    - Stopped the use of `OrderedDict` which would be written down to the `config.ini` file and prevent further use of said file ([pull/16](https://github.com/pylhc/submitter/pull/16)).

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
