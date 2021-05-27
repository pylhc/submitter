# `pylhc-submitter` Changelog

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
