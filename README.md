# <img src="https://raw.githubusercontent.com/pylhc/pylhc.github.io/master/docs/assets/logos/OMC_logo.svg" height="28"> PyLHC Submitter

[![Cron Testing](https://github.com/pylhc/submitter/workflows/Cron%20Testing/badge.svg)](https://github.com/pylhc/submitter/actions?query=workflow%3A%22Cron+Testing%22)
[![Code Climate coverage](https://img.shields.io/codeclimate/coverage/pylhc/submitter.svg?style=popout)](https://codeclimate.com/github/pylhc/submitter)
[![Code Climate maintainability (percentage)](https://img.shields.io/codeclimate/maintainability-percentage/pylhc/submitter.svg?style=popout)](https://codeclimate.com/github/pylhc/submitter)
<!-- [![GitHub last commit](https://img.shields.io/github/last-commit/pylhc/submitter.svg?style=popout)](https://github.com/pylhc/submitter/) -->
[![PyPI Version](https://img.shields.io/pypi/v/pylhc_submitter?label=PyPI&logo=pypi)](https://pypi.org/project/pylhc_submitter/)
[![GitHub release](https://img.shields.io/github/v/release/pylhc/submitter?logo=github)](https://github.com/pylhc/submitter/)
[![Conda-forge Version](https://img.shields.io/conda/vn/conda-forge/pylhc_submitter?color=orange&logo=anaconda)](https://anaconda.org/conda-forge/pylhc_submitter)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.4818455.svg)](https://doi.org/10.5281/zenodo.4818455)

This package contains scripts to simplify the creation, parametrization and submission of simulation jobs to HTCondor clusters at CERN.

See the [API documentation](https://pylhc.github.io/submitter/) for details.

## Installing

**Note**: This package is available 
- through `PyPI` on `Linux`, `Windows` and `macOS`
- through `conda-forge` on `Linux` and `macOS`

Installation is easily done via `pip`:
```bash
python -m pip install pylhc-submitter
```

One can also install in a `conda` environment via the `conda-forge` channel with:
```bash
conda install -c conda-forge pylhc_submitter
```

After installing, scripts can be run with either `python -m pylhc_submitter.SCRIPT --FLAG ARGUMENT` or by calling the Python files directly.

See the [API documentation](https://pylhc.github.io/submitter/) for details.

## Functionality

- `HTCondor Job Submitter` - Allows to generate jobs based on a templates and submit them to HTCondor. ([**job_submitter.py**](pylhc_submitter/job_submitter.py))
- `AutoSix` - Allows to generate and submit parametric SixDesk studies easily. ([**autosix.py**](pylhc_submitter/autosix.py))

## License

This project is licensed under the `MIT` License - see the [LICENSE](LICENSE) file for details.
