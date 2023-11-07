import pathlib

import setuptools

# The directory containing this file
MODULE_NAME = "pylhc_submitter"
TOPLEVEL_DIR = pathlib.Path(__file__).parent.absolute()
ABOUT_FILE = TOPLEVEL_DIR / MODULE_NAME / "__init__.py"
README = TOPLEVEL_DIR / "README.md"


def about_package(init_posixpath: pathlib.Path) -> dict:
    """
    Return package information defined with dunders in __init__.py as a dictionary, when
    provided with a PosixPath to the __init__.py file.
    """
    about_text: str = init_posixpath.read_text()
    return {
        entry.split(" = ")[0]: entry.split(" = ")[1].strip('"')
        for entry in about_text.strip().split("\n")
        if entry.startswith("__")
    }


ABOUT_PYLHC_SUBMITTER = about_package(ABOUT_FILE)

with README.open("r") as docs:
    long_description = docs.read()

# Dependencies for the module itself
DEPENDENCIES = [
    "numpy>=1.19",
    "pandas>=1.0",
    "htcondor>=8.9.2 ; sys_platform=='linux'",  # no bindings for macOS or windows on PyPI
    "tfs-pandas>=3.0",
    "generic-parser>=1.0.8",
    "scipy>=1.4.0",
    "matplotlib>=3.2.0",
]

EXTRA_DEPENDENCIES = {
    "test": [
        "pytest>=5.2",
        "pytest-cov>=2.7",
    ],
    "doc": ["sphinx", "sphinx_rtd_theme"],
}
EXTRA_DEPENDENCIES.update(
    {"all": [elem for list_ in EXTRA_DEPENDENCIES.values() for elem in list_]}
)


setuptools.setup(
    name=ABOUT_PYLHC_SUBMITTER["__title__"],
    version=ABOUT_PYLHC_SUBMITTER["__version__"],
    description=ABOUT_PYLHC_SUBMITTER["__description__"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    author=ABOUT_PYLHC_SUBMITTER["__author__"],
    author_email=ABOUT_PYLHC_SUBMITTER["__author_email__"],
    url=ABOUT_PYLHC_SUBMITTER["__url__"],
    python_requires=">=3.7",
    license=ABOUT_PYLHC_SUBMITTER["__license__"],
    classifiers=[
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    packages=setuptools.find_packages(exclude=["tests*", "doc"]),
    include_package_data=True,
    install_requires=DEPENDENCIES,
    tests_require=EXTRA_DEPENDENCIES["test"],
    extras_require=EXTRA_DEPENDENCIES,
)
