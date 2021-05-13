"""
Logging Tools
-------------

Tools that make the logging life easier.
"""
import logging
import sys


def log_setup():
    """ Set up a basic logger. """
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(levelname)7s | %(message)s | %(name)s"
    )
