"""
Multiparser Parsing
===================

Contains functions and decorators for parsing of file data either reading the
file in its entirety or reading the latest written content. The contents are
sent to a dictionary.

"""

__date__ = "2023-10-16"
__author__ = "Kristian Zarebski"
__maintainer__ = "Kristian Zarebski"
__email__ = "kristian.zarebski@ukaea.uk"
__copyright__ = "Copyright 2024, United Kingdom Atomic Energy Authority"

import typing

import flatdict

from .file import file_parser, record_file  # noqa
from .tail import log_parser, record_log  # noqa


def flatten_data(data: dict[str, typing.Any]) -> dict[str, typing.Any]:
    """Flatten dictionary into a single level of key-value pairs

    Parameters
    ----------
    data : dict[str, typing.Any]
        the data to flatten

    Returns
    -------
    dict[str, typing.Any]
        the data as a single level dictionary with '.' used as a delimiter for the
        key addresses
    """
    _data = dict(flatdict.FlatterDict(data, delimiter="."))
    return {
        key: value
        if not isinstance(value, flatdict.FlatterDict)
        else value.as_dict() or None
        for key, value in _data.items()
    }
