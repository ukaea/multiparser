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
__copyright__ = "Copyright 2023, United Kingdom Atomic Energy Authority"

import contextlib
import csv
import datetime
import functools
import json
import os.path
import pickle
import platform
import typing

try:
    import f90nml
except ImportError:
    f90nml = None

try:
    import flatdict
except ImportError:
    flatdict = None

try:
    import pyarrow
except ImportError:
    pyarrow = None

try:
    import pandas
except ImportError:
    pandas = None

import loguru
import toml
import yaml

TimeStampedData = typing.Tuple[
    typing.Dict[str, str | int],
    typing.Dict[str, typing.Any] | typing.List[typing.Dict[str, typing.Any]],
]


def file_parser(parser: typing.Callable) -> typing.Callable:
    """Attach metadata to the current parser call for a file parser.

    This is a decorator for parser functions which attaches information
    on which file is being passed as well as the last modified time
    for that file.

    Parameters
    ----------
    parser : typing.Callable
        the parser function to wrap

    Returns
    -------
    typing.Callable
        new parse function with metadata capturing
    """

    @functools.wraps(parser)
    def _wrapper(input_file: str, *args, **kwargs) -> TimeStampedData:
        """Full file parser decorator"""
        _data: TimeStampedData = parser(input_file, *args, **kwargs)
        _meta_data: typing.Dict[str, str] = {
            "timestamp": datetime.datetime.fromtimestamp(
                os.path.getmtime(input_file)
            ).strftime("%Y-%m-%d %H:%M:%S.%f"),
            "hostname": platform.node(),
            "file_name": input_file,
        }
        return _meta_data | _data[0], _data[1]

    _wrapper.__name__ += "__mp_parser"

    return _wrapper


def log_parser(parser: typing.Callable) -> typing.Callable:
    """Attach metadata to the current parser call for a log parser.

    This is a decorator for parser functions which attaches information
    on which file is being passed as well as the last modified time
    for that file.

    Parameters
    ----------
    parser : typing.Callable
        the parser function to wrap

    Returns
    -------
    typing.Callable
        new parse function with metadata capturing
    """

    @functools.wraps(parser)
    def _wrapper(file_content, *args, **kwargs) -> TimeStampedData:
        """Log file parser decorator"""
        if "__read_bytes" not in kwargs:
            raise RuntimeError("Failed to retrieve argument '__read_bytes'")
        if not (_input_file := kwargs.get("__input_file")):
            raise RuntimeError("Failed to retrieve argument '__input_file'")
        _meta_data: typing.Dict[str, str] = {
            "timestamp": datetime.datetime.fromtimestamp(
                os.path.getmtime(_input_file)
            ).strftime("%Y-%m-%d %H:%M:%S.%f"),
            "hostname": platform.node(),
            "file_name": _input_file,
            "read_bytes": kwargs["__read_bytes"],
        }
        del kwargs["__input_file"]
        del kwargs["__read_bytes"]
        _meta, _data = parser(file_content, *args, **kwargs)
        return _meta | _meta_data, _data

    _wrapper.__name__ += "__mp_parser"

    return _wrapper


def flatten_data(data: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
    """Flatten dictionary into a single level of key-value pairs

    Parameters
    ----------
    data : typing.Dict[str, typing.Any]
        the data to flatten

    Returns
    -------
    typing.Dict[str, typing.Any]
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


@file_parser
def record_json(input_file: str) -> TimeStampedData:
    """Parse a JSON file"""
    return {}, json.load(open(input_file))


@file_parser
def record_yaml(input_file: str) -> TimeStampedData:
    """Parse a YAML file"""
    return {}, yaml.load(open(input_file), Loader=yaml.SafeLoader)


@file_parser
def record_pickle(input_file: str) -> TimeStampedData:
    """Parse a pickle file"""
    return {}, pickle.load(open(input_file, "rb"))


@file_parser
def record_fortran_nml(input_file: str) -> TimeStampedData:
    """Parse a Fortran Named List"""
    if not f90nml:
        raise ImportError("Module 'f90nml' is required for Fortran named list")
    if not flatdict:
        raise ImportError("Module 'flatdict' is required for Fortran named list")

    return {}, dict(f90nml.read(input_file).todict())


@file_parser
def record_csv(input_file: str) -> TimeStampedData:
    """Parse a comma separated values file"""
    with open(input_file, newline="") as in_f:
        _read_csv = csv.DictReader(in_f)
        return {}, [row for row in _read_csv]


@file_parser
def record_feather(input_file: str) -> TimeStampedData:
    """Parse a feather file"""
    if not pyarrow:
        raise ImportError("Module 'pyarrow' is required for feather file type")
    if not pandas:
        raise ImportError("Module 'pandas' is required for feather file type")
    return {}, pandas.read_feather(input_file).to_dict()


@file_parser
def record_parquet(input_file: str) -> TimeStampedData:
    """Parse a parquet file"""
    if not pyarrow:
        raise ImportError("Module 'pyarrow' is required for parquet file type")
    if not pandas:
        raise ImportError("Module 'pandas' is required for feather file type")
    return {}, pandas.read_parquet(input_file).to_dict()


@file_parser
def record_toml(input_file: str) -> TimeStampedData:
    """Parse a TOML file"""
    return {}, toml.load(input_file)


@log_parser
def _process_log_line(
    file_content: str,
    tracked_values: typing.List[typing.Tuple[str | None, typing.Pattern]] | None = None,
    convert: bool = True,
    **_,
) -> TimeStampedData:
    """Process a single line of a log file extracting the tracked values.

    Parameters
    ----------
    _ : str
        input file name (ignored by this function itself but needed so metadata decorator can be called)
    __ : int
        the number of bytes currently read (ignored by this function itself but needed so metadata decorator can be called)
    file_line : str
        the contents of the file line
    tracked_values : typing.List[typing.Tuple[str  |  None, typing.Pattern]] | None, optional
        regular expressions defining which values to track within the log file, by default None
    convert : bool, optional
        whether to convert values from string to integer etc, by default True

    Returns
    -------
    TimeStampedData
        * metadata outlining properties such as modified time etc.
        * actual recorded data from the file.
    """
    if not tracked_values:
        return {}, {}

    _out_data: typing.Dict[str, typing.Any] = {}

    def _converter(value: str) -> typing.Any:
        """Convert from string to numeric type"""
        with contextlib.suppress(ValueError):
            _int_val = int(value)
            return _int_val
        if value.replace(".", "", 1).isdigit():
            return float(value)
        return value

    for label, tracked_val in tracked_values:
        if isinstance(tracked_val, str):
            if tracked_val not in file_content:
                continue
            _results = [tracked_val]
            _type: str = "Parameter ID"
        else:
            if not (_results := tracked_val.findall(file_content)):
                continue
            _type = "Regex string"

        _multiple_results: bool = len(_results) > 1

        for i, result in enumerate(_results):
            if isinstance(result, tuple):
                if len(result) == 1:
                    if not label:
                        raise ValueError(
                            "Expected label for regex with only single matching entry"
                        )
                    _value_str: str = result[0]
                    _label: str = label
                elif len(result) == 2:
                    _label, _value_str = result

                    # If the user has provided a label as well
                    # as regex capturing a label, the provided
                    # value takes precedence
                    _label = label or _label
                else:
                    raise ValueError(
                        f"{_type} '{tracked_val}' with label assignment must return a single value"
                    )
            elif not label:
                if len(result) != 2:
                    raise ValueError(
                        f"{_type} '{tracked_val}' without label assignment must return a key-value pair"
                    )
                _label, _value_str = result
            else:
                _value_str = result
                _label = label

            _label = f"{label}_{i}" if _multiple_results else _label
            _out_data[_label] = _converter(_value_str) if convert else _value_str
    return {}, _out_data


def tail_file_n_bytes(
    file_name: str, read_bytes: int | None
) -> typing.Tuple[int, typing.List[str]]:
    """Read lines from the end of a file.

    This function retrieves the lines from a file,
    if the number of blocks is specified the read
    is skipped ahead to this point preventing the
    re-reading of all lines in larger files.

    Parameters
    ----------
    file_name : str
        the path of the file to be read
    read_bytes : int, optional
        if specified, skip to this position in the file
        before reading

    Returns
    -------
    typing.Tuple[int, typing.List[str]]
        * position at which read terminated
        * lines read
    """
    with open(file_name, "r") as _in_f:
        if read_bytes is not None:
            _in_f.seek(read_bytes)
        _lines = _in_f.readlines()
        return _in_f.tell(), _lines


def record_log(
    input_file: str,
    tracked_values: typing.List[typing.Tuple[str | None, typing.Pattern]] | None = None,
    convert: bool = True,
    read_bytes: int | None = None,
    custom_parser: typing.Callable | None = None,
    **_,
) -> typing.List[TimeStampedData]:
    """Record lines within a log type file.

    The 'read_bytes' option allows the reader to skip ahead of any lines
    which have already been processed important for any larger files.

    Parameters
    ----------
    input_file : str
        the path of the file to be parsed
    tracked_values : typing.List[typing.Tuple[str  |  None, typing.Pattern]] | None, optional
        regular expressions defining the values to be monitored, by default None
    convert : bool, optional
        whether to convert parsed values to int, float etc, by default True
    read_bytes : int | None, optional
        if provided, the position in bytes from which to read the file, by default None

    Returns
    -------
    typing.List[TimeStampedData]
        * metadata outlining properties such as modified time etc.
        * actual recorded data from the file.
    """
    _read_bytes, _lines = tail_file_n_bytes(input_file, read_bytes)
    if custom_parser:
        return [
            custom_parser(
                "\n".join(_lines), __input_file=input_file, __read_bytes=_read_bytes
            )
        ]
    return [
        _process_log_line(
            __input_file=input_file,
            __read_bytes=_read_bytes,
            file_content=line,
            tracked_values=tracked_values,
            convert=convert,
        )
        for line in _lines
    ]


SUFFIX_PARSERS: typing.Dict[typing.Tuple[str, ...], typing.Callable] = {
    ("csv",): record_csv,
    ("pkl", "pickle", "pckl"): record_pickle,
    ("pqt", "parquet"): record_parquet,
    ("toml",): record_toml,
    ("yaml", "yml"): record_yaml,
    ("nml", "fortran"): record_fortran_nml,
    ("json",): record_json,
    ("ft", "feather"): record_feather,
}


def _full_file_parse(parse_func, in_file, tracked_values) -> TimeStampedData:
    """Apply specific parser to a file"""
    _data: typing.List[typing.Dict[str, typing.Any]]
    _meta: typing.Dict[str, typing.Any]

    _parsed = parse_func(input_file=in_file)
    _meta, _data = _parsed

    # Need to handle case where there is only one set of values and
    # where there are multiple sets the same way
    if not isinstance(_data, (tuple, list, set)):
        _data = [_data]

    # If no tracked values are stated return everything
    if not tracked_values:
        return _parsed

    # Filter by key through each set of values
    _out_data: typing.List[typing.Dict[str, typing.Any]] = []

    for entry in _data:
        _out_data_entry: typing.Dict[str, typing.Any] = {}
        for tracked_val in tracked_values or []:
            _out_data_entry |= {
                k: v
                for k, v in entry.items()
                if (isinstance(tracked_val, str) and tracked_val in k)
                or (not isinstance(tracked_val, str) and tracked_val.findall(k))
            }
        _out_data.append(_out_data_entry)

    return _meta, _out_data


def record_file(
    input_file: str,
    tracked_values: typing.List[typing.Pattern] | None,
    custom_parser: typing.Callable | None,
    file_type: str | None,
    **_,
) -> TimeStampedData:
    """Record a recognised file, parsing its contents.

    If a parser exists for the given file type the file is read using
    the relevant parser and the results returned,

    Parameters
    ----------
    input_file : str
        the file to parse
    tracked_values : typing.List[typing.Pattern] | None
        regular expressions defining the values to be monitored, by default None
    custom_parser : typing.Callable | None
        a custom parser to use for the given file
    file_type : str | None
        override the parser by file extension choice

    Returns
    -------
    TimeStampedData
        * metadata outlining properties such as modified time etc.
        * actual recorded data from the file.

    Raises
    ------
    TypeError
        if the given file type is not recognised
    """
    _extension: str = file_type or os.path.splitext(input_file)[1].replace(".", "")
    _tracked_vals: typing.List[typing.Pattern] | None = tracked_values or []

    if custom_parser:
        return _full_file_parse(custom_parser, input_file, _tracked_vals)
    else:
        for key, parser in SUFFIX_PARSERS.items():
            if _extension not in key:
                continue
            print(_extension, parser)
            return _full_file_parse(parser, input_file, _tracked_vals)

    loguru.logger.error(
        f"The file extension '{_extension}' is not supported for 'record_file' without custom parsing"
    )
    raise TypeError(f"File of type '{_extension}' could not be recognised")
