"""
Multiparser Tail Parsing
========================

Contains functions and decorators for parsing file data line by line.
The contents are sent to a dictionary.

"""
__date__ = "2023-10-16"
__author__ = "Kristian Zarebski"
__maintainer__ = "Kristian Zarebski"
__email__ = "kristian.zarebski@ukaea.uk"
__copyright__ = "Copyright 2023, United Kingdom Atomic Energy Authority"

import contextlib
import datetime
import functools
import os.path
import platform
import typing

__all__ = ["record_csv", "log_parser", "record_log"]

from multiparser.typing import TimeStampedData


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


def _converter(value: str) -> typing.Any:
    """Convert from string to numeric type"""
    with contextlib.suppress(ValueError):
        _int_val = int(value)
        return _int_val
    if value.replace(".", "", 1).isdigit():
        return float(value)
    return value


def _record_any_delimited(
    file_content: str,
    delimiter: str,
    headers: typing.List[str] | None = None,
    tracked_values: typing.List[typing.Tuple[str | None, typing.Pattern]] | None = None,
    convert: bool = True,
    **_,
) -> TimeStampedData:
    """General internal function for any delimited file line.

    Parameters
    ----------
    file_content : str
        the contents of the file line
    delimiter : str
        the delimiter separating values within the file line
    header : typing.List[str]
        the file headers representing the keys for the values
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
    # In case where user has provided headers but they are also in
    # the file itself auto-skip this line
    if headers and delimiter.join(headers) in file_content:
        return {}, []

    _line = [
        _stripped for i in file_content.split(delimiter) if (_stripped := i.strip())
    ]

    if not _line:
        return {}, {}

    if not headers:
        return {"headers": _line}, []

    if convert:
        _line = [_converter(i) for i in _line]

    _out: typing.Dict[str, typing.Any] = dict(zip(headers, _line))

    if not tracked_values:
        return {}, [_out]

    _out_filtered: typing.Dict[str, typing.Any] = {}

    for key, value in _out.items():
        for label, tracked_val in tracked_values:
            label = label or key

            if any(
                [
                    (isinstance(tracked_val, str) and tracked_val == key),
                    tracked_val.findall(key),
                ]
            ):
                _out_filtered[label] = value

    return {}, [_out_filtered]


@log_parser
def record_with_delimiter(
    file_content: str,
    delimiter: str,
    headers: typing.List[str] | None = None,
    tracked_values: typing.List[typing.Tuple[str | None, typing.Pattern]] | None = None,
    convert: bool = True,
    **kwargs,
) -> TimeStampedData:
    """Process a single line of a delimited file extracting the tracked values.

    Parameters
    ----------
    file_content : str
        the contents of the file line
    delimiter : str
        the delimiter separating values within the file line
    headers : typing.List[str]
        the file headers representing the keys for the values
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
    # The delimiter parser assumes each line is a new data entry so
    # revert back to list of lines here
    _file_lines: typing.List[str] = file_content.split("\n")

    _metadata: typing.Dict[str, str | int | typing.List[str]]
    _out_data: typing.Dict[str, typing.Any] | typing.List[typing.Dict[str, typing.Any]]
    _metadata, _out_data = {}, []

    for file_line in _file_lines:
        _parsed_data: TimeStampedData = _record_any_delimited(
            file_content=file_line,
            delimiter=delimiter,
            headers=headers,
            tracked_values=tracked_values,
            convert=convert,
            **kwargs,
        )

        # Make sure each line does not erase the previous metadata collected at the start
        # of processing the block, e.g. if headers are set. May have further use in future
        # if other info is extractable but not necessarily present in the first line
        _metadata = {k: v for k, v in _parsed_data[0].items() if not _metadata.get(k)}
        _out_data += (
            _parsed_data[1] if isinstance(_parsed_data[1], list) else [_parsed_data[1]]
        )

    return _metadata, _out_data


@log_parser
def record_csv(
    file_content: str,
    headers: typing.List[str] | None = None,
    tracked_values: typing.List[typing.Tuple[str | None, typing.Pattern]] | None = None,
    convert: bool = True,
    **kwargs,
) -> TimeStampedData:
    """Process a single line of a CSV file extracting the tracked values.

    Parameters
    ----------
    file_content : str
        the contents of the file line
    delimiter : str
        the delimiter separating values within the file line
    header : typing.List[str]
        the file headers representing the keys for the values
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
    # The delimiter parser assumes each line is a new data entry so
    # revert back to list of lines here
    _file_lines: typing.List[str] = file_content.split("\n")

    _metadata: typing.Dict[str, str | int | typing.List[str]]
    _out_data: typing.Dict[str, typing.Any] | typing.List[typing.Dict[str, typing.Any]]
    _metadata, _out_data = {}, []

    for file_line in _file_lines:
        _parsed_data: TimeStampedData = _record_any_delimited(
            file_content=file_line,
            delimiter=",",
            headers=headers,
            tracked_values=tracked_values,
            convert=convert,
            **kwargs,
        )

        # Make sure each line does not erase the previous metadata collected at the start
        # of processing the block, e.g. if headers are set. May have further use in future
        # if other info is extractable but not necessarily present in the first line
        _metadata = {k: v for k, v in _parsed_data[0].items() if not _metadata.get(k)}
        _out_data += (
            _parsed_data[1] if isinstance(_parsed_data[1], list) else [_parsed_data[1]]
        )

    return _metadata, _out_data


@log_parser
def _process_log_content(
    file_content: str,
    tracked_values: typing.List[typing.Tuple[str | None, typing.Pattern]] | None = None,
    convert: bool = True,
    **_,
) -> TimeStampedData:
    """Process a single line of a log file extracting the tracked values.

    Parameters
    ----------
    file_content : str
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
    parser_func: typing.Callable | None = None,
    **parser_kwargs: typing.Dict[str, typing.Any],
) -> typing.List[TimeStampedData]:
    """Record lines within a log type file.

    The 'read_bytes' option allows the reader to skip ahead of any lines
    which have already been processed important for any larger files.

    Parameters
    ----------
    input_file : str
        the path of the file to be parsed
    skip_n_lines : int, optional
        skip the first 'n' lines before parsing. Default 0.
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

    if parser_func:
        # In general parser functions are assumed to parse blocks of information
        # so join lines into single string here
        _parsed_content = parser_func(
            "\n".join(_lines),
            __input_file=input_file,
            __read_bytes=_read_bytes,
            convert=convert,
            **parser_kwargs,
        )
        return (
            list(_parsed_content)
            if isinstance(_parsed_content, (list, tuple, set))
            else [_parsed_content]
        )
    return [
        _process_log_content(
            __input_file=input_file,
            __read_bytes=_read_bytes,
            file_content=line,
            tracked_values=tracked_values,
            convert=convert,
        )
        for line in _lines
    ]
