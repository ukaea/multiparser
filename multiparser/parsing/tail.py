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
import re
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
            "__read_bytes": kwargs["__read_bytes"],
        }
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


@log_parser
def _record_any_delimited(
    file_content: str,
    delimiter: str,
    headers: typing.List[str] | None = None,
    header_pattern: str | re.Pattern | None = None,
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
    header_pattern : str | Pattern, optional
        if specified, a string or pattern which identifies which line is to be used
        for headers
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
        return {}, {}

    _line = []

    # CSV files etc often use quotes for strings, where this is the case
    # we can remove these, else assume quote is part of header/info
    for component in file_content.split(delimiter):
        _component = component.strip()
        for quote_symbol in ("'", '"'):
            if _component.startswith(quote_symbol) and _component.endswith(
                quote_symbol
            ):
                _component = _component[1:-1]
        _line.append(_component)

    if not _line:
        return {}, {}

    if not headers and any(
        [
            isinstance(header_pattern, re.Pattern)
            and header_pattern.findall(file_content),
            isinstance(header_pattern, str) and header_pattern in file_content,
            not header_pattern,
        ]
    ):
        return {"headers": _line}, {}

    # If a patter has been specified for headers, but none have been identified yet
    # then return as we only want data that follows a header line
    if not headers and header_pattern:
        return {}, {}

    if convert:
        _line = [_converter(i) for i in _line]

    _out: typing.Dict[str, typing.Any] = dict(zip(headers, _line))  # type: ignore

    if not tracked_values:
        return {}, _out

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

    return {}, _out_filtered


def record_with_delimiter(
    file_content: str,
    delimiter: str,
    headers: typing.List[str] | None = None,
    tracked_values: typing.List[typing.Tuple[str | None, typing.Pattern]] | None = None,
    convert: bool = True,
    **kwargs,
) -> typing.List[TimeStampedData]:
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
    _file_lines: typing.List[str] = [i for i in file_content.split("\n") if i]

    _parsed_data: typing.List[TimeStampedData] = []
    _global_metadata: typing.Dict[str, str | int | typing.List[str]] = {}

    if headers:
        _global_metadata["headers"] = headers

    _parsed_lines_output: typing.List[typing.Dict[str, typing.Any]] = []

    if not _file_lines:
        return []

    for file_line in _file_lines:
        _parsed_line: TimeStampedData = _record_any_delimited(
            file_content=file_line,
            delimiter=delimiter,
            tracked_values=tracked_values,
            convert=convert,
            **(_global_metadata | kwargs),
        )

        if not isinstance(_parsed_line[1], dict):
            raise AssertionError(
                "Expected parsed statement to return a dictionary "
                f"of recorded data but got {_parsed_line}"
            )

        # Make sure each line does not erase the previous metadata collected at the start
        # of processing the block, e.g. if headers are set. May have further use in future
        # if other info is extractable but not necessarily present in the first line
        for key, value in _parsed_line[0].items():
            if key in ("headers",) and not _global_metadata.get(key):
                _global_metadata["headers"] = value
            else:
                _global_metadata[key] = value
        _parsed_lines_output.append(_parsed_line[1])

    for _parse_entry in _parsed_lines_output:
        _parsed_data.append((_global_metadata, _parse_entry))

    # Headers must be read when the file is first created else any values after read
    # will not align with these headings
    if not _global_metadata.get("headers"):
        raise AssertionError("Failed to retrieve file header during initial read")

    return _parsed_data


def record_csv(
    file_content: str,
    headers: typing.List[str] | None = None,
    tracked_values: typing.List[typing.Tuple[str | None, typing.Pattern]] | None = None,
    convert: bool = True,
    **kwargs,
) -> typing.List[TimeStampedData]:
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
    _file_lines: typing.List[str] = [i for i in file_content.split("\n") if i]

    _parsed_data: typing.List[TimeStampedData] = []
    _global_metadata: typing.Dict[str, str | int | typing.List[str]] = {}

    if headers:
        _global_metadata["headers"] = headers

    _parsed_lines_output: typing.List[typing.Dict[str, typing.Any]] = []

    if not _file_lines:
        return []

    for file_line in _file_lines:
        _parsed_line: TimeStampedData = _record_any_delimited(
            file_content=file_line,
            delimiter=",",
            tracked_values=tracked_values,
            convert=convert,
            **(_global_metadata | kwargs),
        )

        if not isinstance(_parsed_line[1], dict):
            raise AssertionError(
                "Expected parsed statement to return a dictionary "
                f"of recorded data but got {_parsed_line}"
            )

        # Make sure each line does not erase the previous metadata collected at the start
        # of processing the block, e.g. if headers are set. May have further use in future
        # if other info is extractable but not necessarily present in the first line
        for key, value in _parsed_line[0].items():
            if key in ("headers",) and not _global_metadata.get(key):
                _global_metadata["headers"] = value
            else:
                _global_metadata[key] = value
        _parsed_lines_output.append(_parsed_line[1])

    for _parse_entry in _parsed_lines_output:
        _parsed_data.append((_global_metadata, _parse_entry))

    # Headers must be read when the file is first created else any values after read
    # will not align with these headings
    if not _global_metadata.get("headers"):
        raise AssertionError("Failed to retrieve file header during initial read")

    return _parsed_data


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
    ignore_lines: typing.List[typing.Pattern] | None = None,
    parser_func: typing.Callable | None = None,
    __read_bytes: int | None = None,
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
    ignore_lines : typing.List[Pattern], optional
        specify patterns defining lines which should be skipped
    parser_func : typing.Callable, optional
        specify an alternative tail parsing function
    __read_bytes : int | None, optional
        internally set if provided, the position in bytes from which to read the file, by default None
    **parser_kwargs
        arguments to pass to a custom tail parsing function if provided

    Returns
    -------
    typing.List[TimeStampedData]
        * metadata outlining properties such as modified time etc.
        * actual recorded data from the file.
    """
    __read_bytes, _lines = tail_file_n_bytes(input_file, __read_bytes)

    # Check if there are patterns defined for lines that should be ignored
    # if this is the case loop through all patterns for each line,
    # the patterns can either be string literals or regex compiled patterns
    if ignore_lines:
        _passing_lines: typing.List[str] = []
        for line in _lines:
            if any(
                [
                    any(
                        [
                            isinstance(pattern, re.Pattern) and pattern.findall(line),
                            isinstance(pattern, str) and pattern in line,
                        ]
                    )
                    for pattern in ignore_lines
                ]
            ):
                continue
            _passing_lines.append(line)
        _lines = _passing_lines

    if parser_func:
        # In general parser functions are assumed to parse blocks of information
        # so join lines into single string here, the number of bytes processed
        # is passed into the parser so it is stored
        _parsed_content = parser_func(
            "\n".join(_lines),
            __input_file=input_file,
            __read_bytes=__read_bytes,
            convert=convert,
            **parser_kwargs,
        )
        return (
            _parsed_content if isinstance(_parsed_content, list) else [_parsed_content]
        )
    return [
        _process_log_content(
            __input_file=input_file,
            __read_bytes=__read_bytes,
            file_content=line,
            tracked_values=tracked_values,
            convert=convert,
        )
        for line in _lines
    ]


# Built in parsers do not need to be validated by the File Monitor
record_csv.__skip_validation = True  # type: ignore
record_log.__skip_validation = True  # type: ignore
record_with_delimiter.__skip_validation = True  # type: ignore
