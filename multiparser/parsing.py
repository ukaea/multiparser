import contextlib
import datetime
import json
import os.path
import pickle
import platform
import typing

import f90nml
import loguru
import pandas
import toml
import yaml

TimeStampedData = typing.Tuple[
    typing.Dict[str, str | int], typing.Dict[str, typing.Any]
]


def meta_stamp_record(parser: typing.Callable) -> typing.Callable:
    """Attach metadata to the current parser call.

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

    def _wrapper(input_file: str, *args, **kwargs) -> TimeStampedData:
        _data: TimeStampedData = parser(input_file, *args, **kwargs)
        _meta_data: typing.Dict[str, str] = {
            "timestamp": datetime.datetime.fromtimestamp(
                os.path.getmtime(input_file)
            ).strftime("%Y-%M-%d %H:%M:%S.%f"),
            "hostname": platform.node(),
            "file_name": input_file,
        }
        return _meta_data | _data[0], _data[1]

    return _wrapper


@meta_stamp_record
def record_json(input_file: str, **_) -> TimeStampedData:
    """Parse a JSON file"""
    return {}, json.load(open(input_file))


@meta_stamp_record
def record_yaml(input_file: str, **_) -> TimeStampedData:
    """Parse a YAML file"""
    return {}, yaml.load(open(input_file), Loader=yaml.SafeLoader)


@meta_stamp_record
def record_pickle(input_file: str, **_) -> TimeStampedData:
    """Parse a pickle file"""
    return {}, pickle.load(open(input_file, "rb"))


@meta_stamp_record
def record_fortran_nml(input_file: str, **_) -> TimeStampedData:
    """Parse a Fortran Named List"""
    return {}, f90nml.read(input_file)


@meta_stamp_record
def record_csv(input_file: str, **_) -> TimeStampedData:
    """Parse a comma separated values file"""
    return {}, pandas.read_csv(input_file).to_dict()


@meta_stamp_record
def record_feather(input_file: str, **_) -> TimeStampedData:
    """Parse a feather file"""
    return {}, pandas.read_feather(input_file).to_dict()


@meta_stamp_record
def record_parquet(input_file: str, **_) -> TimeStampedData:
    """Parse a parquet file"""
    return {}, pandas.read_parquet(input_file).to_dict()


@meta_stamp_record
def record_hdf(input_file: str, **_) -> TimeStampedData:
    """Parse a HDF5 file"""
    return {}, pandas.read_hdf(input_file).to_dict()


@meta_stamp_record
def record_toml(input_file: str, **_) -> TimeStampedData:
    """Parse a TOML file"""
    return {}, toml.load(input_file)


@meta_stamp_record
def _process_log_line(
    _: str,
    file_line: str,
    read_bytes: int,
    tracked_values: typing.List[typing.Tuple[str | None, typing.Pattern]] | None = None,
    convert: bool = True,
) -> TimeStampedData:
    """Process a single line of a log file extracting the tracked values.

    Parameters
    ----------
    _ : str
        input file name (ignored by this function itself but needed so metadata decorator can be called)
    file_line : str
        the contents of the file line
    read_bytes : int
        the number of bytes currently read, this information is stored within the metadata so it can
        be referred to at a later point
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
        return {"read_bytes": read_bytes}, {}

    _out_data: typing.Dict[str, typing.Any] = {}

    def _converter(value: str) -> typing.Any:
        with contextlib.suppress(ValueError):
            _int_val = int(value)
            return _int_val
        if value.replace(".", "", 1).isdigit():
            return float(value)
        return value

    for label, regex in tracked_values:
        if not (_results := regex.findall(file_line)):
            continue

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
                else:
                    raise ValueError(
                        f"Regex string '{regex}' with label assignment must return a value"
                    )
            elif not label:
                if len(result) != 2:
                    raise ValueError(
                        f"Regex string '{regex}' without label assignment must return a key-value pair"
                    )
                _label, _value_str = result
            else:
                _value_str = result
                _label = label

            _label = f"{label}_{i}" if _multiple_results else _label
            _out_data[_label] = _converter(_value_str) if convert else _value_str
    return {"read_bytes": read_bytes}, _out_data


def tail_file_n_bytes(
    file_name: str, read_blocks: int | None
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
    read_blocks : int, optional
        if specified, skip to this position in the file
        before reading

    Returns
    -------
    typing.Tuple[int, typing.List[str]]
        * position at which read terminated
        * lines read
    """
    with open(file_name, "r") as _in_f:
        if read_blocks:
            _in_f.seek(read_blocks, 0)
        _lines = _in_f.readlines()
        return _in_f.tell(), _lines


def record_log(
    input_file: str,
    tracked_values: typing.List[typing.Tuple[str | None, typing.Pattern]] | None = None,
    convert: bool = True,
    read_bytes: int | None = None,
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
    return [
        _process_log_line(
            input_file,
            file_line=line,
            read_bytes=_read_bytes,
            tracked_values=tracked_values,
            convert=convert,
        )
        for line in _lines
    ]


SUFFIX_PARSERS: typing.Dict[typing.Tuple[str, ...], typing.Callable] = {
    ("csv",): record_csv,
    ("pkl", "pickle"): record_pickle,
    ("pqt",): record_parquet,
    ("hdf", "h5", "hdf5"): record_hdf,
    ("toml",): record_toml,
    ("yaml", "yml"): record_yaml,
    ("nml",): record_fortran_nml,
    ("json",): record_json,
}


def record_file(
    input_file: str, tracked_values: typing.List[typing.Pattern] | None
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
    _extension: str = os.path.splitext(input_file)[1].replace(".", "")
    _tracked_vals: typing.List[typing.Pattern] | None = tracked_values or []

    for key, parser in SUFFIX_PARSERS.items():
        if _extension not in key:
            continue
        _parsed_data: TimeStampedData = parser(input_file)

        if not _tracked_vals:
            return _parsed_data

        _out_data: typing.Dict[str, typing.Any] = {}

        for reg_ex in tracked_values or []:
            _out_data |= {k: v for k, v in _parsed_data[1].items() if reg_ex.findall(k)}

        return _parsed_data[0], _out_data

    loguru.logger.error(
        f"The file extension '{_extension}' is not supported for 'record_file', did you mean to use 'tail'?"
    )
    raise TypeError(f"File of type '{_extension}' could not be recognised")
