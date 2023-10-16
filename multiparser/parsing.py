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
import sh
import toml
import yaml

TimeStampedData = typing.Tuple[typing.Dict[str, str], typing.Dict[str, typing.Any]]


def meta_stamp_record(parser: typing.Callable) -> typing.Callable:
    def _wrapper(input_file: str, *args, **kwargs) -> TimeStampedData:
        _data: TimeStampedData = parser(input_file, *args, **kwargs)
        _meta_data: typing.Dict[str, str] = {
            "timestamp": datetime.datetime.fromtimestamp(
                os.path.getmtime(input_file)
            ).strftime("%Y-%M-%d %H:%M:%S.%f"),
            "hostname": platform.node(),
            "file_name": input_file,
        }
        return _meta_data, _data[1]

    return _wrapper


@meta_stamp_record
def record_json(input_file: str) -> TimeStampedData:
    return {}, json.load(open(input_file))


@meta_stamp_record
def record_yaml(input_file: str) -> TimeStampedData:
    return {}, yaml.load(open(input_file), Loader=yaml.SafeLoader)


@meta_stamp_record
def record_pickle(input_file: str) -> TimeStampedData:
    return {}, pickle.load(open(input_file, "rb"))


@meta_stamp_record
def record_fortran_nml(input_file: str) -> TimeStampedData:
    return {}, f90nml.read(input_file)


@meta_stamp_record
def record_csv(input_file: str) -> TimeStampedData:
    return {}, pandas.read_csv(input_file).to_dict()


@meta_stamp_record
def record_feather(input_file: str) -> TimeStampedData:
    return {}, pandas.read_feather(input_file).to_dict()


@meta_stamp_record
def record_parquet(input_file: str) -> TimeStampedData:
    return {}, pandas.read_parquet(input_file).to_dict()


@meta_stamp_record
def record_hdf(input_file: str) -> TimeStampedData:
    return {}, pandas.read_hdf(input_file).to_dict()


@meta_stamp_record
def record_toml(input_file: str) -> TimeStampedData:
    return {}, toml.load(input_file)


@meta_stamp_record
def record_log(
    input_file: str,
    tracked_values: typing.List[typing.Tuple[str | None, typing.Pattern]] = None,
    convert: bool = True,
) -> TimeStampedData:
    """Records latest line only"""

    def _converter(value: str) -> typing.Any:
        with contextlib.suppress(ValueError):
            _int_val = int(value)
            return _int_val
        if value.replace(".", "", 1).isdigit():
            return float(value)
        return value

    _out_data: typing.Dict[str, typing.Any] = {}

    _line = sh.tail(f"-1", input_file)

    if not tracked_values:
        return {}, {"line": _line}

    for label, regex in tracked_values:
        if _results := regex.findall(_line):
            for i, result in enumerate(_results):
                if isinstance(result, tuple):
                    if len(result) == 1:
                        _value_str: str = result
                        _label = f"{label}_{i}" if len(_results) > 1 else label
                    elif len(result) == 2:
                        _label, _value_str = result
                    else:
                        raise ValueError(
                            f"Regex string '{regex}' with label assignment must return a value"
                        )
                else:
                    if not label:
                        if len(result) != 2:
                            raise ValueError(
                                f"Regex string '{regex}' without label assignment must return a key-value pair"
                            )
                        label, _value_str = result

                _out_data[_label] = _converter(_value_str) if convert else _value_str
    return {}, _out_data


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
    _extension: str = os.path.splitext(input_file)[1].replace(".", "")
    _tracked_vals: typing.List[str] | None = tracked_values or []

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
