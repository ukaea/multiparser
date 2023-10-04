import datetime
import json
import os.path
import pickle
import platform
import re
import typing

import f90nml
import pandas
import toml
import yaml

TimeStampedData = typing.Tuple[typing.Dict[str, str], typing.Dict[str, typing.Any]]


def meta_stamp_record(parser: typing.Callable) -> typing.Callable:
    def _wrapper(input_file: str) -> TimeStampedData:
        _data: TimeStampedData = parser(input_file)
        _meta_data: typing.Dict[str, str] = {
            "timestamp": datetime.datetime.fromtimestamp(
                os.path.getmtime(input_file)
            ).strftime("%Y-%M-%d %H:%M:%S.%f"),
            "hostname": platform.node(),
            "file_name": input_file,
        }
        print(_data)
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
    input_file: str,
    tracked_values: typing.List[str] | None,
    tracked_regex: typing.List[str] | None,
) -> TimeStampedData:
    _extension: str = os.path.splitext(input_file)[1].replace(".", "")
    _tracked_vals: typing.List[str] | None = [i.lower() for i in tracked_values or []]

    for key, parser in SUFFIX_PARSERS.items():
        if _extension not in key:
            continue
        _parsed_data: TimeStampedData = parser(input_file)

        if not _tracked_vals and not tracked_regex:
            return _parsed_data

        _out_data: typing.Dict[str, typing.Any] = {}

        for reg_ex in tracked_regex or []:
            _out_data |= {
                k: v for k, v in _parsed_data[1].items() if re.findall(reg_ex, k)
            }
        _out_data |= {
            k: v
            for k, v in _parsed_data[1].items()
            if k not in _out_data and k in _tracked_vals
        }
        return _parsed_data[0], _out_data

    raise TypeError(f"File of type '{_extension}' could not be recognised.")
