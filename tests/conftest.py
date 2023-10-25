import multiprocessing
import os
import random
import string
import tempfile
import time
import typing

import pandas
import pytest
import toml
import xeger

DATA_DIR: str = os.path.join(os.path.dirname(__file__), "data")


def rand_str(length: int = 5) -> str:
    _letters = list(string.ascii_letters)
    return "".join(random.choice(_letters) for _ in range(length))


def fake_csv(output_dir: str, file_name: str | None = None) -> str:
    _file_name: str = file_name or os.path.join(output_dir, rand_str() + ".csv")
    pandas.DataFrame({rand_str(): [random.randint(0, 100) for _ in range(100)]}).to_csv(
        _file_name
    )
    return _file_name


def fake_feather(output_dir: str, file_name: str | None = None) -> str:
    _file_name: str = file_name or os.path.join(output_dir, rand_str() + ".ft")
    pandas.DataFrame(
        {rand_str(): [random.randint(0, 100) for _ in range(100)]}
    ).to_feather(_file_name)
    return _file_name


def fake_toml(output_dir: str, file_name: str | None = None) -> str:
    _file_name: str = file_name or os.path.join(output_dir, rand_str()) + ".toml"
    toml.dump(
        {rand_str(): [random.randint(0, 100) for _ in range(100)]},
        open(_file_name, "w"),
    )
    return _file_name


def fake_nml(output_dir: str, file_name: str | None = None) -> str:
    _file_name: str = os.path.join(DATA_DIR, "example.nml")
    return _file_name


def to_nml(dictionary: typing.Dict[str, typing.Any], file_name: str) -> None:
    with open(file_name, "w") as out_f:
        out_str = ["&DEMONML"]
        for key, value in dictionary.items():
            _value = value if not isinstance(value, str) else "'" + value + "'"
            out_str.append(f"{key.upper()}={_value}")
        out_str.append("/")
        out_f.write("\n".join(out_str))


@pytest.fixture
def fake_log() -> typing.Tuple[str, typing.List[typing.Tuple[None, str]]]:
    _rand_regex_1 = r"(\w+_\w+_\d+)=(\d+)"
    _rand_regex_2 = r"(test_\w+)=(\'\w+\')"
    _regex_gen = xeger.Xeger(limit=10)

    def _write_dummy_data(file_name: str) -> None:
        for _ in range(5):
            time.sleep(0.1)
            with open(file_name, "a") as out_f:
                _out_line = _regex_gen.xeger(_rand_regex_1)
                _out_line += _regex_gen.xeger(r"\w+")
                _out_line += _regex_gen.xeger(_rand_regex_2)
                out_f.writelines([_out_line])

    with tempfile.NamedTemporaryFile(suffix=".log") as temp_f:
        _process = multiprocessing.Process(
            target=_write_dummy_data, args=(temp_f.name,)
        )
        _process.start()
        yield temp_f.name, (_rand_regex_1, _rand_regex_2), (None, None)
        _process.join()
