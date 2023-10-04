import pytest
import os.path
import pandas
import tempfile
import toml

import multiparser.parsing as cc_parse

from conftest import fake_csv, fake_toml, fake_nml, fake_feather


DATA_LIBRARY: str = os.path.join(os.path.dirname(__file__), "data")


@pytest.mark.parsing
def test_parse_f90nml() -> None:
    with tempfile.TemporaryDirectory() as temp_d:
        _data_file = fake_nml(temp_d)
        _meta, _data = cc_parse.record_fortran_nml(_data_file)
        _, _data2 = cc_parse.record_file(_data_file, None, None)
        assert "timestamp" in _meta
        assert list(sorted(_data.items())) == sorted(_data2.items())


@pytest.mark.parsing
def test_parse_csv() -> None:
    with tempfile.TemporaryDirectory() as temp_d:
        _data_file = fake_csv(temp_d)
        _meta, _data = cc_parse.record_csv(_data_file)
        _, _data2 = cc_parse.record_file(_data_file, None, None)
        assert "timestamp" in _meta
        assert list(sorted(_data.items())) == sorted(_data2.items())


@pytest.mark.parsing
def test_parse_feather() -> None:
    with tempfile.TemporaryDirectory() as temp_d:
        _data_file = fake_feather(temp_d)
        _meta, _ = cc_parse.record_feather(_data_file)
        assert "timestamp" in _meta


@pytest.mark.parsing
def test_parse_toml() -> None:
    with tempfile.TemporaryDirectory() as temp_d:
        _data_file = fake_toml(temp_d)
        _meta, _data = cc_parse.record_toml(_data_file)
        _, _data2 = cc_parse.record_file(_data_file, None, None)
        assert "timestamp" in _meta
        assert list(sorted(_data.items())) == sorted(_data2.items())