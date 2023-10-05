import json
import os
import random
import tempfile
import time

import pandas
import pytest
import toml
from conftest import fake_csv, fake_nml, fake_toml, to_nml

import multiparser


@pytest.mark.monitor
def test_run_on_directory_all(fake_log) -> None:
    _interval: float = 0.1
    with tempfile.TemporaryDirectory() as temp_d:
        for _ in range(8):
            random.choice([fake_csv, fake_nml, fake_toml])(temp_d)
        _log, _regex = fake_log

        def per_thread_callback(data, meta):
            return
            print(
                json.dumps(
                    {
                        "time_recorded": meta["timestamp"],
                        "file": meta["file_name"],
                        "data": data,
                    },
                    indent=2,
                )
            )

        with multiparser.FileMonitor(
            per_thread_callback, interval=_interval
        ) as monitor:
            monitor.track(os.path.join(temp_d, "*"))
            monitor.exclude(os.path.join(temp_d, "*.toml"))
            monitor.tail(_log, _regex)
            monitor.run()
            for _ in range(10):
                time.sleep(_interval)
            monitor.terminate()


@pytest.mark.monitor
def test_run_on_directory_filtered() -> None:
    _interval: float = 0.1
    with tempfile.TemporaryDirectory() as temp_d:
        _csv_dict = {
            "a_value": [10],
            "b_value": ["Hello World!"],
            "c_num": [5.6786],
            "d_other": [2.34],
        }
        _nml_dict = {"x_val_i": 4, "y": 3.45, "z_val_k": "testing"}
        _toml_dict = {"input_2": 2.34, "input_345": "test", "input_swe": 76}

        with open((_toml_file := os.path.join(temp_d, "toml_file.toml")), "w") as out_f:
            toml.dump(_toml_dict, out_f)
        pandas.DataFrame(_csv_dict).to_csv(
            (_csv_file := os.path.join(temp_d, "csv_file.csv"))
        )
        to_nml(_nml_dict, _nml_file := os.path.join(temp_d, "nml_file.nml"))

        def per_thread_callback(data, meta):
            print(
                json.dumps(
                    {
                        "time_recorded": meta["timestamp"],
                        "file": meta["file_name"],
                        "data": data,
                    },
                    indent=2,
                )
            )

        with multiparser.FileMonitor(
            per_thread_callback, interval=_interval
        ) as monitor:
            monitor.track(_csv_file, ["d_other", r"\w_value"])
            monitor.track(_nml_file, ["\w_val_\w"])
            monitor.track(_toml_file, ["input_swe", r"input_\d"])
            monitor.run()
            for _ in range(10):
                time.sleep(_interval)
            monitor.terminate()
