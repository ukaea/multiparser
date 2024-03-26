import datetime
import multiprocessing.synchronize
import os.path
import toml
import random
import tempfile
import time
import contextlib
import typing
import multiprocessing
import re

import pytest
import xeger

import multiparser

XEGER_SEED: int = 23
N_FILE_LINES: int = 15
N_FILES: int = 10


def run_dummy_analysis_toml(
    output_dir: str, termination_trigger: multiprocessing.synchronize.Event
) -> None:
    """Run dummy analysis where multiple files are written containing data blocks"""
    time_step: float = 0.1
    random.seed(XEGER_SEED)

    for _ in range(N_FILES):
        time.sleep(time_step)
        time_str = f"{time_step:.1f}".replace(".", "_")
        _results_file_txt: str = os.path.join(output_dir, f"out_{time_str}.toml")
        _out_dict = {"x": random.random(), "y": random.random(), "z": random.random()}
        with open(_results_file_txt, "w") as out_f:
            print(_results_file_txt)
            toml.dump(_out_dict, out_f)
        time_step += 0.1
    time.sleep(1)
    termination_trigger.set()


def run_dummy_analysis_csv(
    output_dir: str,
    termination_trigger: multiprocessing.synchronize.Event
) -> None:
    """Run dummy analysis where multiple files are written containing data blocks"""
    _xeger = xeger.Xeger(seed=XEGER_SEED)
    time_step: float = 0.1

    for _ in range(N_FILES):
        time.sleep(time_step)
        time_str = f"{time_step:.1f}".replace(".", "_")
        _results_file_txt: str = os.path.join(output_dir, f"out_{time_str}.csv")
        with open(_results_file_txt, "w") as out_f:
            out_f.write("x,y,z\n")
        with contextlib.suppress(FileNotFoundError):
            with open(_results_file_txt, "a") as out_f:
                # Simulate a block being written to the output
                _lines = [
                    _xeger.xeger(r"\d+")
                    + ","
                    + _xeger.xeger(r"\d+")
                    + ","
                    + _xeger.xeger(r"\d+")
                    + "\n"
                    for _ in range(N_FILE_LINES)
                ]
                out_f.writelines(_lines)
        time_step += 0.1
    time.sleep(1)
    termination_trigger.set()


@pytest.mark.scenario
@pytest.mark.parametrize("mode", ("toml", "csv"))
def test_scenario_track_timestepped_files(mode: str) -> None:
    _trigger = multiprocessing.Event()
    with multiprocessing.Manager() as manager:
        _counter_dict = manager.dict()
        _counter_dict[mode] = 0

        def per_file_callback(
            data: dict[str, typing.Any],
            meta_data: dict[str, typing.Any],
            counter_dict: dict[str, int] = _counter_dict,
            dict_key: str = mode,
        ):
            print(f"Read input from file '{meta_data['file_name']}': {data}")
            counter_dict[dict_key] += 1

        with tempfile.TemporaryDirectory() as temp_d:
            _process = multiprocessing.Process(
                target=run_dummy_analysis_toml
                if mode == "toml"
                else run_dummy_analysis_csv,
                args=(temp_d, _trigger),
            )
            _process.start()
            with multiparser.FileMonitor(
                per_thread_callback=per_file_callback, termination_trigger=_trigger, terminate_all_on_fail=True
            ) as monitor:
                if mode == "toml":
                    monitor.track(
                        path_glob_exprs=os.path.join(temp_d, "*.toml"),
                        static=True
                    )
                else:
                    monitor.track(
                        path_glob_exprs=os.path.join(temp_d, "*.csv"),
                        static=True,
                    )
                monitor.run()
            _process.join()
        assert _counter_dict[mode] == N_FILES * (1 if mode == "toml" else N_FILE_LINES)
 

if __name__ in "__main__":
    test_scenario_track_timestepped_files()
