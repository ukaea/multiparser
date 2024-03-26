import datetime
import multiprocessing.synchronize
import multiprocessing
import os.path
import random
import tempfile
import time
import contextlib
import re
import typing

import pytest
import xeger

import multiparser
import multiparser.parsing.tail as mp_parse_tail

XEGER_SEED: int = 15
N_LOG_ADDITIONS: int = 20


def run_dummy_analysis_text(
    output_dir: str,
    termination_trigger: multiprocessing.synchronize.Event,
    counter_dict: dict[str, int],
    dict_key: str
) -> None:
    """Run analysis where a single file is written containing increasing data blocks"""
    _results_file_txt: str = os.path.join(output_dir, "out.txt")
    _xeger = xeger.Xeger(seed=XEGER_SEED)
    for _ in range(N_LOG_ADDITIONS):
        time.sleep(0.1)
        with contextlib.suppress(FileNotFoundError):
            with open(_results_file_txt, "a") as out_f:
                # Simulate a block being written to the output
                _lines = [
                    datetime.datetime.now().strftime("%Y-%M-%d %H:%M:%S\tvalue=")
                    + _xeger.xeger(r"\d+")
                    + "\t"
                    + _xeger.xeger(r"\d+")
                    + _xeger.xeger(r"\d+")
                    + "\n"
                    for _ in range(random.randint(1, 4))
                ]
                out_f.writelines(_lines)
                counter_dict[dict_key] += len(_lines)
    time.sleep(1)
    termination_trigger.set()


def run_dummy_analysis_csv(
    output_dir: str,
    termination_trigger: multiprocessing.synchronize.Event,
    counter_dict: dict[str, int],
    dict_key: str
) -> None:
    """Run analysis where a single file is written containing increasing data blocks"""
    _results_file_csv: str = os.path.join(output_dir, "out.csv")
    _xeger = xeger.Xeger(seed=XEGER_SEED)
    with open(_results_file_csv, "w") as out_f:
        out_f.writelines(["x,y,z\n"])
    for _ in range(N_LOG_ADDITIONS):
        time.sleep(0.1)
        with contextlib.suppress(FileNotFoundError):
            with open(_results_file_csv, "a") as out_f:
                # Simulate a block being written to the output
                _lines = [
                    _xeger.xeger(r"\d+")
                    + ","
                    + _xeger.xeger(r"\d+")
                    + ","
                    + _xeger.xeger(r"\d+\n")
                    for _ in range(random.randint(1, 4))
                ]
                out_f.writelines(_lines)
                counter_dict[dict_key] += len(_lines)
    time.sleep(1)
    termination_trigger.set()


@pytest.mark.scenario
@pytest.mark.parametrize(
    "mode", ("csv", "text")
)
def test_scenario_tail_file(mode: str) -> None:
    with multiprocessing.Manager() as manager:
        _trigger = multiprocessing.Event()
        _dummy_analysis_entry_counter = manager.dict()
        _parser_counter = manager.dict()
        _parser_counter[mode] = 0
        _dummy_analysis_entry_counter[mode] = 0
        def per_file_callback(
            data: dict[str, typing.Any], meta_data: dict[str, typing.Any],
            counter: dict[str, int]=_parser_counter,
            dict_key: str=mode
        ):
            print(f"Read input from file '{meta_data['file_name']}': {data}")
            counter[dict_key] += 1
        with tempfile.TemporaryDirectory() as temp_d:
            _process = multiprocessing.Process(
                target=run_dummy_analysis_text if mode == "text" else run_dummy_analysis_csv,
                args=(temp_d, _trigger, _dummy_analysis_entry_counter, mode)
            )
            _process.start()
            with multiparser.FileMonitor(per_thread_callback=per_file_callback, termination_trigger=_trigger) as monitor:
                if mode == "text":
                    monitor.tail(
                        path_glob_exprs=os.path.join(temp_d, "*.txt"),
                        tracked_values=[re.compile(r"value=(\d+)")],
                        labels=["entry"]
                    )
                else:
                    monitor.tail(
                        path_glob_exprs=os.path.join(temp_d, "*.csv"),
                        parser_func=mp_parse_tail.record_csv
                    )
                monitor.run()
            _process.join()

        assert _dummy_analysis_entry_counter[mode] > 0
        assert _parser_counter[mode] == _dummy_analysis_entry_counter[mode]


if __name__ in "__main__":
    test_scenario_tail_file()
