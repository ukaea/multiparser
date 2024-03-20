import datetime
import multiprocessing.synchronize
import os.path
import random
import tempfile
import time
import re
import typing

import pytest
import xeger

import multiparser


def run_dummy_analysis(
    output_dir: str, termination_trigger: multiprocessing.synchronize.Event
) -> None:
    """Run analysis where a single file is written containing increasing data blocks"""
    _results_file_csv: str = os.path.join(output_dir, "out.csv")
    _xeger = xeger.Xeger()
    while not termination_trigger.is_set():
        time.sleep(0.1)
        try:
            with open(_results_file_csv, "a") as out_f:
                # Simulate a block being written to the output
                out_f.writelines(
                    [
                        datetime.datetime.now().strftime("%Y-%M-%d %H:%M:%S\tvalue=")
                        + _xeger.xeger(r"\d+")
                        + "\t"
                        + _xeger.xeger(r"\d+")
                        + _xeger.xeger(r"\d+")
                        + "\n"
                        for _ in range(random.randint(1, 4))
                    ]
                )
        except FileNotFoundError:
            pass


def per_file_callback(
    data: dict[str, typing.Any], meta_data: dict[str, typing.Any]
):
    print(f"Read input from file '{meta_data['file_name']}': {data}")


@pytest.mark.scenario
def test_scenario_1() -> None:
    _trigger = multiprocessing.Event()
    with tempfile.TemporaryDirectory() as temp_d:
        _process = multiprocessing.Process(
            target=run_dummy_analysis, args=(temp_d, _trigger)
        )
        _process.start()
        with multiparser.FileMonitor(per_thread_callback=per_file_callback) as monitor:
            monitor.tail(
                path_glob_exprs=os.path.join(temp_d, "*.csv"),
                tracked_values=[re.compile(r"value=(\d+)")],
                labels=["entry"]
            )
            monitor.run()
            time.sleep(10)
            monitor.terminate()
        _trigger.set()
        _process.join()


if __name__ in "__main__":
    test_scenario_1()
