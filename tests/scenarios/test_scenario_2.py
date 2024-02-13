import datetime
import multiprocessing.synchronize
import os.path
import random
import tempfile
import time
import typing
import re

import pytest
import xeger

import multiparser


def run_dummy_analysis(
    output_dir: str, termination_trigger: multiprocessing.synchronize.Event
) -> None:   
    _xeger = xeger.Xeger()
    time_step: float = 0.1
    lines_per_file: int = 4

    while not termination_trigger.is_set():
        time.sleep(time_step)
        time_str = f"{time_step:.1f}".replace('.', '_')
        _results_file_csv: str = os.path.join(output_dir, f"out_{time_str}.csv")
        try:
            with open(_results_file_csv, "a") as out_f:
                # Simulate a block being written to the output
                for _ in range(lines_per_file):
                    out_f.writelines(
                        [
                            datetime.datetime.now().strftime(f"%Y-%M-%d %H:%M:%S\tvalue_{time_str}=")
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
        time_step += 0.1


def per_file_callback(
    data: typing.Dict[str, typing.Any], meta_data: typing.Dict[str, typing.Any]
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
            monitor.tail(os.path.join(temp_d, "*.csv"), [re.compile(r"(value_\d+_\d+)=(\d+)")])
            monitor.run()
            time.sleep(10)
            monitor.terminate()
        _trigger.set()
        _process.join()


if __name__ in "__main__":
    test_scenario_1()