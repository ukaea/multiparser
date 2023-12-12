import json
import logging
import os
import random
import re
import tempfile
import time
import typing
import dataclasses

import pandas
import pytest
import pytest_mock
import multiprocessing
import toml
import xeger
from conftest import fake_csv, fake_nml, fake_toml, to_nml

import multiparser
import multiparser.exceptions as mp_exc
import multiparser.thread as mp_thread
import multiparser.parsing as mp_parse
from tests.conftest import fake_feather, fake_json, fake_parquet, fake_pickle, fake_yaml
from multiparser.parsing.tail import record_with_delimiter as tail_record_delimited


DATA_LIBRARY: str = os.path.join(os.path.dirname(__file__), "data")


@pytest.mark.monitor
@pytest.mark.parametrize(
    "exception",
    (
        "file_thread_exception",
        "file_monitor_thread_exception",
        "log_monitor_thread_exception",
        None,
    ),
)
@pytest.mark.parametrize(
    "fake_log", [
        (True, None)
    ],
    indirect=True,
)
def test_run_on_directory_all(
    fake_log, exception: str | None, mocker: pytest_mock.MockerFixture
) -> None:
    _interval: float = 0.1
    _fakers: typing.Tuple[typing.Callable, ...] = (
        fake_csv,
        fake_nml,
        fake_toml,
        fake_feather,
        fake_json,
        fake_yaml,
        fake_pickle,
        fake_parquet
    )
    with tempfile.TemporaryDirectory() as temp_d:
        for faker in _fakers:
            faker(temp_d)
        for _ in range(8):
            random.choice(_fakers)(temp_d)

        def per_thread_callback(_, __, exception=exception):
            if exception == "file_thread_exception":
                raise TypeError("Oh dear!")

        @mp_thread.handle_monitor_thread_exception
        def fail_run(*_):
            raise AssertionError("Oh dear!")

        if exception in (
            "file_monitor_thread_exception",
            "log_monitor_thread_exception",
        ):
            mocker.patch.object(mp_thread.FileThreadLauncher, "run", fail_run)

        _allowed_exception = None

        if exception:
            if exception == "file_thread_exception":
                _allowed_exception = mp_exc.SessionFailure
            else:
                _allowed_exception = AssertionError

            with pytest.raises(_allowed_exception):
                with multiparser.FileMonitor(
                    per_thread_callback,
                    interval=_interval,
                    log_level=logging.INFO,
                    terminate_all_on_fail=True
                ) as monitor:
                    monitor.track(os.path.join(temp_d, "*"))
                    monitor.exclude(os.path.join(temp_d, "*.toml"))
                    monitor.tail(**fake_log)
                    monitor.run()
                    for _ in range(10):
                        time.sleep(_interval)
                    monitor.terminate()
        else:
            with multiparser.FileMonitor(
                per_thread_callback,
                interval=_interval,
                log_level=logging.INFO,
                terminate_all_on_fail=True
            ) as monitor:
                monitor.track(os.path.join(temp_d, "*"))
                monitor.exclude(os.path.join(temp_d, "*.toml"))
                monitor.tail(**fake_log)
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
            per_thread_callback,
            interval=_interval,
            flatten_data=True,
            terminate_all_on_fail=True
        ) as monitor:
            monitor.track(_csv_file, ["d_other", re.compile("\w_value")])
            monitor.track(_nml_file, [re.compile("\w_val_\w")])
            monitor.track(_toml_file, ["input_swe", re.compile(r"input_\d")])
            monitor.run()
            for _ in range(10):
                time.sleep(_interval)
            monitor.terminate()


@pytest.mark.parsing
@pytest.mark.parametrize(
    "stage,contains",
    [[1, ("matrix", "k", "v_sync", "i(1)", "i(2)")]],
    ids=[f"stage_{i}" for i in range(1, 2)],
)
def test_custom_data(stage: int, contains: typing.Tuple[str, ...]) -> None:
    _file: str = os.path.join(DATA_LIBRARY, f"custom_output_stage_{stage}.dat")

    @mp_parse.file_parser
    def _parser_func(
        input_file: str
    ) -> typing.Tuple[typing.Dict[str, typing.Any], typing.Dict[str, typing.Any]]:
        _get_matrix = r"^[(\d+.\d+) *]{16}$"
        _initial_params_regex = r"^([\w_\(\)]+)\s*=\s*(\d+\.*\d*)$"
        _out_data = {}
        with open(input_file) as in_f:
            _file_data = in_f.read()
            _matrix_iter = re.finditer(_get_matrix, _file_data, re.MULTILINE)
            _init_params_iter = re.finditer(
                _initial_params_regex, _file_data, re.MULTILINE
            )

            _matrix = []
            for result in _matrix_iter:
                _matrix.append([float(i) for i in str(result.group()).split(" ")])
            _out_data["matrix"] = _matrix

            for result in _init_params_iter:
                _key = result.group(1)
                _value = result.group(2)
                _out_data[_key] = float(_value)

            if not _out_data:
                raise AssertionError("Failed to retrieve any values")

        return {}, _out_data

    _expected = {
        "matrix": [
            [10.0, 2.0, 3.0, 4.0],
            [2.0, 10.0, 2.5, 8.0],
            [3.0, 2.5, 10.0, 1.0],
            [4.0, 8.0, 1.0, 10.0],
        ],
        "k": 5.81,
        "v_sync": 4.2389,
        "i(1)": 3,
        "i(2)": 9.81,
    }

    def _validation_callback(data, _, check=contains):
        for key, value in _expected.items():
            if key in check:
                assert data[key] == value

    with multiparser.FileMonitor(
        _validation_callback,
        interval=0.1,
        log_level=logging.DEBUG,
        terminate_all_on_fail=True
    ) as monitor:
        monitor.track(
            _file,
            parser_func=_parser_func,
            tracked_values=list(_expected.keys()),
            static=True,
        )
        monitor.run()
        time.sleep(2)
        monitor.terminate()


@pytest.mark.parsing
def test_parse_log_in_blocks() -> None:
    _refresh_interval: float = 0.1
    _expected = [{f"var_{i}": random.random() * 10 for i in range(5)} for _ in range(10)]
    _xeger = xeger.Xeger()
    _file_blocks = []
    _gen_ignore_pattern = r"<!--ignore-this-\w+-\d+-->"
    _gen_rgx = r"\w+: \d+\.\d+"
    _file_blocks += [
        [_xeger.xeger(_gen_rgx)+"\n"] +
        [_xeger.xeger(_gen_rgx)+ "\n"] +
        [_xeger.xeger(_gen_rgx)+"\n"] +
        [_xeger.xeger(_gen_rgx)+"\n"] +
        [_xeger.xeger(_gen_ignore_pattern)+"\n"] +
        ["\tData Out\n"] +
        [f"\tResult: {i['var_0']}\n"] +
        [f"\tMetric: {i['var_1']}\n"] +
        [f"\tNormalised: {i['var_2']}\n"] +
        [f"\tAccuracy: {i['var_3']}\n"] +
        [f"\tDeviation: {i['var_4']}\n"]
        for i in _expected
    ]

    def run_simulation(out_file: str, trigger, file_content: typing.List[typing.List[str]]=_file_blocks, interval:float=_refresh_interval) -> None:
        for block in file_content:
            time.sleep(interval)
            with open(out_file, "a") as out_f:
                out_f.writelines(block)
        trigger.set()

    @dataclasses.dataclass
    class Counter:
        value: int = 0

    _counter = Counter()

    def callback_check(data, _, comparison=_expected, counter=_counter) -> None:
        for key, value in data.items():
            assert value == comparison[counter.value][key]
        counter.value += 1

    @mp_parse.log_parser
    def parser_func(file_data: str, **_) -> typing.Tuple[typing.Dict[str, typing.Any], ...]:
        _regex_search_str = r"\s*Data Out\n\s*Result:\ (\d+\.\d+)\n\s*Metric:\ (\d+\.\d+)\n\s*Normalised:\ (\d+\.\d+)\n\s*Accuracy:\ (\d+\.\d+)\n\s*Deviation:\ (\d+\.\d+)"

        _parser = re.compile(_regex_search_str, re.MULTILINE)
        _out_data = []

        for match_group in _parser.finditer(file_data):
            _out_data += [
                {f"var_{i}": float(match_group.group(i+1)) for i in range(5)}
            ]
        return {}, _out_data

    with tempfile.NamedTemporaryFile(suffix=".log") as temp_f:
        _termination_trigger = multiprocessing.Event()
        _process = multiprocessing.Process(target=run_simulation, args=(temp_f.name,_termination_trigger))

        with multiparser.FileMonitor(
            per_thread_callback=callback_check,
            termination_trigger=_termination_trigger,
            interval=0.1*_refresh_interval,
            log_level=logging.DEBUG,
            terminate_all_on_fail=True
        ) as monitor:
            monitor.tail(
                [temp_f.name],
                parser_func=parser_func,
                skip_lines_w_pattern=[re.compile(_gen_ignore_pattern)]
            )
            _process.start()
            monitor.run()
            _process.join()


@pytest.mark.parsing
@pytest.mark.parametrize(
    "delimiter", (",", " "),
    ids=("comma", "whitespace")
)
@pytest.mark.parametrize(
    "explicit_headers", ("no_headers", "headers", "headers_search")
)
def test_parse_delimited_in_blocks(delimiter, explicit_headers) -> None:
    _refresh_interval: float = 0.1
    _xeger = xeger.Xeger()

    # Cases where user provides the headers, or they are read as first line in file
    if explicit_headers == "headers":
        _headers = [f"num_{i}" for i in range(5)]
        _header_search = None
        _expected = [{k: random.random() * 10 for k in _headers} for _ in range(40)]
        _file_blocks = []
    elif explicit_headers == "headers_search":
        _headers = None
        _header_search = re.compile(r"var_", re.IGNORECASE)
        _expected = [{f"var_{i}": random.random() * 10 for i in range(5)} for _ in range(40)]
        _file_blocks = [
            _xeger.xeger("\w+\s\w+") + "\n" for _ in range(2) 
        ]
    else:
        _headers = None
        _header_search = None
        _expected = [{f"var_{i}": random.random() * 10 for i in range(5)} for _ in range(40)]
        _file_blocks = [delimiter.join(f"var_{i}" for i in range(5)) + "\n"]

    _gen_ignore_pattern = r"<!--ignore-this-\w+-\d+-->"
    _file_blocks += [_xeger.xeger(_gen_ignore_pattern) + "\n"]

    if explicit_headers == "headers_search":
        _file_blocks += [delimiter.join(f"var_{i}" for i in range(5)) + "\n"]

    _file_blocks += [
        delimiter.join(map(str, row.values())) + "\n"
        for row in _expected
    ]

    @dataclasses.dataclass
    class Counter:
        value: int = 0

    _counter = Counter()

    def run_simulation(out_file: str, trigger, file_content: typing.List[typing.List[str]]=_file_blocks, interval:float=_refresh_interval) -> None:
        current_line = 0
        while current_line + (n_lines := random.randint(4, 6)) < len(file_content):
            time.sleep(interval)
            with open(out_file, "a") as out_f:
                out_f.writelines(file_content[current_line:current_line+n_lines])
            current_line += n_lines
        trigger.set()

    def callback_check(data, _, comparison=_expected, counter=_counter) -> None:
        for key, value in data.items():
            assert value == comparison[counter.value][key]
        counter.value += 1

    with tempfile.NamedTemporaryFile(suffix=".csv") as temp_f:
        _termination_trigger = multiprocessing.Event()
        _process = multiprocessing.Process(target=run_simulation, args=(temp_f.name,_termination_trigger))

        with multiparser.FileMonitor(
            per_thread_callback=callback_check,
            termination_trigger=_termination_trigger,
            interval=0.1*_refresh_interval,
            log_level=logging.DEBUG,
            terminate_all_on_fail=True
        ) as monitor:
            monitor.tail(
                [temp_f.name],
                parser_func=tail_record_delimited,
                parser_kwargs={"delimiter": delimiter, "headers": _headers, "header_pattern": _header_search},
                skip_lines_w_pattern=[re.compile(_gen_ignore_pattern)]
            )
            _process.start()
            monitor.run()
            _process.join()


@pytest.mark.parsing
def test_parse_h5() -> None:
    _data_file: str = os.path.join(DATA_LIBRARY, "example.h5")

    def parser_func(file_name: str):
        return pandas.read_hdf(file_name, key={"key": "my_group/my_dataset"}).to_dict()

    with multiparser.FileMonitor(
        per_thread_callback=lambda *_, **__: (),
        log_level=logging.DEBUG,
        terminate_all_on_fail=True
    ) as monitor:
        monitor.track(
            _data_file,
            parser_func=parser_func,
            static=True
        )
        monitor.run()
        monitor.terminate()
    