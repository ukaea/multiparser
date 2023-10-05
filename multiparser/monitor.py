import datetime
import glob
import os.path
import threading
import time
import typing

import loguru

import multiparser.parsing as cc_parse

__all__ = ["FileMonitor"]

LogFileRegexPair = typing.Tuple[str, typing.List[typing.Tuple[str | None, str]]]
FullFileTrackedValue = typing.Tuple[str, typing.List[str]]


class FileMonitor:
    def __init__(
        self,
        per_thread_callback: typing.Callable,
        interval: float = 1e-3,
        log_level: str = "INFO",
    ) -> None:
        """Create an instance of the file monitor for tracking file modifications.

        The main thread talks to the file monitor via a dictionary which holds information
        from the latest round of file parsing. When the main thread has processed the gathered
        information it will clear the entries preventing over-caching.

        Parameters
        ----------
        threads_recorder : typing.Dict
            Dictionary for caching values read from the file parsing threads
        per_thread_callback : typing.Callable
            function to be executed whenever a monitored file is modified
        var_list : List[str], optional
            list of variables to track from the given files
        interval : float, optional
            the refresh rate of the file monitors, by default 10.0 seconds
        """
        self._interval: float = interval
        self._per_thread_callback = per_thread_callback
        self._complete = threading.Event()
        self._known_files: typing.List[str] = []
        self._threads: typing.Dict[str | None, threading.Thread] = {}
        self._records: typing.List[typing.Tuple[str, str]] = []
        self._file_globex: typing.List[FullFileTrackedValue] = []
        self._log_globex: typing.List[LogFileRegexPair] = []
        self._excluded_patterns: typing.List[str] = []

    def _append_thread(
        self,
        file_name: str,
        tracked_values: typing.List[FullFileTrackedValue] | None = None,
        log_regular_expressions: typing.List[LogFileRegexPair] | None = None,
    ) -> None:
        def _read_action(
            callback: typing.Callable,
            file_name: str,
            termination_trigger: threading.Event,
            tracked_vals: typing.List[str] | None,
            line_regex: typing.List[typing.Tuple[str | None, str]] | None,
            interval: float,
            log: bool = False,
        ) -> None:
            while not termination_trigger.is_set():
                time.sleep(interval)

                # If the file does not exist yet then continue
                if not os.path.exists(file_name):
                    continue

                _modified_time_stamp = os.path.getmtime(file_name)
                _modified_time = datetime.datetime.fromtimestamp(
                    _modified_time_stamp
                ).strftime("%Y-%M-%d %H:%M:%S.%f")

                # If the file has not been modified then we do not need to parse it
                if (_modified_time, file_name) in self._records:
                    continue

                if not log:
                    _meta, _data = cc_parse.record_file(file_name, tracked_vals)
                else:
                    _meta, _data = cc_parse.record_log(file_name, line_regex)

                callback(_data, _meta)
                self._records.append((_modified_time, file_name))

        self._threads[file_name] = threading.Thread(
            target=_read_action,
            args=(
                self._per_thread_callback,
                file_name,
                self._complete,
                tracked_values,
                log_regular_expressions,
                self._interval,
                log_regular_expressions is not None,
            ),
        )

    def _create_monitor_threads(self) -> None:
        def _full_file_monitor_func(
            glob_exprs: typing.List[FullFileTrackedValue],
            exc_glob_exprs: typing.List[str],
            thread_launch_func: typing.Callable,
            threads: typing.Dict[str | None, threading.Thread],
            termination_trigger: threading.Event,
            interval: float,
            file_list: typing.List[str],
            message_callback: typing.Callable = lambda item: loguru.logger.info(
                f"Found NEW file '{item}'"
            ),
        ) -> None:
            while not termination_trigger.is_set():
                time.sleep(interval)
                _excludes: typing.List[str] = []
                for expr in exc_glob_exprs:
                    _excludes += glob.glob(expr)
                for expr, tracked_values in glob_exprs:
                    for file in glob.glob(expr):
                        if file not in threads and file not in _excludes:
                            message_callback(file)
                            file_list.append(file)
                            thread_launch_func(file, tracked_values=tracked_values)
                            threads[file].start()

        def _file_tail_monitor_func(
            glob_exprs: typing.List[LogFileRegexPair],
            exc_glob_exprs: typing.List[str],
            thread_launch_func: typing.Callable,
            threads: typing.Dict[str | None, threading.Thread],
            termination_trigger: threading.Event,
            interval: float,
            file_list: typing.List[str],
            message_callback: typing.Callable = lambda item: loguru.logger.info(
                f"Found NEW log '{item}'"
            ),
        ) -> None:
            while not termination_trigger.is_set():
                time.sleep(interval)
                _excludes: typing.List[str] = []
                for expr in exc_glob_exprs:
                    _excludes += glob.glob(expr)
                for expr, reg_lab_expr_pairing in glob_exprs:
                    for file in glob.glob(expr):
                        if file not in threads and file not in _excludes:
                            message_callback(file)
                            file_list.append(file)
                            thread_launch_func(
                                file, log_regular_expressions=reg_lab_expr_pairing
                            )
                            threads[file].start()

        self._threads["__FULL_FILE_MONITOR__"] = threading.Thread(
            target=_full_file_monitor_func,
            args=(
                self._file_globex,
                self._excluded_patterns,
                self._append_thread,
                self._threads,
                self._complete,
                self._interval,
                self._known_files,
            ),
        )

        self._threads["__LOG_FILE_MONITOR__"] = threading.Thread(
            target=_file_tail_monitor_func,
            args=(
                self._log_globex,
                self._excluded_patterns,
                self._append_thread,
                self._threads,
                self._complete,
                self._interval,
                self._known_files,
            ),
        )

    def exclude(self, path_glob_exprs: typing.List[str] | str) -> None:
        if isinstance(path_glob_exprs, str):
            self._excluded_patterns.append(path_glob_exprs)
        else:
            self._excluded_patterns += path_glob_exprs

    def track(
        self,
        path_glob_exprs: typing.List[str] | str,
        tracked_values: typing.List[str] | None = None,
    ) -> None:
        if isinstance(path_glob_exprs, str):
            self._file_globex.append((path_glob_exprs, tracked_values))
        else:
            self._file_globex += [(g, tracked_values) for g in path_glob_exprs]

    def tail(
        self,
        path_glob_exprs: typing.List[str] | str,
        regular_exprs: typing.List[str] | None = None,
        labels: typing.List[str] | None = None,
    ) -> None:
        if labels and len(labels) != len(regular_exprs):
            raise AssertionError(
                "Number of labels must match number of regular expressions in 'tail'."
            )
        if regular_exprs:
            labels = labels or [None] * len(regular_exprs)
            _reg_lab_expr_pairing: typing.List[typing.Tuple[str | None, str]] | None = [
                (label, reg_ex) for label, reg_ex in zip(labels, regular_exprs)
            ]
        else:
            _reg_lab_expr_pairing = None

        if isinstance(path_glob_exprs, str):
            self._log_globex.append((path_glob_exprs, _reg_lab_expr_pairing))
        else:
            self._log_globex += [(g, _reg_lab_expr_pairing) for g in path_glob_exprs]

    def terminate(self) -> None:
        self._complete.set()
        for thread in self._threads.values():
            thread.join()

    def run(self) -> None:
        self._threads["__FULL_FILE_MONITOR__"].start()
        self._threads["__LOG_FILE_MONITOR__"].start()

    def __enter__(self) -> "FileMonitor":
        self._create_monitor_threads()
        return self

    def __exit__(self, *args, **kwargs) -> None:
        self._complete.set()
