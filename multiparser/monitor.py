import datetime
import glob
import os.path
import threading
import time
import typing

import multiparser.parsing as cc_parse

__all__ = ["FileMonitor"]


class FileMonitor:
    def __init__(
        self, per_thread_callback: typing.Callable, interval: float = 10.0
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
        self._known_files: typing.Dict[str, typing.List[str] | None] = {}
        self._threads: typing.Dict[str | None, threading.Thread] = {}
        self._records: typing.List[typing.Tuple[str, str]] = []
        self._file_globex: typing.List[typing.Tuple[str, typing.List[str]]] = []

    def _append_thread(
        self, file_name: str, tracked_values: typing.List[str] | None = None
    ) -> None:
        def _read_action(
            callback: typing.Callable,
            file_name: str,
            termination_trigger: threading.Event,
            tracked_vals: typing.List[str] | None,
            interval: float,
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
                _meta, _data = cc_parse.record_file(file_name, tracked_vals)
                callback(_data, _meta)
                self._records.append((_modified_time, file_name))

        self._threads[file_name] = threading.Thread(
            target=_read_action,
            args=(
                self._per_thread_callback,
                file_name,
                self._complete,
                tracked_values,
                self._interval,
            ),
        )

    def _create_monitor_thread(self) -> None:
        def _monitor_func(
            glob_exprs: typing.List[str],
            thread_launch_func: typing.Callable,
            threads: typing.Dict[str | None, threading.Thread],
            termination_trigger: threading.Event,
            interval: float,
        ) -> None:
            while not termination_trigger.is_set():
                time.sleep(interval)
                for expr, tracked_values in glob_exprs:
                    for file in glob.glob(expr):
                        if file not in threads:
                            thread_launch_func(file, tracked_values)
                            threads[file].start()

        self._threads[None] = threading.Thread(
            target=_monitor_func,
            args=(
                self._file_globex,
                self._append_thread,
                self._threads,
                self._complete,
                self._interval,
            ),
        )

    def track(
        self,
        glob_exprs: typing.List[str] | str,
        tracked_values: typing.List[str] | None = None,
    ) -> None:
        if isinstance(glob_exprs, str):
            self._file_globex.append((glob_exprs, tracked_values))
        else:
            self._file_globex += [(g, tracked_values) for g in glob_exprs]

    def terminate(self) -> None:
        self._complete.set()
        for thread in self._threads.values():
            thread.join()

    def run(self) -> None:
        self._threads[None].start()

    def __enter__(self) -> "FileMonitor":
        self._create_monitor_thread()
        return self

    def __exit__(self, *args, **kwargs) -> None:
        self._complete.set()
