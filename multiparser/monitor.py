import datetime
import os.path
import threading
import time
import typing

import multiparser.parsing as cc_parse


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
        self._files: typing.Dict[str, typing.List[str] | None] = {}
        self._threads: typing.Dict[str, threading.Thread] = {}
        self._records: typing.List[typing.Tuple[str, str]] = []

    def _prepare_threads(self) -> None:
        for file, tracked_items in self._files.items():

            def _read_action(
                callback: typing.Callable,
                file_name: str,
                termination_trigger: threading.Event,
                tracked_info: typing.Dict[str, typing.Any],
            ) -> None:
                while not termination_trigger.is_set():
                    time.sleep(self._interval)
                    _modified_time_stamp = os.path.getmtime(file_name)
                    _modified_time = datetime.datetime.fromtimestamp(
                        _modified_time_stamp
                    ).strftime("%Y-%M-%d %H:%M:%S.%f")

                    # If the file has not been modified then we do not need to parse it
                    if (_modified_time, file_name) in self._records:
                        continue
                    _meta, _data = cc_parse.record_file(file_name, **tracked_info)
                    callback(_data, _meta)
                    self._records.append((_modified_time, file_name))

            # Create a thread for each file to be read
            self._threads[file] = threading.Thread(
                target=_read_action,
                args=(self._per_thread_callback, file, self._complete, tracked_items),
            )

    def track(
        self,
        file_name: str,
        values: typing.List[str] | None = None,
        regex: typing.List[str] | None = None,
    ) -> None:
        self._files[file_name] = {"tracked_values": values, "tracked_regex": regex}

    def terminate(self) -> None:
        self._complete.set()
        for thread in self._threads.values():
            thread.join()

    def run(self) -> None:
        self._prepare_threads()
        self._running = True
        for thread in self._threads.values():
            thread.start()

    def __enter__(self) -> "FileMonitor":
        return self

    def __exit__(self, *args, **kwargs) -> None:
        self._running = False
