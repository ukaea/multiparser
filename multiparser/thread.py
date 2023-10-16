import datetime
import functools
import glob
import os.path
import threading
import time
import typing

import loguru

import multiparser.exceptions as mp_exc
import multiparser.parsing as mp_parse
from multiparser.typing import FullFileTrackedValue, LogFileRegexPair


class HandledThread(threading.Thread):
    def __init__(
        self,
        terminate_all_on_failure: bool = False,
        task_identifier: str | None = None,
        *args,
        **kwargs,
    ) -> None:
        self.terminate_all_on_failure: bool = terminate_all_on_failure
        self.task_description: str | None = task_identifier
        self.exception: BaseException | None = None
        super().__init__(*args, **kwargs)
        self._wrap_target()

    def _wrap_target(self) -> None:
        def wrapper(func: typing.Callable) -> typing.Callable:
            def _inner(*args, **kwargs) -> typing.Any:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    self.exception = e

            return _inner

        self._target: typing.Callable = wrapper(self._target)


def abort_on_fail(function: typing.Callable) -> typing.Callable:
    @functools.wraps(function)
    def _wrapper(self: "FileThreadLauncher", *args, **kwargs) -> typing.Any:
        try:
            return function(self, *args, **kwargs)
        except Exception as e:
            self._termination_trigger.set()
            raise e

    return _wrapper


class FileThreadLauncher:
    def __init__(
        self,
        file_thread_callback: typing.Callable,
        file_thread_lock: threading.Lock,
        file_thread_termination_trigger: threading.Event,
        parsing_callback: typing.Callable,
        notification_callback: typing.Callable,
        refresh_interval: float,
        trackables: typing.List[LogFileRegexPair | FullFileTrackedValue],
        exclude_files_globex: typing.List[str] | None,
        file_list: typing.List[str] | None = None,
    ) -> None:
        self._trackables: typing.List[
            LogFileRegexPair | FullFileTrackedValue
        ] = trackables
        self._per_thread_callback: typing.Callable = file_thread_callback
        self._lock: threading.Lock = file_thread_lock
        self._termination_trigger: threading.Event = file_thread_termination_trigger
        self._parsing_callback: typing.Callable = parsing_callback
        self._notifier: typing.Callable = notification_callback
        self._file_threads: typing.Dict[str, HandledThread] = {}
        self._exclude_globex: typing.List[str] | None = exclude_files_globex
        self._records: typing.List[typing.Tuple[str, str]] = []
        self._interval = refresh_interval
        self._monitored_files = file_list or []

    def _append_thread(
        self, file_name: str, tracked_values: LogFileRegexPair | FullFileTrackedValue
    ) -> None:
        def _read_action(
            monitor_callback: typing.Callable = self._per_thread_callback,
            file_name: str = file_name,
            termination_trigger: threading.Event = self._termination_trigger,
            records: typing.List[typing.Tuple[str, str]] = self._records,
            interval: float = self._interval,
            tracked_vals: LogFileRegexPair | FullFileTrackedValue = tracked_values,
            lock: threading.Lock = self._lock,
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
                if (_modified_time, file_name) in records:
                    continue

                _meta, _data = self._parsing_callback(file_name, tracked_vals)

                with lock:
                    monitor_callback(_data, _meta)
                records.append((_modified_time, file_name))

        self._file_threads[file_name] = HandledThread(target=_read_action)

    @abort_on_fail
    def run(self) -> None:
        while not self._termination_trigger.is_set():
            time.sleep(self._interval)
            _excludes: typing.List[str] = []
            for expr in self._exclude_globex or []:
                _excludes += glob.glob(expr)
            for expr, tracked_values in self._trackables:
                for file in glob.glob(expr):
                    if file not in self._file_threads and file not in _excludes:
                        self._notifier(file)
                        self._monitored_files.append(file)
                        self._append_thread(file, tracked_values)
                        self._file_threads[file].start()

    def _raise_exceptions(self) -> None:
        if not any(
            (
                _exceptions := {
                    name: thread.exception
                    for name, thread in self._file_threads.items()
                }
            ).values()
        ):
            return
        raise mp_exc.SessionFailure(_exceptions)

    def abort(self) -> None:
        self._raise_exceptions()


class LogFileThreadLauncher(FileThreadLauncher):
    def __init__(
        self,
        trackables: typing.List[LogFileRegexPair],
        file_thread_callback: typing.Callable,
        file_thread_lock: threading.Lock,
        file_thread_termination_trigger: threading.Event,
        refresh_interval: float,
        exclude_files_globex: typing.List[str] | None,
        file_list: typing.List[str] | None = None,
    ) -> None:

        super().__init__(
            file_thread_callback=file_thread_callback,
            parsing_callback=mp_parse.record_log,
            refresh_interval=refresh_interval,
            file_thread_lock=file_thread_lock,
            trackables=trackables,
            notification_callback=lambda item: loguru.logger.info(
                f"Found NEW log '{item}'"
            ),
            exclude_files_globex=exclude_files_globex,
            file_list=file_list,
            file_thread_termination_trigger=file_thread_termination_trigger,
        )


class FullFileThreadLauncher(FileThreadLauncher):
    def __init__(
        self,
        trackables: typing.List[LogFileRegexPair],
        file_thread_callback: typing.Callable,
        file_thread_lock: threading.Lock,
        file_thread_termination_trigger: threading.Event,
        refresh_interval: float,
        exclude_files_globex: typing.List[str] | None,
        file_list: typing.List[str] | None = None,
    ) -> None:

        super().__init__(
            file_thread_callback=file_thread_callback,
            parsing_callback=mp_parse.record_file,
            refresh_interval=refresh_interval,
            trackables=trackables,
            notification_callback=lambda item: loguru.logger.info(
                f"Found NEW file '{item}'"
            ),
            exclude_files_globex=exclude_files_globex,
            file_list=file_list,
            file_thread_lock=file_thread_lock,
            file_thread_termination_trigger=file_thread_termination_trigger,
        )
