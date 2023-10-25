"""
Multiparser Threading
=====================

Contains methods and classes for the creation of threads towards the
monitoring of output files. These are broken down into two types, log files
which are files requiring only the read of the latest line, and full files
which are defined as those requiring the whole file to be re-read on modification

"""
__date__ = "2023-10-16"
__author__ = "Kristian Zarebski"
__maintainer__ = "Kristian Zarebski"
__email__ = "kristian.zarebski@ukaea.uk"
__copyright__ = "Copyright 2023, United Kingdom Atomic Energy Authority"


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
    """Thread with Exception capture

    Extension to the built-in Thread type storing any exception
    throw information so it can be handled
    """

    def __init__(
        self,
        throw_callback: typing.Callable | None = None,
        task_identifier: str | None = None,
        *args,
        **kwargs,
    ) -> None:
        """Initialise a thread with exception capture.

        Parameters
        ----------
        task_identifier : str | None, optional
            a unique identifier for this thread, by default None
        """
        self.task_description: str | None = task_identifier
        self.exception: BaseException | None = None
        self.callback = throw_callback
        super().__init__(*args, **kwargs)
        self._wrap_target()

    def _wrap_target(self) -> None:
        """Wrap the thread target in order to store any exception throws"""

        def wrapper(func: typing.Callable) -> typing.Callable:
            def _inner(*args, **kwargs) -> typing.Any:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if self.callback:
                        self.callback(e)
                    self.exception = e

            return _inner

        self._target: typing.Callable = wrapper(self._target)


def abort_on_fail(function: typing.Callable) -> typing.Callable:
    """Decorator for setting termination event variable on failure.

    A decorator has been used to assist testing of the underlying
    functionality of the file thread launcher super-class.

    Parameters
    ----------
    function : typing.Callable
        the class method to trigger thread termination on failure

    Returns
    -------
    typing.Callable
        modified method
    """

    @functools.wraps(function)
    def _wrapper(self: "FileThreadLauncher", *args, **kwargs) -> typing.Any:
        try:
            return function(self, *args, **kwargs)
        except Exception as e:
            self._termination_trigger.set()
            raise e

    return _wrapper


class FileThreadLauncher:
    """Base class for all file monitor thread launchers.

    Spins up threads whenever a new file matching a given set of globular
    expressions is created in order to monitor any changes to that file.
    """

    def __init__(
        self,
        file_thread_callback: typing.Callable,
        file_thread_termination_trigger: threading.Event,
        parsing_callback: typing.Callable,
        notification_callback: typing.Callable,
        refresh_interval: float,
        trackables: typing.List[LogFileRegexPair | FullFileTrackedValue],
        exclude_files_globex: typing.List[str] | None,
        exception_callback: typing.Callable | None = None,
        file_thread_lock: typing.Any | None = None,
        file_list: typing.List[str] | None = None,
    ) -> None:
        """Create a new instance of the file monitor thread launcher.

        Parameters
        ----------
        file_thread_callback : typing.Callable
            the callback function to be triggered whenever a change is detected
            to a monitored file
        file_thread_termination_trigger : threading.Event
            threading event which when set will trigger termination of all file
            monitor loops
        parsing_callback : typing.Callable
            function to be called to parse the monitored files
        notification_callback : typing.Callable
            function called to notify when a new file is detected
        refresh_interval : float
            how often to check for new files
        trackables : typing.List[LogFileRegexPair  |  FullFileTrackedValue]
            a tuple containing:
                - globular expression for file capture
                - regular_expressions for variable tracking within files
                - whether the file is static (written once) or changing
        exclude_files_globex : typing.List[str] | None
            a list of globular expressions for files to exclude
        exception_callback : typing.Callable | None, optional
            function to call when an exception is thrown
        file_list : typing.List[str] | None, optional
            container to append any found file names, by default None
        file_thread_lock : threading.Lock, optional
            shared mutex to prevent the callback being called simultaneously by
            multiple threads.
        """
        self._trackables: typing.List[
            LogFileRegexPair | FullFileTrackedValue
        ] = trackables
        self._per_thread_callback: typing.Callable = file_thread_callback
        self._exception_callback: typing.Callable | None = exception_callback
        self._lock: typing.Any | None = file_thread_lock
        self._termination_trigger: threading.Event = file_thread_termination_trigger
        self._parsing_callback: typing.Callable = parsing_callback
        self._notifier: typing.Callable = notification_callback
        self._file_threads: typing.Dict[str, HandledThread] = {}
        self._exclude_globex: typing.List[str] | None = exclude_files_globex
        self._records: typing.List[typing.Tuple[str, str]] = []
        self._interval = refresh_interval
        self._monitored_files = file_list if file_list is not None else []

    def _append_thread(
        self,
        file_name: str,
        tracked_values: LogFileRegexPair | FullFileTrackedValue,
        static: bool = False,
        custom_parser: typing.Callable | None = None,
        convert: bool = True,
        **_,
    ) -> None:
        """Create a new thread for a monitored file

        Parameters
        ----------
        file_name : str
            name of the file to observe
        tracked_values : LogFileRegexPair | FullFileTrackedValue
            values to monitor within the given file
        static : bool, optional
            whether the file is written only once, by default False
        custom_parser : typing.Callable | None, optional
            use a user defined parser instead
        convert : bool, optional
            convert values from string to numeric where appropriate
        """

        def _read_action(
            records: typing.List[typing.Tuple[str, str]],
            monitor_callback: typing.Callable = self._per_thread_callback,
            file_name: str = file_name,
            termination_trigger: threading.Event = self._termination_trigger,
            interval: float = self._interval,
            tracked_vals: LogFileRegexPair | FullFileTrackedValue = tracked_values,
            lock: typing.Any | None = self._lock,
            static_read: bool = static,
            cstm_parser: typing.Callable | None = custom_parser,
        ) -> None:

            _cached_metadata: typing.Dict[str, str | int] = {}

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

                # Pass previous cached metadata to the parser in case required
                _parsed = self._parsing_callback(
                    file_name,
                    tracked_vals,
                    custom_parser=cstm_parser,
                    convert=convert,
                    **_cached_metadata,
                )

                # Some parsers return multiple results, e.g. those parsing multiple file lines
                _parsed_list = [_parsed] if not isinstance(_parsed, list) else _parsed

                for _meta, _data in _parsed_list:
                    # Keep latest
                    _cached_metadata = _meta

                    if not _data:
                        continue
                    loguru.logger.debug(f"{file_name}: Recorded: {_data}")
                    if lock:
                        with lock:
                            monitor_callback(_data, _meta)
                    else:
                        monitor_callback(_data, _meta)

                records.append((_modified_time, file_name))

                # If only a single read is requirement terminate loop
                if static_read:
                    break

        self._file_threads[file_name] = HandledThread(
            target=_read_action, args=(self._records,)
        )

    @abort_on_fail
    def run(self) -> None:
        """Start the thread launcher"""
        while not self._termination_trigger.is_set():
            time.sleep(self._interval)
            _excludes: typing.List[str] = []
            for expr in self._exclude_globex or []:
                _excludes += glob.glob(expr)
            for trackable in self._trackables:
                # Check for multiple tracking entries for the same file
                # not allowed due to constraint of one thread spawned per file
                _registered_files: typing.List[str] = []
                for file in glob.glob(trackable["glob_exprs"]):
                    if file in _registered_files:
                        raise AssertionError(
                            "Conflicting globular expressions. "
                            f"File '{file}' cannot be tracked above once."
                        )
                    if file not in self._file_threads and file not in _excludes:
                        self._notifier(file)
                        self._monitored_files.append(file)
                        self._append_thread(file, **trackable)
                        self._file_threads[file].start()
                        _registered_files.append(file)

    def _raise_exceptions(self) -> None:
        """Raise an exception summarising exception throws in all threads.

        Assembles the exception information for all threads within the
        file thread launcher and then raises a session exception. This means
        the failure of monitoring of a file will not interrupt monitoring of
        other files.

        Raises
        ------
        mp_exc.SessionFailure
            an exception summarising all thread failures
        """
        if not any(
            (
                _exceptions := {
                    name: thread.exception
                    for name, thread in self._file_threads.items()
                    if thread.exception
                }
            ).values()
        ):
            return
        raise mp_exc.SessionFailure(_exceptions)

    def abort(self) -> None:
        self._raise_exceptions()


class LogFileThreadLauncher(FileThreadLauncher):
    """Create a file thread launcher for tailing files.

    This class focuses on the monitoring of log files whereby only
    the latest line or group of lines requires monitoring.
    """

    def __init__(
        self,
        trackables: typing.List[LogFileRegexPair],
        file_thread_callback: typing.Callable,
        file_thread_termination_trigger: threading.Event,
        refresh_interval: float,
        exclude_files_globex: typing.List[str] | None,
        exception_callback: typing.Callable | None = None,
        notification_callback: typing.Callable | None = None,
        file_list: typing.List[str] | None = None,
        file_thread_lock: typing.Any | None = None,
    ) -> None:
        """Initialise a log file monitor thread launcher.

        Parameters
        ----------
        trackables : typing.List[LogFileRegexPair]
            list of tuples containing:
                - globular expressions of files to monitor
                - regex defining the variables to track
                - whether the file is static (written once) or changing.
        file_thread_callback : typing.Callable
            the function to call when a file is modified
        file_thread_termination_trigger : threading.Event
            threading event which when set will trigger termination of all file
            monitor loops
        refresh_interval : float
            how often to check for new files
        exclude_files_globex : typing.List[str] | None
            a list of globular expressions for files to exclude
        exception_callback : typing.Callable | None, optional
            function to call when an exception is thrown
        notification_callback : typing.Callable | None, optional
            function called to notify when a new file is detected.
            Default is a print statement.
        file_list : typing.List[str] | None, optional
            container to append any found file names, by default None
        file_thread_lock : threading.Lock, optional
            shared mutex to prevent the callback being called simultaneously by
            multiple threads.
        """

        super().__init__(
            file_thread_callback=file_thread_callback,
            parsing_callback=mp_parse.record_log,
            refresh_interval=refresh_interval,
            file_thread_lock=file_thread_lock,
            trackables=trackables,
            notification_callback=notification_callback
            or (lambda item: loguru.logger.info(f"Found NEW log '{item}'")),
            exclude_files_globex=exclude_files_globex,
            file_list=file_list,
            file_thread_termination_trigger=file_thread_termination_trigger,
            exception_callback=exception_callback,
        )


class FullFileThreadLauncher(FileThreadLauncher):
    """Create a file thread launcher for full files.

    This class focuses on the monitoring of full files whereby the
    whole file content is read each time the file is modified.
    """

    def __init__(
        self,
        trackables: typing.List[FullFileTrackedValue],
        file_thread_callback: typing.Callable,
        file_thread_lock: threading.Lock,
        file_thread_termination_trigger: threading.Event,
        refresh_interval: float,
        exclude_files_globex: typing.List[str] | None,
        exception_callback: typing.Callable | None = None,
        notification_callback: typing.Callable | None = None,
        file_list: typing.List[str] | None = None,
    ) -> None:
        """Initialise a full file monitor thread launcher.

        Parameters
        ----------
        trackables : typing.List[FullFileTrackedValue]
            Dictionary containing:
                - globular expressions of files to monitor
                - regex defining the variables to track
                - custom parser
                - whether the file is static (written once) or changing.
        file_thread_callback : typing.Callable
            the function to call when a file is modified
        file_thread_termination_trigger : threading.Event
            threading event which when set will trigger termination of all file
            monitor loops
        refresh_interval : float
            how often to check for new files
        exclude_files_globex : typing.List[str] | None
            a list of globular expressions for files to exclude
        exception_callback : typing.Callable | None, optional
            function to call when an exception is thrown
        notification_callback : typing.Callable | None, optional
            function called to notify when a new file is detected.
            Default is a print statement.
        file_list : typing.List[str] | None, optional
            container to append any found file names, by default None
        file_thread_lock : threading.Lock, optional
            shared mutex to prevent the callback being called simultaneously by
            multiple threads.
        """

        super().__init__(
            file_thread_callback=file_thread_callback,
            parsing_callback=mp_parse.record_file,
            refresh_interval=refresh_interval,
            trackables=trackables,
            notification_callback=notification_callback
            or (lambda item: loguru.logger.info(f"Found NEW file '{item}'")),
            exclude_files_globex=exclude_files_globex,
            file_list=file_list,
            file_thread_lock=file_thread_lock,
            file_thread_termination_trigger=file_thread_termination_trigger,
            exception_callback=exception_callback,
        )
