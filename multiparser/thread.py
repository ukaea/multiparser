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
from multiparser.typing import (
    FullFileTrackable,
    LogFileTrackable,
    Trackable,
    TrackableList,
)


def handle_monitor_thread_exception(function: typing.Callable) -> typing.Callable:
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
        """Decorator to trigger termination event if exception thrown"""
        try:
            return function(self, *args, **kwargs)
        except Exception as e:
            if self._exception_callback:
                self._exception_callback(e)
            self._termination_trigger.set()

    return _wrapper


class FileThreadLauncher:
    """Base class for all file monitor thread launchers.

    Spins up threads whenever a new file matching a given set of globular
    expressions is created in order to monitor any changes to that file.
    """

    def __init__(
        self,
        file_thread_termination_trigger: threading.Event,
        parsing_callback: typing.Callable,
        notification_callback: typing.Callable,
        refresh_interval: float,
        trackables: TrackableList,
        file_limit: int | None,
        exclude_files_globex: typing.List[str] | None,
        exception_callback: typing.Callable | None = None,
        file_thread_lock: typing.Any | None = None,
        file_list: typing.List[str] | None = None,
        flatten_data: bool = False,
        abort_on_fail: bool = False,
        test_exception_capture: bool = False,
    ) -> None:
        """Create a new instance of the file monitor thread launcher.

        Parameters
        ----------
        file_thread_termination_trigger : threading.Event
            threading event which when set will trigger termination of all file
            monitor loops
        parsing_callback : typing.Callable
            function to be called to parse the monitored files
        notification_callback : typing.Callable
            function called to notify when a new file is detected
        refresh_interval : float
            how often to check for new files
        trackables : typing.List[LogFileTrackable  |  FullFileTrackable]
            a tuple containing:
                - globular expression for file capture
                - regular_expressions for variable tracking within files
                - whether the file is static (written once) or changing
        file_limit : int, optional
            place a limit on the number of files that can be monitored
        exclude_files_globex : typing.List[str] | None
            a list of globular expressions for files to exclude
        exception_callback : typing.Callable | None, optional
            function to call when an exception is thrown
        file_list : typing.List[str] | None, optional
            container to append any found file names, by default None
        file_thread_lock : threading.Lock, optional
            shared mutex to prevent the callback being called simultaneously by
            multiple threads.
        flatten_data : bool, optional
            whether to convert data to a single level dictionary of key-value pairs
        abort_on_fail : bool, optional
            whether to terminate all file threads if one fails
        test_exception_capture : bool, optional
            set to exception capture testing mode, this is for testing only
            throwing a dummy exception of a known form
        """
        self._trackables: TrackableList = trackables
        self._exception_callback: typing.Callable | None = exception_callback
        self._terminate_on_file_thread_fail: bool = abort_on_fail
        self._lock: typing.Any | None = file_thread_lock
        self._termination_trigger: threading.Event = file_thread_termination_trigger
        self._parsing_callback: typing.Callable = parsing_callback
        self._notifier: typing.Callable = notification_callback
        self._file_threads: typing.Dict[str, threading.Thread] = {}
        self._exclude_globex: typing.List[str] | None = exclude_files_globex
        self._records: typing.List[typing.Tuple[str, str]] = []
        self._interval = refresh_interval
        self._monitored_files = file_list if file_list is not None else []
        self._flatten_data = flatten_data
        self._exceptions: typing.Dict[str, Exception | None] = {}
        self._file_limit: int | None = file_limit
        self._exception_test: bool = test_exception_capture

    @property
    def exceptions(self) -> typing.Dict[str, Exception | None]:
        return self._exceptions

    @property
    def n_running(self) -> int:
        return sum(thread.is_alive() for thread in self._file_threads.values())

    @handle_monitor_thread_exception
    def _append_thread(
        self,
        file_name: str,
        flatten_data: bool,
        tracked_values: Trackable,
        callback: typing.Callable,
        static: bool = False,
        parser_func: typing.Callable | None = None,
        parser_kwargs: typing.Dict | None = None,
        convert: bool = True,
        ignore_lines: typing.List[typing.Pattern | str] | None = None,
        file_type: str | None = None,
        **_,
    ) -> None:
        """Create a new thread for a monitored file

        Parameters
        ----------
        file_name : str
            name of the file to observe
        tracked_values : Trackable
            values to monitor within the given file
        static : bool, optional
            whether the file is written only once, by default False
        parser_func : typing.Callable | None, optional
            use a user defined parser instead
        convert : bool, optional
            convert values from string to numeric where appropriate
        """

        def _thread_exception_callback(
            exception: Exception,
            target_file: str = file_name,
            exceptions: typing.Dict[str, Exception | None] = self._exceptions,
        ) -> None:
            exceptions[target_file] = exception

        def _read_action(
            records: typing.List[typing.Tuple[str, str]],
            monitor_callback: typing.Callable = callback,
            exception_callback=_thread_exception_callback,
            file_name: str = file_name,
            termination_trigger: threading.Event = self._termination_trigger,
            interval: float = self._interval,
            tracked_vals: Trackable = tracked_values,
            lock: typing.Any | None = self._lock,
            static_read: bool = static,
            cstm_parser: typing.Callable | None = parser_func,
            convert: bool = convert,
            ignore_lines: typing.List[str | typing.Pattern] | None = ignore_lines,
            kwargs: typing.Dict = parser_kwargs or {},
            flatten_data: bool = flatten_data,
        ) -> None:
            """Thread target function for parsing of detected file"""

            _cached_metadata: typing.Dict[str, str | int] = {}

            try:
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
                        tracked_values=tracked_vals,
                        parser_func=cstm_parser,
                        convert=convert,
                        ignore_lines=ignore_lines,
                        file_type=file_type,
                        **(_cached_metadata | {k: v for k, v in kwargs.items() if v}),
                    )

                    # Some parsers return multiple results, e.g. those parsing multiple file lines
                    _parsed_list = (
                        [_parsed] if not isinstance(_parsed, list) else _parsed
                    )
                    _flattened_list = []

                    # If the parser method records a list of dictionaries as data
                    # we need to ensure these are handled in the same way as for parsers
                    # which return only a single data dictionary
                    for _meta, _entry in _parsed_list:
                        if isinstance(_entry, (list, tuple, set)) and _entry:
                            for section in _entry:
                                _flattened_list.append((_meta, section))
                        else:
                            _flattened_list.append((_meta, _entry))

                    for _meta, _data in _flattened_list:
                        # Keep latest
                        _cached_metadata = _meta

                        if not _data:
                            continue

                        if flatten_data:
                            _data = mp_parse.flatten_data(_data)

                        loguru.logger.debug(
                            f"{file_name}: {_modified_time}: Recorded: {_data}"
                        )

                        if lock:
                            with lock:
                                monitor_callback(_data, _meta)
                        else:
                            monitor_callback(_data, _meta)

                    records.append((_modified_time, file_name))

                    # If only a single read is required terminate loop
                    if static_read:
                        break
            except Exception as e:
                loguru.logger.error(
                    f"{type(e).__name__} exception raised on thread during parsing of file '{file_name}': {e}"
                )
                exception_callback(exception=e)

        self._file_threads[file_name] = threading.Thread(
            target=_read_action,
            args=(self._records,),
        )

    @handle_monitor_thread_exception
    def run(self) -> None:
        """Start the thread launcher"""
        if self._exception_test:
            raise AssertionError("TESTING_MODE: Test AssertionError")

        while not self._termination_trigger.is_set():
            if self.exceptions and self._terminate_on_file_thread_fail:
                break

            time.sleep(self._interval)
            _excludes: typing.List[str] = []
            for expr in self._exclude_globex or []:
                _excludes += glob.glob(expr)
            for trackable in self._trackables:
                # Check for multiple tracking entries for the same file
                # not allowed due to constraint of one thread spawned per file
                _registered_files: typing.List[str] = []
                if not isinstance((_glob_str := trackable["glob_expr"]), str):
                    raise AssertionError(
                        f"Expected type AnyStr for globular expression but got '{_glob_str}'"
                    )
                for file in glob.glob(_glob_str):
                    if file in _registered_files:
                        raise AssertionError(
                            "Conflicting globular expressions. "
                            f"File '{file}' cannot be tracked above once."
                        )
                    if file not in self._file_threads and file not in _excludes:
                        if self._file_limit and self.n_running >= self._file_limit:
                            loguru.logger.warning(
                                f"Reached file limit, cannot parse '{file}'"
                            )
                            continue

                        self._notifier(file)
                        self._monitored_files.append(file)
                        self._exceptions[file] = None
                        self._append_thread(file, self._flatten_data, **trackable)
                        self._file_threads[file].start()
                        _registered_files.append(file)
        self._raise_exceptions()

    def abort_threads(self) -> None:
        for thread in self._file_threads.values():
            thread.join()

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
        if self._terminate_on_file_thread_fail:
            self.abort_threads()

        if not any(self._exceptions.values()):
            return

        if self._exception_callback:
            self._exception_callback(
                mp_exc.FileMonitorThreadException(self._exceptions)
            )


class LogFileThreadLauncher(FileThreadLauncher):
    """Create a file thread launcher for tailing files.

    This class focuses on the monitoring of log files whereby only
    the latest line or group of lines requires monitoring.
    """

    def __init__(
        self,
        trackables: typing.List[LogFileTrackable],
        file_thread_termination_trigger: threading.Event,
        refresh_interval: float,
        file_limit: int | None,
        exclude_files_globex: typing.List[str] | None,
        exception_callback: typing.Callable | None = None,
        notification_callback: typing.Callable | None = None,
        file_list: typing.List[str] | None = None,
        file_thread_lock: typing.Any | None = None,
        flatten_data: bool = False,
        abort_on_fail: bool = False,
        test_exception_capture: bool = False,
    ) -> None:
        """Initialise a log file monitor thread launcher.

        Parameters
        ----------
        trackables : typing.List[LogFileTrackable]
            list of tuples containing:
                - globular expressions of files to monitor
                - regex defining the variables to track
                - whether the file is static (written once) or changing.
        file_thread_termination_trigger : threading.Event
            threading event which when set will trigger termination of all file
            monitor loops
        refresh_interval : float
            how often to check for new files
        file_limit : int, optional
            place a limit on the number of files that can be monitored
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
        flatten_data : bool, optional
            whether to convert data to a single level dictionary of key-value pairs
        abort_on_fail : bool, optional
            whether to terminate all file threads if one fails
        test_exception_capture : bool, optional
            set to exception capture testing mode, this is for testing only
            throwing a dummy exception of a known form
        """

        super().__init__(
            parsing_callback=mp_parse.record_log,
            refresh_interval=refresh_interval,
            file_thread_lock=file_thread_lock,
            trackables=trackables,
            file_limit=file_limit,
            notification_callback=notification_callback
            or (lambda item: loguru.logger.info(f"Found NEW log '{item}'")),
            exclude_files_globex=exclude_files_globex,
            file_list=file_list,
            file_thread_termination_trigger=file_thread_termination_trigger,
            exception_callback=exception_callback,
            flatten_data=flatten_data,
            abort_on_fail=abort_on_fail,
            test_exception_capture=test_exception_capture,
        )


class FullFileThreadLauncher(FileThreadLauncher):
    """Create a file thread launcher for full files.

    This class focuses on the monitoring of full files whereby the
    whole file content is read each time the file is modified.
    """

    def __init__(
        self,
        trackables: typing.List[FullFileTrackable],
        file_thread_termination_trigger: threading.Event,
        refresh_interval: float,
        file_limit: int | None,
        exclude_files_globex: typing.List[str] | None,
        exception_callback: typing.Callable | None = None,
        notification_callback: typing.Callable | None = None,
        file_list: typing.List[str] | None = None,
        file_thread_lock: "threading.Lock | None" = None,
        flatten_data: bool = False,
        abort_on_fail: bool = False,
        test_exception_capture: bool = False,
    ) -> None:
        """Initialise a full file monitor thread launcher.

        Parameters
        ----------
        trackables : typing.List[FullFileTrackable]
            Dictionary containing:
                - globular expressions of files to monitor
                - regex defining the variables to track
                - custom parser
                - whether the file is static (written once) or changing.
        file_thread_termination_trigger : threading.Event
            threading event which when set will trigger termination of all file
            monitor loops
        refresh_interval : float
            how often to check for new files
        file_limit : int, optional
            place a limit on the number of files that can be monitored
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
        flatten_data : bool, optional
            whether to convert data to a single level dictionary of key-value pairs
        abort_on_fail : bool, optional
            whether to terminate all file threads if one fails
        test_exception_capture : bool, optional
            set to exception capture testing mode, this is for testing only
            throwing a dummy exception of a known form
        """

        super().__init__(
            parsing_callback=mp_parse.record_file,
            refresh_interval=refresh_interval,
            trackables=trackables,
            file_limit=file_limit,
            notification_callback=notification_callback
            or (lambda item: loguru.logger.info(f"Found NEW file '{item}'")),
            exclude_files_globex=exclude_files_globex,
            file_list=file_list,
            file_thread_lock=file_thread_lock,
            file_thread_termination_trigger=file_thread_termination_trigger,
            exception_callback=exception_callback,
            flatten_data=flatten_data,
            abort_on_fail=abort_on_fail,
            test_exception_capture=test_exception_capture,
        )
