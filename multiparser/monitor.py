"""
Multiparser File Monitor
========================

Contains a class for defining how to track the changes within output files
created by a process. These files can be defined by name or using globular
expressions, with content of significance then being defined using regular
expressions. The files are split into log files where the last line is read
on modification, and full files where the whole file is read and parsed as
a whole.

"""
__date__ = "2023-10-16"
__author__ = "Kristian Zarebski"
__maintainer__ = "Kristian Zarebski"
__email__ = "kristian.zarebski@ukaea.uk"
__copyright__ = "Copyright 2023, United Kingdom Atomic Energy Authority"

import glob
import logging
import re
import string
import sys
import threading
import typing

import loguru

import multiparser.thread as mp_thread
from multiparser.typing import FullFileTrackedValue, LogFileRegexPair

__all__ = ["FileMonitor"]


class FileMonitor:
    def __init__(
        self,
        per_thread_callback: typing.Callable,
        exception_callback: typing.Callable | None = None,
        notification_callback: typing.Callable | None = None,
        termination_trigger: threading.Event | None = None,
        lock_callbacks: bool = True,
        interval: float = 1e-3,
        log_level: int | str = logging.INFO,
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
        exception_callback : typing.Callable | None, optional
            function to be executed when an exception is thrown
        notification_callback : typing.Callable | None, optional
            function to be called when a new file is found, default is
            a print statement
        lock_callback : bool, optional
            whether to only allow one thread to execute the callbacks
            at a time. Default is True.
        interval : float, optional
            the refresh rate of the file monitors, by default 10.0 seconds
        log_level : int | str, optional
            log level for this object
        """
        self._interval: float = interval
        self._per_thread_callback = per_thread_callback
        self._notification_callback = notification_callback
        self._exception_callback = exception_callback
        self._file_threads_mutex: typing.Any | None = (
            threading.Lock() if lock_callbacks else None
        )
        self._manual_abort: bool = termination_trigger is not None
        self._complete = termination_trigger or threading.Event()
        self._abort_file_monitors = termination_trigger or threading.Event()
        self._known_files: typing.List[str] = []
        self._file_globex: typing.List[FullFileTrackedValue] = []
        self._log_globex: typing.List[LogFileRegexPair] = []
        self._excluded_patterns: typing.List[str] = []
        self._file_monitor_thread: mp_thread.HandledThread | None = None
        self._log_monitor_thread: mp_thread.HandledThread | None = None

        loguru.logger.add(
            sys.stderr,
            format="{level.icon} | <green>{elapsed}</green> "
            "| <level>{level: <8}</level> | <c>multiparse</c> | {message}",
            colorize=True,
            level=log_level,
        )

    def _create_monitor_threads(self) -> None:
        """Create threads for the log file and full file monitors"""

        def _full_file_monitor_func(
            glob_exprs: typing.List[FullFileTrackedValue],
            exc_glob_exprs: typing.List[str],
            file_list: typing.List[str],
            termination_trigger: threading.Event,
            interval: float,
        ) -> None:
            _full_file_threads = mp_thread.FullFileThreadLauncher(
                trackables=glob_exprs,
                exclude_files_globex=exc_glob_exprs,
                refresh_interval=interval,
                file_list=file_list,
                file_thread_callback=self._per_thread_callback,
                file_thread_lock=self._file_threads_mutex,
                file_thread_termination_trigger=termination_trigger,
                exception_callback=self._exception_callback,
                notification_callback=self._notification_callback,
            )
            _full_file_threads.run()
            _full_file_threads.abort()

        def _log_file_monitor_func(
            glob_exprs: typing.List[LogFileRegexPair],
            exc_glob_exprs: typing.List[str],
            file_list: typing.List[str],
            termination_trigger: threading.Event,
            interval: float,
        ) -> None:
            _log_file_threads = mp_thread.LogFileThreadLauncher(
                trackables=glob_exprs,
                exclude_files_globex=exc_glob_exprs,
                refresh_interval=interval,
                file_list=file_list,
                file_thread_callback=self._per_thread_callback,
                file_thread_lock=self._file_threads_mutex,
                file_thread_termination_trigger=termination_trigger,
                exception_callback=self._exception_callback,
                notification_callback=self._notification_callback,
            )
            _log_file_threads.run()
            _log_file_threads.abort()

        self._file_monitor_thread = mp_thread.HandledThread(
            task_identifier="Full File Monitor",
            target=_full_file_monitor_func,
            throw_callback=self._exception_callback,
            args=(
                self._file_globex,
                self._excluded_patterns,
                self._known_files,
                self._abort_file_monitors,
                self._interval,
            ),
        )

        self._log_monitor_thread = mp_thread.HandledThread(
            task_identifier="Log File Monitor",
            target=_log_file_monitor_func,
            throw_callback=self._exception_callback,
            args=(
                self._log_globex,
                self._excluded_patterns,
                self._known_files,
                self._abort_file_monitors,
                self._interval,
            ),
        )

    def _check_custom_log_parser(self, parser: typing.Callable) -> None:
        """Verifies the parser works correctly before launching threads"""
        _test_str = string.ascii_lowercase
        _test_str += string.ascii_uppercase
        _test_str += string.ascii_letters
        _test_str *= 100
        try:
            _out = parser(_test_str, __input_file=__file__, __read_bytes=None)
        except Exception as e:
            raise AssertionError(f"Custom parser testing failed with exception:\n{e}")
        if len(_out) != 2:
            raise AssertionError(
                "Parser function must return two objects, a metadata dictionary and parsed values"
            )
        if "_wrapper" not in parser.__name__:
            raise AssertionError(
                "Parser function must be decorated using the multiparser.parser decorator"
            )

    def exclude(self, path_glob_exprs: typing.List[str] | str) -> None:
        """Exclude a set of files from monitoring.

        Parameters
        ----------
        path_glob_exprs : typing.List[str] | str
            a list or string defining globular expressions for files
            to exclude from tracking
        """
        if isinstance(path_glob_exprs, str):
            self._excluded_patterns.append(path_glob_exprs)
        else:
            self._excluded_patterns += path_glob_exprs

        # Check globular expressions before passing them to thread
        for expression in self._excluded_patterns:
            glob.glob(expression)

    def track(
        self,
        path_glob_exprs: typing.List[str] | str,
        tracked_values: typing.List[str] | None = None,
        custom_parser: typing.Callable | None = None,
        parser_kwargs: typing.Dict | None = None,
        static: bool = False,
    ) -> None:
        """Track a set of files.

        Tracking a file means reading the whole contents at a time,
        this should be reserved for file types whereby reading on a
        per line basis is unconventional (e.g. JSON etc), for such
        file types the file must be loaded in as a whole.

        Parameters
        ----------
        path_glob_exprs : typing.List[str] | str
            set of or single globular expression(s) defining files
            to monitor
        tracked_values : typing.List[str] | None, optional
            a list of regular expressions defining variables to track
            within the file, by default None
        custom_parser : typing.Callable | None, optional
            provide a custom parsing function
        parser_kwargs : typing.Dict | None, optional
            arguments to include when running the specified custom parser
        static : bool, optional
            (if known) whether the given file(s) are written only once
            and so looped monitoring is not required, by default False
        """
        if isinstance(path_glob_exprs, str):
            _parsing_dict: typing.Dict[str, typing.Any] = {
                "glob_exprs": path_glob_exprs,
                "tracked_values": tracked_values,
                "static": static,
                "custom_parser": custom_parser,
                "parser_kwargs": parser_kwargs,
            }
            self._file_globex.append(_parsing_dict)
        else:
            self._file_globex += [
                {
                    "glob_exprs": g,
                    "tracked_values": tracked_values,
                    "static": static,
                    "custom_parser": custom_parser,
                    "parser_kwargs": parser_kwargs,
                }
                for g in path_glob_exprs
            ]

        # Check globular expressions before passing them to thread
        for entry in self._file_globex:
            glob.glob(entry["glob_exprs"])

    def tail(
        self,
        path_glob_exprs: typing.List[str] | str,
        tracked_values: typing.List[typing.Pattern] | None = None,
        labels: str | typing.List[str | None] | None = None,
        custom_parser: typing.Callable | None = None,
        parser_kwargs: typing.Dict | None = None,
    ) -> None:
        """Tail a set of files.

        Tailing a file means reading the last line of that file,
        the file(s) in question should be read line by line, e.g.
        for a log file.

        Capture groups for the regular expressions defining the values
        to monitor can have two forms:

        * A single regular expression group, e.g. r'\d+' or r'(\d+)'
          with a name present in 'labels', e.g. "my_var".

          tail(path_glob_exprs=[r'(\d+),', r'\d\.\d+'], labels=['my_var', 'other'])

        * A double regular expression group, e.g. r'(\w+\_var)=(\d+)'
          where the first group is the label, and the second the value.

          tail(path_glob_exprs=[r'(\w+\_var)=(\d+)'])
          tail(path_glob_exprs=[r'(\w+\_var)=(\d+)',r'(\w+\_i=(\d+)'])

          This can be overwritten by providing a value for that group.

          tail(path_glob_exprs=[r'(\w+\_var)=(\d+)'])
          tail(path_glob_exprs=[r'(\w+\_var)=(\d+)',r'(\w+\_i=(\d+)'], labels=['my_var', None])

        Parameters
        ----------
        path_glob_exprs : typing.List[str] | str
            set of or single globular expression(s) defining files
            to monitor
        tracked_values : typing.List[Pattern | str], optional
            a set of regular expressions or strings defining variables to track.
            Where one capture group is defined the user must provide
            an associative label. Where two are defined, the first capture
            group is taken to be the label, the second the value.
        labels : typing.List[str], optional
            define the label to assign to each value, if an element in the
            list is None, then a capture group is used. If labels itself is
            None, it is assumed all matches have a label capture group.
        custom_parser : typing.Callable | None, optional
            provide a custom parsing function
        parser_kwargs : typing.Dict | None, optional
            arguments to include when running the specified custom parser
        """
        if custom_parser:
            self._check_custom_log_parser(custom_parser)

        if custom_parser and tracked_values:
            raise AssertionError(
                "Cannot specify both tracked values and custom parser for monitor "
                "method 'track'"
            )

        if custom_parser and labels:
            raise AssertionError(
                "Cannot specify both labels and custom parser for monitor "
                "method 'track'"
            )

        if tracked_values and isinstance(labels, (str, re.Pattern)):
            tracked_values = [tracked_values]

        if labels and isinstance(labels, (str, re.Pattern)):
            labels = [labels]

        if labels and tracked_values and len(labels) != len(tracked_values):
            raise AssertionError(
                "Number of labels must match number of regular expressions in 'tail'."
            )
        if not tracked_values or custom_parser:
            _reg_lab_expr_pairing = None
        else:
            labels = labels or [None] * len(tracked_values)
            _reg_lab_expr_pairing: typing.List[
                typing.Tuple[str | None, typing.Pattern | str]
            ] | None = [
                (label, reg_ex) for label, reg_ex in zip(labels, tracked_values)
            ]

        if isinstance(path_glob_exprs, (str, re.Pattern)):
            _parsing_dict: typing.Dict[str, typing.Any] = {
                "glob_exprs": path_glob_exprs,
                "tracked_values": _reg_lab_expr_pairing,
                "static": False,
                "custom_parser": custom_parser,
                "parser_kwargs": parser_kwargs,
            }
            self._log_globex.append(_parsing_dict)
        else:
            self._log_globex += [
                {
                    "glob_exprs": g,
                    "tracked_values": _reg_lab_expr_pairing,
                    "static": False,
                    "custom_parser": custom_parser,
                    "parser_kwargs": parser_kwargs,
                }
                for g in path_glob_exprs
            ]

        # Check globular expressions before passing them to thread
        for expression in self._log_globex:
            glob.glob(expression["glob_exprs"])

    def terminate(self, __manual_abort: bool = True) -> None:
        """Terminate all monitors."""
        if __manual_abort:
            self._abort_file_monitors.set()
            self._complete.set()
        self._file_monitor_thread.join()
        self._log_monitor_thread.join()

        if not self._known_files:
            loguru.logger.warning("No files were processed during this session.")

        if (
            _exception := self._file_monitor_thread.exception
            or self._log_monitor_thread.exception
        ):
            raise _exception

    def run(self) -> None:
        """Launch all monitors"""
        self._file_monitor_thread.start()
        self._log_monitor_thread.start()
        if self._manual_abort:
            self.terminate(False)

    def __enter__(self) -> "FileMonitor":
        self._create_monitor_threads()
        return self

    def __exit__(self, *_, **__) -> None:
        self._complete.set()
