import glob
import logging
import re
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
        var_list : List[str], optional
            list of variables to track from the given files
        interval : float, optional
            the refresh rate of the file monitors, by default 10.0 seconds
        """
        self._interval: float = interval
        self._per_thread_callback = per_thread_callback
        self._file_threads_mutex = threading.Lock()
        self._complete = threading.Event()
        self._abort_file_monitors = threading.Event()
        self._known_files: typing.List[str] = []
        self._file_globex: typing.List[FullFileTrackedValue] = []
        self._log_globex: typing.List[LogFileRegexPair] = []
        self._excluded_patterns: typing.List[str] = []
        self._file_monitor_thread: mp_thread.HandledThread | None = None
        self._log_monitor_thread: mp_thread.HandledThread | None = None

        loguru.logger.add(
            sys.stderr,
            format="{level.icon} | <green>{elapsed}</green> | <level>{level: <8}</level> | <c>multiparse</c> | {message}",
            colorize=True,
            level=log_level,
        )

    def _create_monitor_threads(self) -> None:
        def _full_file_monitor_func(
            glob_exprs: typing.List[FullFileTrackedValue],
            exc_glob_exprs: typing.List[str],
            termination_trigger: threading.Event = self._abort_file_monitors,
            interval: float = self._interval,
        ) -> None:
            _full_file_threads = mp_thread.FullFileThreadLauncher(
                trackables=glob_exprs,
                exclude_files_globex=exc_glob_exprs,
                refresh_interval=interval,
                file_list=self._known_files,
                file_thread_callback=self._per_thread_callback,
                file_thread_lock=self._file_threads_mutex,
                file_thread_termination_trigger=termination_trigger,
            )
            _full_file_threads.run()
            _full_file_threads.abort()

        def _log_file_monitor_func(
            glob_exprs: typing.List[FullFileTrackedValue],
            exc_glob_exprs: typing.List[str],
            termination_trigger: threading.Event = self._abort_file_monitors,
            interval: float = self._interval,
        ) -> None:
            _log_file_threads = mp_thread.LogFileThreadLauncher(
                trackables=glob_exprs,
                exclude_files_globex=exc_glob_exprs,
                refresh_interval=interval,
                file_list=self._known_files,
                file_thread_callback=self._per_thread_callback,
                file_thread_lock=self._file_threads_mutex,
                file_thread_termination_trigger=termination_trigger,
            )
            _log_file_threads.run()
            _log_file_threads.abort()

        self._file_monitor_thread = mp_thread.HandledThread(
            task_identifier="Full File Monitor",
            target=_full_file_monitor_func,
            args=(self._file_globex, self._excluded_patterns),
        )

        self._log_monitor_thread = mp_thread.HandledThread(
            task_identifier="Log File Monitor",
            target=_log_file_monitor_func,
            args=(self._log_globex, self._excluded_patterns),
        )

    def exclude(self, path_glob_exprs: typing.List[str] | str) -> None:
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
    ) -> None:
        if tracked_values:
            tracked_values = [re.compile(t, re.IGNORECASE) for t in tracked_values]
        if isinstance(path_glob_exprs, str):
            self._file_globex.append((path_glob_exprs, tracked_values))
        else:
            self._file_globex += [(g, tracked_values) for g in path_glob_exprs]

        # Check globular expressions before passing them to thread
        for expression in self._file_globex:
            glob.glob(expression[0])

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
            _reg_lab_expr_pairing: typing.List[
                typing.Tuple[str | None, typing.Pattern]
            ] | None = [
                (label, re.compile(reg_ex, re.IGNORECASE))
                for label, reg_ex in zip(labels, regular_exprs)
            ]
        else:
            _reg_lab_expr_pairing = None

        if isinstance(path_glob_exprs, str):
            self._log_globex.append((path_glob_exprs, _reg_lab_expr_pairing))
        else:
            self._log_globex += [(g, _reg_lab_expr_pairing) for g in path_glob_exprs]

        # Check globular expressions before passing them to thread
        for expression in self._log_globex:
            glob.glob(expression[0])

    def terminate(self) -> None:
        self._abort_file_monitors.set()
        self._complete.set()
        self._file_monitor_thread.join()
        self._log_monitor_thread.join()

        if (
            _exception := self._file_monitor_thread.exception
            or self._log_monitor_thread.exception
        ):
            raise _exception

    def run(self) -> None:
        self._file_monitor_thread.start()
        self._log_monitor_thread.start()

    def __enter__(self) -> "FileMonitor":
        self._create_monitor_threads()
        return self

    def __exit__(self, *args, **kwargs) -> None:
        self._complete.set()
