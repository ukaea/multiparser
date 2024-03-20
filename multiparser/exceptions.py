import traceback


class FileMonitorThreadException(Exception):
    def __init__(self, file_thread_exceptions: dict[str, Exception | None]) -> None:
        self.exceptions = file_thread_exceptions


class SessionFailure(Exception):
    def __init__(self, exceptions_dict: dict[str, BaseException]) -> None:
        _info_str: str = ""
        for name, exception in exceptions_dict.items():
            _info_str += f"{name}:\n\t\t"
            _info_str += "\n\t\t".join(traceback.format_exception(exception))
        super().__init__(
            "Multiparser session encountered the following exceptions:\n\t"
            f"{_info_str}"
        )
