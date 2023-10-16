import traceback
import typing


class ThreadException(Exception):
    def __init__(self, thread_id: str, thread_exception: BaseException) -> None:
        super().__init__(f"[{thread_id}] {thread_exception}")


class SessionFailure(Exception):
    def __init__(self, exceptions_dict: typing.Dict[str, BaseException]) -> None:
        _info_str: str = ""
        for name, exception in exceptions_dict.items():
            _info_str += f"{name}:\n\t\t"
            _info_str += "\n\t\t".join(traceback.format_exception(exception))
        super().__init__(
            "Multiparser session encountered the following exceptions:\n\t"
            f"{_info_str}"
        )
