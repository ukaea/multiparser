import typing
import concio.monitor
import logging


def monitor_files(
    file_filters: typing.List[str],
    result_handler: typing.Callable | None
) -> None:
    """Monitor changes within a directory

    Parameters
    ----------
    file_filters : List[str]
        list of globular expressions for filtering
        files for processing
    result_handler : Callable | None
        optional callback on handling of results. By default the changes will
        be printed to a log file
    """
    if not result_handler:
        def log_callback() -> None:
            pass