import loguru

from multiparser.monitor import FileMonitor as FileMonitor

__all__ = ["FileMonitor"]

loguru.logger.remove()
