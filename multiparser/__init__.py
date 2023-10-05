import sys

import loguru

from .monitor import *

loguru.logger.remove()

loguru.logger.add(
    sys.stderr,
    format="{level.icon} | <green>{elapsed}</green> | <level>{level: <8}</level> | <c>multiparse</c> | {message}",
    colorize=True,
)
