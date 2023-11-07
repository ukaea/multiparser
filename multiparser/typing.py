"""
Multiparser Typing
==================

Defines typing aliases for multiparser argument types

"""
__date__ = "2023-10-16"
__author__ = "Kristian Zarebski"
__maintainer__ = "Kristian Zarebski"
__email__ = "kristian.zarebski@ukaea.uk"
__copyright__ = "Copyright 2023, United Kingdom Atomic Energy Authority"

import re
import typing

TrackedValues = typing.List[typing.Pattern | str] | typing.Pattern | str

LogFileTrackable = typing.Dict[
    str,
    typing.AnyStr
    | typing.List[typing.Tuple[str | None, typing.Pattern]]
    | bool
    | typing.Callable
    | typing.Dict[str, typing.Any]
    | None,
]

FullFileTrackable = typing.Dict[
    str,
    typing.AnyStr
    | bool
    | typing.Tuple[str | None, re.Pattern | str]
    | typing.Callable
    | None
    | typing.Dict[str, typing.Any],
]

Trackable = LogFileTrackable | FullFileTrackable

TrackableList = typing.List[LogFileTrackable] | typing.List[FullFileTrackable]

TimeStampedData = typing.Tuple[
    typing.Dict[str, str | int | typing.List[str]],
    typing.Dict[str, typing.Any] | typing.List[typing.Dict[str, typing.Any]],
]
