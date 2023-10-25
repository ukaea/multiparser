import typing

LogFileRegexPair = typing.Dict[
    str,
    typing.List[typing.Tuple[str | None, typing.Pattern]]
    | bool
    | typing.Callable
    | None,
]

LogFileCustomPair = typing.Dict[str, typing.Callable]

FullFileTrackedValue = typing.Dict[
    str, typing.List[str] | bool | typing.Callable | None
]
