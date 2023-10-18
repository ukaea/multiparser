import typing

LogFileRegexPair = typing.Dict[
    str,
    typing.List[typing.Tuple[str | None, typing.Pattern]]
    | bool
    | typing.Callable
    | None,
]
FullFileTrackedValue = typing.Dict[
    str, typing.List[str] | bool | typing.Callable | None
]
