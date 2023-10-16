import typing

LogFileRegexPair = typing.Tuple[
    str, typing.List[typing.Tuple[str | None, typing.Pattern]]
]
FullFileTrackedValue = typing.Tuple[str, typing.List[str]]
