import typing

LogFileRegexPair = typing.Tuple[
    str, typing.List[typing.Tuple[str | None, typing.Pattern]], bool
]
FullFileTrackedValue = typing.Tuple[str, typing.List[str], bool]
