# `FileMonitor.track`

The `FileMonitor` method `track` is used to monitor files as a whole, that is loading the full file whenever it is modified and parsing it in its entirety, for example JSON or YAML files.

```python
monitor.track(
    path_glob_exprs="*.log",
    tracked_values=[re.compile(r'^(\w+)=(\d+))', re.compile(r'\w+=([\d\.]+)')],
    labels=[None, "fraction"],
    callback=lambda data, _: logging.getLogger("Simulation").info(data),
    parser_func=None,
    parser_kwargs=None
)
```

## Arguments

### `path_glob_exprs`
`#!python list[str] | str`

Either a single globular expression string or a list of such strings defining files to be monitored by this tail, can be an explicit string if only one file is required.

### `tracked_values`
`#!python list[str | Pattern] | str | Pattern | None`

Default: `None`

Either a single string or regular expression, or a list of a combination of either defining keys for values to be recorded. Regular expressions must be `re.compile` objects. In the case of regular expressions `findall` is used to filter required information. If no tracked values are specified all content is read.

```python
tracked_values = [
    "name",
    re.compile(r'var_(\w+)'),
]
```

### `callback`
`#!python Callable[[dict[str, Any], dict[str, Any]], None] | None`

Default: `None`

Optional callback exclusively for this file set, this overwrites the global callback (if specified) within the `FileMonitor` definition (see [here](./file_monitor.md#per-thread-callback)).


### `parser_func`
`#!python Callable[[str, *_], tuple[dict[str, Any], dict[str, Any]]] | None`

Default: `None`

Specific a custom parser for this file set, this must be a file parsing function wrapped in the `multiparser.parsing.file.file_parser` decorator, for more information see [here](./custom_parsers.md#file-parsers).

### `parser_kwargs`
`#!python dict[str, Any] | None`

Default: `None`

Keyword arguments to pass to the function defined above, useful if you want to specify near-identical parsing of multiple file sets with slight customisation.


### `static`
`#!python bool`

Default: `False`

If `True` the file is read only once, recommended if a recorded file is never overwritten later during process execution as it will terminate the thread monitoring that file.


### `file_type`
`#!python Literal['csv', 'pkl', 'json', 'toml', 'yaml', 'nml', 'pqt', 'ft'] | None`

Default: `None`

If using "lazy" parsing whereby the file is read based on extension, override the default. This is mandatory whereby the input file suffix does not match any of the above. For more information on built-in parsers see [here](./builtin_parsers.md).
