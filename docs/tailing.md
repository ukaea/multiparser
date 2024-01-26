# Tailing

The `FileMonitor` method `tail` is used to monitor _appended_ changes to files, that is content which has been added to the file incrementally, for example log files containing the `stdout` of a given process.

```python
monitor.tail(
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

Either a single globular expression string or a list of such strings defining files to be monitored by this tail, can be an explicit string if only one file is required.

### `tracked_values`

Either a single string or regular expression, or a list of a combination of either defining values to be recorded. In general a regular expression applies here as it provides a pattern for recording, a string may be used if waiting for the appearance of a statement explicitly. Regular expressions must be `re.compile` objects.

Regular expressions can have either one capture group representing the value to capture, or two representing the key and the value. Note when only one regex group or a string is specified a label must also be present (see [`labels`](#labels)).

```python
tracked_values = [
    "END OF LINE",  # Wait for exact string (requires label)
    re.compile(r'(\w+)=(\d+)'), # Specify label/key and value to capture
    re.compile(r'\w+=(\d+)') # Specify only value to capture (requires label)
]
```
