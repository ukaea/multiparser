# The File Monitor

The `FileMonitor` class is the basis of all Multiparser sessions, it allows the user to define what files are tracked, and what to do when a change is detected. The class must be used as a context manager with the Python `with` statement:

```python
with multiparser.FileMonitor(
    per_thread_callback,
    exception_callback,
    notification_callback,
    termination_trigger,
    timeout,
    lock_callbacks
) as file_monitor:
    ...
```

## Arguments

### `per_thread_callback`

A callback function executed when changes are detected on any monitored file, this can also be overwritten on a per file basis. The function must accept the arguments `data` and `metadata`, e.g.:

```python
def my_callback(data: dict[str, Any], metadata: dict[str, Any]) -> None:
    logging.getLogger("Simulation").info(f"Recorded {data}")
```

This argument is optional, although a recommended starting point.

### `exception_callback`

An optional callback which is handed an exception message which is an amalgamation of all exceptions for running file monitor threads, e.g.:

```python
def exception_callback(exception_msg: str) -> None:
    logging.getLogger("Simulation").error(exception_msg)
```

### `notification_callback`

An optional callback which is handed the name of a file upon initial detection, e.g.:

```python
def notification_callback(file_name: str) -> None:
    logging.getLogger("Simulation").info(f"Found new file: {file_name}")
```

Note this already has a default of informing the user of any new files that have been discovered.

## `termination_trigger`

A `multiprocessing.Event` object, if specified the `FileMonitor` will run indefinitely until the trigger is `set`:

```python
import multiprocessing

trigger = multiprocessing.Event()
```

Not needed if `timeout` is specified.blu

## `subprocess_triggers`

If specificed, these are `multiprocessing.Event` objects which are `set` by the `FileMonitor` itself when it is terminated.

## `timeout`

In a case where `termination_trigger` cannot be specified this is the time in seconds the `FileMonitor` will run before timing out.

## `lock_callbacks`

Whether to only allow a single file monitoring thread to execute the callback at a given time. Uses a mutex to prevent the callback being made by two threads at the same time.

## `interval`

File monitoring interval, i.e. how often the thread monitoring a file should check for any updates, the default is `0.1` seconds.

## `log_level`

Logger level for the `FileMonitor`, default is `logging.INFO`, for more information and the display recorded data set to `logging.DEBUG`.

## `flatten_data`

By default Multiparser will pass the data mapping assigning the result 'as is' as an argument to the specified callback. Alternatively if `flatten_data` is set to `True` a delimiter `.` is used to flatten the data into single level key-value pairs. Default is `False`.

```python
# Before
data = {
    "contents": {
        "car": "ford",
        "pet": "dog",
        "house": "chalet"
    }
}

# After

data = {
    "contents.car": "ford",
    "contents.pet": "dog",
    "contents.house": "chalet
}
```

## `plain_logging`

Disable the color formatted logger statements replacing them with plain text only, default is `False`.

## `terminate_all_on_failure`

If set all file threads are terminated when one fails, i.e. all activity is ceased in the case where a thread throws an exception. Default is `False`.
