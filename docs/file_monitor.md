# The File Monitor

The `FileMonitor` class is the basis of all Multiparser sessions, it allows the user to define what files are tracked, and what to do when a change is detected. The class must be used as a context manager with the Python `with` statement:

```python
with multiparser.FileMonitor(
    per_thread_callback,
    exception_callback,
    notification_callback,
    termination_trigger,
    timeout
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
