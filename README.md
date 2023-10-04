# Multiparser

_Multiparser_ is module for performing functionality across a set of output files. Given a set of files, and optionally a list of output parameter names or regex filters the module keeps track of changes by monitoring the "last modified" timestamp of each file and executing the assigned callback.

For example, in the case where a set of model outputs were written to a set of files the user is able to specify which files are of importance, the variables to be noted within these files, and a function to execute whenever a change is made to one of these files.

## The FileMonitor class

The main component of _Multiparser_ is the `FileMonitor` class which is a context manager. Files are individually "tracked" with the option to filter by value names or regular expressions.

```python
with FileMonitor(per_thread_callback=callback_function, interval=10.0) as monitor:
    monitor.add(
        file_name="file_of_interest.toml",
        values=["list", "of", "interesting", "values"],
        regex=[r"^list", r"of\s", r"regular", r"Expressions"]
    )
    monitor.run()
    ...
    monitor.terminate()
```

## Callback Function Specification

The callback function allocated to the `multiparser` monitor must have the form:

```python
def callback(latest_recorded_data: Dict[str, Any], meta_data: Dict[str, Any]) -> None:
    ...
```

receiving two arguments:

* The first, `latest_recorded_data`, is the data recorded which is a dictionary containing key-value pairs
for the parameter name and its value.
* The second, `meta_data`, is a dictionary containing information about the record including the file name and the time at which the record was created.

## Example

```python
import multiparser
import typing
import json
import time


def callback_function(data: typing.Dict[str, typing.Any], meta_data: typing.Dict[str, typing.Any]) -> None:
    """Simple callback whereby the data is just printed to stdout"""
    print(
        json.dumps(
            {
                "time_recorded": meta_data["timestamp"],
                "file": meta_data["file_name"],
                "data": data,
            },
            indent=2,
        )
    )


def run_monitor() -> None:
    with multiparser.FileMonitor(
        per_thread_callback=callback_function,  # callback for each file update
        interval=1.0                           # refresh interval in seconds
    ) as monitor:
        monitor.track("my_file.csv")                          # Track a CSV file in the current directory
        monitor.track("my_output.toml", ["important_param"])  # Track a specific value (by name) in a file
        monitor.track("my_metrics.nml", None, [r"^metric_\d"]) # Track a set of values using regex
        monitor.run()
        for _ in range(10):
            time.sleep(1)
        monitor.terminate()
```
