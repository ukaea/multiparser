# Multiparser

_Multiparser_ is module for performing functionality across a set of output files. Given a set of files, and optionally a list of output parameter names or regex filters the module keeps track of changes by monitoring the "last modified" timestamp of each file and executing the assigned callback.

For example, in the case where a set of model outputs were written to a set of files the user is able to specify which files are of importance, the variables to be noted within these files, and a function to execute whenever a change is made to one of these files.

## The FileMonitor class

The main component of _Multiparser_ is the `FileMonitor` class which is a context manager. Files are individually "tracked" with the option to filter by value names or regular expressions. These files are read as a whole, suitable for files such as JSON and TOML:

```python
with FileMonitor(
    per_thread_callback=callback_function,
    lock_callback=True,
    interval=10.0,
) as monitor:
    monitor.track(
        path_glob_exprs=["file_of_interest.toml", "out_dir/*.other"],
        tracked_values=[
            "list", "of", "interesting", "values",
            r"^list", r"of\s", r"regular", r"Expressions"
        ],
        static=True
    )
    monitor.run()
    ...
    monitor.terminate()
```

The `lock_callback` option on the `FileMonitor` instance ensures only one thread can execute the callback triggered when a file is monitored at a time. The argument `static` for the `track` method tells the file monitor that once the file appears, it will not be modified again so the monitoring can terminate.

For "tailing" files such as logs the `tail` method is used, regular expressions can be optionally specified to extract particular values (see section below). Where there are two capture groups the label for the parameter is taken to be the
first match in the group. These labels can be overwritten using the `labels` argument, if `None` is passed as one of the labels then it is assumed the label will be extracted via regex for that particular entry:

```python
monitor.tail(
    path_glob_exprs=["output.log"],
    regular_exprs=["my_param=(\d+)"],
    labels=["My Parameter"]
)
```

### Using RegEx

The two methods of using regular expressions are either a single expression:

```python
path_glob_exprs=[r"\w+=(\d+\.\d+)"],
labels=["value"]
```

a label argument for this value will then be required, if multiple results are found the value
will be given a numerical suffix.

The alternative is to provide a RegEx which captures the variable name directly:

```python
path_glob_exprs=[r"(\w+)=(\d+\.\d+)", r"(\w+_\d+)=(\d+\.\d+)"],
labels=["value", None]
```

here provision of a label is now optional, the label `None` means the label will be taken from the regex itself.

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
        monitor.track("*.toml", ["important_param"])          # Track a specific value (by name) in a set of files
        monitor.track("my_metrics.nml", [r"^metric_\d"])      # Track a set of values using regex
        monitor.exclude("param*.toml")                        # Exclude file patterns from tracking
        monitor.run()
        for _ in range(10):
            time.sleep(1)
        monitor.terminate()
```
