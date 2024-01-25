<center>
<img src="docs/media/logo.png", width="200">
</center>

# Multiparser

_Multiparser_ is module for performing functionality across a set of output files. Given a set of files, and optionally a list of output parameter names or regex filters the module keeps track of changes by monitoring the "last modified" timestamp of each file and executing the assigned callback.

For example, in the case where a set of model outputs were written to a set of files the user is able to specify which files are of importance, the variables to be noted within these files, and a function to execute whenever a change is made to one of these files.

## Installation

The module is currently in development, to install it run:

```sh
pip install <repository>
```

To install optional extras `arrow` and `fortran` list them during the install, e.g.:

```sh
pip install <repository>[fortran,arrow]
```

## The FileMonitor class

The main component of _Multiparser_ is the `FileMonitor` class which is a context manager. Files are individually "tracked" with the option to filter by value names or regular expressions. These files are read as a whole, suitable for files such as JSON and TOML:

```python
with FileMonitor(
    per_thread_callback=callback_function,
    lock_callback=True,
    interval=10.0,
    flatten_data=True
) as monitor:
    monitor.track(
        path_glob_exprs=["file_of_interest.toml", "out_dir/*.other"],
        tracked_values=[
            "list", "of", "interesting", "values",
            re.compile(r"^list"),
            re.compile(r"of\s"),
            re.compile(r"regular"),
            re.compile(r"Expressions")
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
    tracked_values=[re.compile(r"my_param=(\d+)")],
    labels=["My Parameter"]
)
```

The option `flatten_data` will flatten the recorded values into a single level key-value dictionary, by default this is set to `False`. This has been added for cases whereby callbacks rely on such a structure, for more control over the structure of outputs see [custom parsing](#creating-custom-parsers).


### "Lazy" vs Custom Parsing

_Multiparser_ contains two methods for parsing, the first _lazy_ parsing will use the most appropriate parsing function to read a file based on its extension loading it in its entirety. This is useful when no customisation of the parsing process is required. The parse process assumes that the loaded object is (or can be loaded as) **a single level dictionary**. Lazy parsing supports:

* TOML
* YAML
* Pickle
* Fortran-90 Named List (variables are read into a dictionary, requires "fortran" extra to be installed).
* Feather (requires "arrow" extra to be installed)
* Parquet (requires "arrow" extra to be installed)
* CSV

Custom parsing allows the user to specify their own function for extracting data, for more information see [_Creating Custom Parsers_](#creating-custom-parsers) below.

### Using RegEx

The two methods of using regular expressions are either a single expression:

```python
tracked_values=[re.compile(r"\w+=(\d+\.\d+)")],
labels=["value"]
```

a label argument for this value will then be required, if multiple results are found the value
will be given a numerical suffix.

The alternative is to provide a RegEx which captures the variable name directly:

```python
tracked_values=[re.compile(r"(\w+)=(\d+\.\d+)"), re.compile(r"(\w+_\d+)=(\d+\.\d+)")],
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
import re


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
        interval=1.0                            # refresh interval in seconds
    ) as monitor:
        monitor.track("my_file.csv")                          # Track a CSV file in the current directory
        monitor.track("*.toml", ["important_param"])          # Track a specific value (by name) in a set of files
        monitor.track(
            "my_metrics.nml",
            [re.compile(r"^metric_\d")]
        )                                                     # Track a set of values using regex
        monitor.exclude("param*.toml")                        # Exclude file patterns from tracking
        monitor.run()
        for _ in range(10):
            time.sleep(1)
        monitor.terminate()
```

## Creating Custom Parsers

For some cases it may be easier to create a parser function of your own, this is particularly useful for custom layout log files.

You can provide a parser function and arguments _instead of_ a list of regular expressions or variable names when tracking or tailing:

```python
from pandas import read_hdf

monitor.track(
    "my_file.h5",
    parser_func=read_hdf,
    parser_args={"key": "my_key"}
)
```

### File Parsers

File parsers are those which take a file name and load the file as a whole, these are typically used when the file content is static, or the file is overwritten over time. To define a custom file parser ensure the function uses the `file_parser` decorator and takes a mandatory argument of the `input_file` path:

```python
from typing import Dict, Tuple, Any
from multiparser import file_parser

@file_parser
def custom_file_parser(input_file: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    ...
    return {}, out_data
```

the function should return two dictionaries, the first is used by the decorator to assemble any metadata relating to the record, and the second the data itself which should be a single level dictionary with key-value pairs.


### Log Parsers

A log parser will only read the new content within a file ignoring any previously recorded lines, a custom log parser uses the `log_parser` decorator and takes a mandatory argument of the `file_content` to parse:

```python
from typing import Dict, Tuple, Any, Union, List
from multiparser import file_parser

LogParseContent = Union[Dict[str, Any], List[Dict[str, Any]]]

@log_parser
def custom_log_parser(file_content: str) -> Tuple[Dict[str, Any], LogParseContent]:
    ...
    return {}, out_data
```

Unlike a file parser, a log parser may return either a single level dictionary with key-value pairs, or a list of such dictionaries (covering the case where multiple identical blocks are read, thus preventing overwrite). Log file parsers are validated prior to running.
