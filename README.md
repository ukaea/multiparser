# Multiparser

_Multiparser_ is module for performing functionality across a set of output files. Given a set of files, and optionally a list of output parameter names or regex filters the module keeps track of changes by monitoring the "last modified" timestamp of each file and executing the assigned callback.

For example, in the case where a set of model outputs were written to a set of files the user is able to specify which files are of importance, the variables to be noted within these files, and a function to execute whenever a change is made to one of these files.

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
```
