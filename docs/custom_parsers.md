# Custom Parsers

In the situation where the output files from a process are not processable by any of the built-in parsers, a custom parser can be created to extract the information of interest.

## File Parsers

File parsers are used for tracking, they take the path of an identified candidate as an argument, parse the data from that file and return a key-value mapping of the data of interest. To create a custom parser you will need to use the `multiparser.parsing.file.file_parser` decorator. The function should take an argument `file_path` which is the file path, and allow an arbitrary number of additional arguments (`**_`) to be compatible with the decorator. It should return two dictionaries containing relevant metadata (usually left blank), and the parsed information:

```python
from typing import Any
import multiparser.parsing.file as mp_file_parse

@mp_file_parse.file_parser
def parse_user_file_type(file_path: str, **_) -> tuple[dict[str, Any], dict[str, Any]]:
    with open(file_path) as in_f:
        file_lines = in_f.readlines()

    data = {}

    for line in file_lines:
        key, value = line.split(":", 1)
        data[key.strip().lower().replace(" ", "_")] = value.strip()

    return {}, data
```

To use the parser within a `FileMonitor` session:

```python
with multiparser.FileMonitor(timeout=10) as monitor:
    monitor.track("custom_file.log", parser_func=parser_user_file_type)
    monitor.run()
```

In the case where you would like your parser function to accept additional keyword arguments you can add these
to the definition and pass them to tracking using the `parser_kwargs` argument.

## Log Parsers

In the case where the custom parser will be used in file "tailing", that is read only the latest information appended to the file, the `multiparser.parsing.tail.log_parser` decorator is used when defining the function. The function should take an argument `file_content` which is a string containing the latest read content, and allow an arbitrary number of additional arguments (`**_`) to be compatible with the decorator. It should return two dictionaries containing relevant metadata (usually left blank), and the parsed information:

```python
from typing import Any
import multiparser.parsing.tail as mp_tail_parse

@mp_tail_parse.log_parser
def parse_user_file_type(file_content: str, **_) -> tuple[dict[str, Any], dict[str, Any]]:
    file_lines = file_content.split("\n")

    data = {}

    for line in file_lines:
        key, value = line.split(":", 1)
        data[key.strip().lower().replace(" ", "_")] = value.strip()

    return {}, data
```

To use the parser within a `FileMonitor` session:

```python
with multiparser.FileMonitor(timeout=10) as monitor:
    monitor.tail("custom_file.log", parser_func=parser_user_file_type)
    monitor.run()
```

In the case where you would like your parser function to accept additional keyword arguments you can add these
to the definition and pass them to tracking using the `parser_kwargs` argument.
