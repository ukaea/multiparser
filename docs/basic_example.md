# Basic Example

Consider the case where a user has set up an RestAPI for receiving metrics across a server. A request should be made only if new values are available and the files being monitored have been modified.

First we define a callback which will be executed each time changes are detected:

```python
def file_update_callback(data: dict[str, Any], metadata: dict[str, Any]) -> None:
    _params = {"data": data}
    _response = requests.post(API_ENDPOINT, params=_params)
```

Now we initialise a `FileMonitor` to perform the parallel parsing of any log files from our process, we create an `Event` to trigger termination of the monitor when the process finishes:

```python

trigger = multiprocess.Event()

with FileMonitor(
    per_thread_callback=file_update_callback,
    termination_trigger=trigger,
    interval=1
) as monitor:
```

The files we are interested in are logs of the form `session_X_Y.log`, we use the `tail` method to track any additions to the file since last modification and look for key-value pairs of the form `key = value`:

```python
monitor.track(
    path_glob_exprs="session_*.log",
    tracked_values=[r"(\w+)\s*=\s*([\d\w\.]+)"]
)
```

finally we set the monitor to run:

```python
monitor.run()
```

and then set the process to run waiting for it to complete before terminating the monitor with the trigger:

```python
subprocess.Popen(["./process"], shell=False)
subprocess.poll()
trigger.set()
```

!!! example "Sending data to a RestAPI"

    ```python
    import requests
    import multiparser
    import multiprocessing
    import subprocess

    API_ENDPOINT: str = "https://api.example.com/v1/metrics"

    trigger = multiprocessing.Event()

    def file_update_callback(data: dict[str, Any], metadata: dict[str, Any]) -> None:
        _params = {"data": data}
        _response = requests.post(API_ENDPOINT, params=_params)

    with FileMonitor(
        per_thread_callback=file_update_callback,
        timeout=120,
        termination_trigger=trigger,
        interval=1
    ) as monitor:
        monitor.track(
            path_glob_exprs="session_*.log",
            tracked_values=[r"(\w+)\s*=\s*([\d\w\.]+)"]
        )

        monitor.run()

        subprocess.Popen(["./process"], shell=False)
        subprocess.poll()
        trigger.set()
    ```
