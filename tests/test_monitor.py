import pytest
import tempfile
import os
import glob
import time
import random
import json

import multiparser.monitor as cc_monitor

from conftest import fake_csv, fake_toml, fake_nml


@pytest.mark.monitor
@pytest.mark.parametrize("repeat", range(5))
def test_run_on_directory(repeat) -> None:
    _interval: float = 0.1
    with tempfile.TemporaryDirectory() as temp_d:
        for _ in range(random.randint(2, 10)):
            random.choice([fake_csv, fake_nml, fake_toml])(temp_d)

        def per_thread_callback(data, meta):
            print(json.dumps({"time_recorded": meta["timestamp"], "file": meta["file_name"], "data": data}, indent=2))

        with cc_monitor.FileMonitor(per_thread_callback, interval=_interval) as monitor:
            for file in glob.glob(os.path.join(temp_d, "*")):
                monitor.track(file)
            monitor.run()
            for _ in range(10):
                time.sleep(_interval)
            monitor.terminate()

