# 2024-03-22 [v1.0.0](https://github.com/ukaea/Multiparser/releases/tag/v1.0.0)

* Added ability for FileMonitor to itself set termination triggers to stop external processes if required.
* Added exception handling across all threads whereby any errors occurring in a thread are raised either after the monitor is terminated or causing all threads to be aborted.
* Added functionality for users to create their own custom parsers in both categories.
* Included default parsers for common file types such as JSON, CSV, YAML etc.
* Implemented termination trigger and timeout to allow the user to either abort monitoring via an external process, or set an expiry period.
* Created FileMonitor for observing changes to files specified by the user separating these into two categories "tracked" for files read in full, and "tailed" for those where only added lines are parsed.
