# Tracking and Tailing Files

In Multiparser there are two ways in which changes to a file can be monitored, **tracking** which focuses on revisions of a file and **tailing** which looks at incremental additions.

## Tracking
When a file is _tracked_ it is read in its entirety each time a change is dedicated. For example in the case of JSON and TOML files the data are held as key-value pairs which may be modified during the execution of a process.


## Tailing
When a file is _tailed_ only the most recent additions to the file are read, for example in the case of log files we are only interested in the most recent lines written to the file and can ignore those already parsed previously. This is particularly important when reading large outputs where logs can be kilobytes or even megabytes in size.