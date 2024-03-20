# Parser Functions

Depending on requirement, parsing actions can either be specified using the built-in set of parsers available within Multiparser, or by defining custom parsers.

The module comes with a set of pre-defined parsing functions which can be used when processing a recognised file type from the table below:

|**File Suffix**|**Description**|
|---|---|
|`.json`|JSON key-value file|
|`.toml`|TOML key-value file|
|`.csv`|Comma separated values file|
|`.yaml`|YAML indent based key-value file|
|`.pckl`/`.pickle`/`.pkl`| Pickle file type |
|`.nml`|Fortran named list*|
|`.pqt`/`.parquet`|Apache open source column-orientated data file**|
|`.ft`/`.feather`|Apache arrow portable file format**|

\* Requires the extra `fortran` to be installed.

\*\* Requires the extra `arrow` to be installed.

These are executed when using "tracking" (see [Tracking and Tailing](tracking_and_tailing.md)) on recognised file types without custom parser specification. Note they assume the data can be loaded as a **single level dictionary**.
