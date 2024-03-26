# Installation

Multiparser requires Python 3.10+ and is installed using pip:

```bash
pip install ukaea-multiparser
```

Multiparser has two optional extra components that can be installed:

* `arrow`: Adds compatibility with Apache Arrow file types.
* `fortran`: Adds parsing of Fortran named lists.

To install multiparser with any of these extras:

```bash
pip install ukaea-multiparser[arrow]
```
```bash
pip install ukaea-multiparser[fortran]
```
```bash
pip install ukaea-multiparser[arrow,fortran]
```
