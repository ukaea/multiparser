# Installation

Multiparser requires Python 3.10+ and is installed using pip:

```bash
pip install git+https://git.ccfe.ac.uk/kzarebsk/multiparser
```

Multiparser has two optional extra components that can be installed:

* `arrow`: Adds compatibility with Apache Arrow file types.
* `fortran`: Adds parsing of Fortran named lists.

To install multiparser with any of these extras:

```bash
pip install git+https://git.ccfe.ac.uk/kzarebsk/multiparser#egg=multiparser[arrow]
```
```bash
pip install git+https://git.ccfe.ac.uk/kzarebsk/multiparser#egg=multiparser[fortran]
```
```bash
pip install git+https://git.ccfe.ac.uk/kzarebsk/multiparser#egg=multiparser[arrow,fortran]
```
