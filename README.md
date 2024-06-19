# toshi-hazard-post


[![pypi](https://img.shields.io/pypi/v/toshi-hazard-post.svg)](https://pypi.org/project/toshi-hazard-post/)
[![python](https://img.shields.io/pypi/pyversions/toshi-hazard-post.svg)](https://pypi.org/project/toshi-hazard-post/)
[![Build Status](https://github.com/gns-science/toshi-hazard-post/actions/workflows/dev.yml/badge.svg)](https://github.com/gns-science/toshi-hazard-post/actions/workflows/dev.yml)
[![codecov](https://codecov.io/gh/gns-science/toshi-hazard-post/branch/main/graphs/badge.svg)](https://codecov.io/github/gns-science/toshi-hazard-post)


Seismic hazard from pre-calculated relizations. This application impliments the hazard calculation method described in [Calculation of National Seismic Hazard Models with Large Logic Trees: Application to the NZ NSHM 2022](https://doi.org/10.1785/0220230226).


* Documentation: <https://gns-science.github.io/toshi-hazard-post>
* GitHub: <https://github.com/gns-science/toshi-hazard-post>
* PyPI: <https://pypi.org/project/toshi-hazard-post/>
* Free software: AGPL-3.0

## Requriements

toshi-hazard-post requires python 3.10 - 3.12. It relies on other GNS-Science NSHM libraries:

- nzshm-model: <https://github.com/GNS-Science/nzshm-model>
- nzshm-common: <https://github.com/GNS-Science/nzshm-common-py>
- toshi-hazard-store: <https://github.com/GNS-Science/toshi-hazard-store>

In addition, it requires a small number of scientific computation packages such as numba, numpy, and pandas. See [installation notes](docs/installation.md) for more details on how to install the application.


## Usage

```
thp --help
```

```
thp aggregate INPUT_FILE
```
