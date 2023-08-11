<!-- # Stable release

toshi-hazard-post is not yet released on pypi. When it is:
To install toshi-hazard-post, run this command in your
terminal:

``` console
$ pip install toshi-hazard-post
```

This is the preferred method to install toshi-hazard-post, as it will always install the most recent stable release.

If you don't have [pip][] installed, this [Python installation guide][]
can guide you through the process. -->

# From source
This is currently the only installation option. The source for toshi-hazard-post can be downloaded from the [Github repo][].

<!-- You can either clone the public repository: -->

``` console
$ git clone https://github.com/GNS-Science/toshi-hazard-post.git
```

<!-- Or download the [tarball][]: -->

<!--
console
$ curl -OJL https://github.com/chrisbc/toshi-hazard-post/tarball/master
-->

Once you have a copy of the source, you can install it with:

``` console
$ pip install .
```

  [pip]: https://pip.pypa.io
  [Python installation guide]: http://docs.python-guide.org/en/latest/starting/installation/
  [Github repo]: https://github.com/%7B%7B%20cookiecutter.github_username%20%7D%7D/%7B%7B%20cookiecutter.project_slug%20%7D%7D
  [tarball]: https://github.com/%7B%7B%20cookiecutter.github_username%20%7D%7D/%7B%7B%20cookiecutter.project_slug%20%7D%7D/tarball/master

## Testing
All tests can be run with pytest 

```console
$ pytest
```