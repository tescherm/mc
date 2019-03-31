# mc - Memcached Lite

Basic golang caching service using [cloud native](https://www.cncf.io/) open source technologies like [grpc](https://grpc.io/), [prometheus](https://prometheus.io/) and [docker](https://www.docker.com/). The implementation borrows ideas from [Writing a very fast cache service with millions of entries in Go](https://allegro.tech/2016/03/writing-fast-cache-service-in-go.html) and [groupcache](https://github.com/golang/groupcache).

## Building

To build `mc` you will need:

1.  [Git](https://git-scm.com/downloads)
2.  [Docker](https://docs.docker.com/install/)
3.  [Make](https://www.gnu.org/software/make/)

## Development Environment

Make sure that your `GOPATH` is set to
the directory that contains your `src` directory. For example:

    $ export GOPATH=/home/foo/go
    $ mkdir -p $GOPATH/src/github.com/tescherm
    $ cd $GOPATH/src/github.com/tescherm && git clone git@github.com:tescherm/mc.git
    $ cd mc

## Tests and Validation

The full set of tests can be run with:

    $ make test
    $ make test-integration

The full set of lint checks can be run with:

    $ make check

