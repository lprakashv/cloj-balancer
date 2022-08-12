# cloj-balancer [WIP]

A Simple Java Sockets API (`java.net.*`, `java.io.*`) based Load Balancer app written in __"Clojure"__

This support load-balancing policies:
- round-robin
- random

## Installation

Download from https://github.com/lprakashv/cloj-balancer

## Usage

FIXME: explanation

Run the project, overriding the name to be greeted:

    $ clojure -X:run-m load-balancer-port management-port
    Started Management server at port :  9001
    Started LB server at port :  9000

Build an uberjar:

    $ clojure -X:uberjar

This will update the generated `pom.xml` file to keep the dependencies synchronized with
your `deps.edn` file. You can update the version (and SCM tag) information in the `pom.xml` using the
`:version` argument:

    $ clojure -X:uberjar :version '"1.2.3"'

If you don't want the `pom.xml` file in your project, you can remove it, but you will
also need to remove `:sync-pom true` from the `deps.edn` file (in the `:exec-args` for `depstar`).

Run that uberjar:

    $ java -jar cloj-balancer.jar

## Options

FIXME: listing of options this app accepts.

## Examples

...

### Bugs

...

### Any Other Sections
### That You Think
### Might be Useful

## License

Copyright © 2021 lprakashv@gmail.com

_EPLv1.0 is just the default for projects generated by `clj-new`: you are not_
_required to open source this project, nor are you required to use EPLv1.0!_
_Feel free to remove or change the `LICENSE` file and remove or update this_
_section of the `README.md` file!_

Distributed under the Eclipse Public License version 1.0.
