"""Lets the package be run as ``python -m elt`` (alias for ``python -m elt.run``)."""

import sys

from elt.run import main

if __name__ == "__main__":
    sys.exit(main())
