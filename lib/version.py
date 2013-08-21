#! /usr/bin/env python

import argparse
import os
import sys

#
# IMPORTANT: UPDATE THIS VERSION LINE WITH EVERY CIVET RELEASE, PER THE SOP
#
CIVET_VERSION='V1.1.0'

def parse_options():
    #
    # "But there are libraries that do option parsing! Why do it by hand?"
    #
    # True, but this module is included in a variety of others that all have
    # their own (real) options.  This code only needs to see whether the
    # command has a single option which is either -v or --version. If
    # so, print out the version string and exit; else simply return and
    # let the rest of the program flow happen.
    #
    # If we use argparse, we have to teach it about all the possible options
    # in all the scripts that import this. Trust us.  We tried that way first.
    #
    options = ['-v', '--version']
    if len(sys.argv) == 2:
        if sys.argv[1] in options:
            print_version_string_and_exit()

def print_version_string_and_exit():
    path = os.path.abspath(sys.argv[0])
    script = os.path.split(path)[1]
    print script, CIVET_VERSION, path
    sys.exit(0)

if __name__ == '__main__':
    parse_options()
