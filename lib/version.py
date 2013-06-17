#! /usr/bin/env python

import argparse
import os
import sys

#
# IMPORTANT: UPDATE THIS VERSION LINE WITH EVERY CIVET RELEASE, PER THE SOP
#
CIVET_VERSION='V0.5.0b8'

def parse_options():
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', '-v', action='store_true', 
                        help='display program version')
    args = parser.parse_args()
    #print args

    if args.version:
        path = os.path.abspath(sys.argv[0])
        script = os.path.split(path)[1]
        print script, CIVET_VERSION, path
        sys.exit(0)
if __name__ == '__main__':
    parse_options()
