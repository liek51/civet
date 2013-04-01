#! /usr/bin/env python
# process_filelist.py

"""
A short program that will find all files in a specified directory that
match a regex pattern, and return them as a space-sep string.

Support program for the CGA pipelines.

Arguments:
   1: directory
   2: pattern to match
"""

import sys
import re
import os

paths = []
dir = sys.argv[1]
pattern = sys.argv[2]

for f in os.listdir(dir):
    if re.match(pattern, f):
        paths.append(os.path.abspath(os.path.join(dir, f)))

# Interesting... "print" with a trailing comma didn't suppress NL.
# Use write(), instead...

sys.stdout.write(' '.join(paths))
