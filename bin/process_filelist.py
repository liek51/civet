#! /usr/bin/env python

"""
Copyright (C) 2016  The Jackson Laboratory

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

"""
A short program that will find all files in a specified directory that
match a regex pattern, and return them as a space-sep string.

Support program for Civet.

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
