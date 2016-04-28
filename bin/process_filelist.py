#! /usr/bin/env python

# Copyright 2016 The Jackson Laboratory
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
