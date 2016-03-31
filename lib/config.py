#! /usr/bin/env python

#
# Copyright (C) 2016  The Jackson Laboratory
#
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

import json
import os
import sys

import utilities


with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../conf/config.json")) as conf_file:
    # read all lines from config file into a list of lines
    # strip out // line comments and blank lines
    content = [x for x in conf_file.readlines() if not x.lstrip().startswith("//") and x.strip()]

try:
    # join the remaining config file lines and parse and json
    __config = json.loads(''.join(content))
except Exception as e:
    sys.exit("Error loading config file: {}".format(e))

__valid_params = [
    'io_sync_sleep',
    'civet_job_python_module',
    'purge_user_modulefiles',
]

for param in __config.keys():
    if param not in __valid_params:
        raise ValueError("Invalid config file parameter: '{}'".format(param))

# validate some config file parameters

io_sync_sleep = __config.get('io_sync_sleep')
if io_sync_sleep:
    if not isinstance(io_sync_sleep, int):
        raise ValueError("io_sync_sleep must be an integer")
    if io_sync_sleep < 0:
        raise ValueError("io_sync_sleep must be an integer >= 0")

civet_job_python_module = __config.get('civet_job_python_module')
if civet_job_python_module and not isinstance(civet_job_python_module, utilities.string_types):
        raise ValueError("civet_job_python_modules must be a string")

purge_user_modulefiles = __config.get('purge_user_modulefiles')
if purge_user_modulefiles:
    if not isinstance(purge_user_modulefiles, bool):
        raise ValueError("purge_user_modulefiles must be a boolean")
else:
    purge_user_modulefiles = False

