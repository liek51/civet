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

import os
import sys
import subprocess
import inspect

def version_from_git():
    cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( 
        inspect.getouterframes(inspect.currentframe())[2][0] ))[0]))
    try:
        err = '' # Make sure err exists, if we hit the except clause
        p = subprocess.Popen('cd {0}; git describe --tags'.format(cmd_folder),
                shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        out, err = p.communicate()
    except OSError:
        err = True
    # Mask any errors, for instance not running in a git working directory.
    if err:
	    out = '(undetermined)'
    return 'V' + out.strip()

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
    print script, version_from_git(), path
    sys.exit(0)

if __name__ == '__main__':
    parse_options()
