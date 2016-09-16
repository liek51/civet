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
consolidate_logs.py

A program to concatenate all the various log files into three,
with header sections separating the content from different input
logs.
"""

import sys
import os
import re

if len(sys.argv) != 2:
    print >> sys.stderr, 'USAGE:', sys.argv[0], 'path-to-log-dir'
    sys.exit(1)

def get_file_names(dir):
    """
    Return lists of *-run.log, *.o *-err.log and *.e.
        dir: the path to the directory to be examined.
    """
    runs = []
    o_s = []
    errs = []
    es = []
    shells = []
    versions = []
    batch_stderr = []

    pattern = re.compile(r'^.*\.e$')

    for f in os.listdir(dir):
        if f.startswith("rm_temps_consolidate_logs"):
            continue
        elif pattern.match(f):
            batch_stderr.append(f)
        elif f[-8:] == '-run.log':
            runs.append(f)
        elif f[-2:] == '.o':
            o_s.append(f)
        elif f[-8:] == '-err.log':
            errs.append(f)
        elif f[-3:] == '.sh':
            shells.append(f)
        elif f[-12:] == '-version.log':
            versions.append(f)
        else:
            # Ignore the other files.
            # Note: We're intentionally NOT consolidating the
            # status files, because the monitor program depends on
            # them.
            pass

    runs.sort()
    o_s.sort()
    errs.sort()
    es.sort()
    shells.sort()
    versions.sort()

    return(runs, o_s, errs, batch_stderr, shells, versions)


def write_header(of, fn):
    """
    Write a file header to an already open output file.
        of: the already opened file object
        fn: the name of the file to be logged in the header.
    """
    print >> of, '#'*80
    print >> of, '#'*80
    print >> of, '#'*8
    print >> of, '#'*8, 'Log info from:'
    print >> of, '#'*8, '   ', fn
    print >> of, '#'*8
    print >> of, '#'*80
    print >> of, '#'*80


def write_end_record(of, fn):
    """
    Write a file end record to an already open output file.
        of: the already opened file object
        fn: the name of the file to be logged in the end record.
    """
    print >> of, '#'*80
    print >> of, '#'*80
    print >> of, '#'*8
    print >> of, '#'*8, 'End of log info from:'
    print >> of, '#'*8, '   ', fn
    print >> of, '#'*8
    print >> of, '#'*80
    print >> of, '#'*80
    print >> of

def output_file(fn, dir, of):
    """
    Concatenate a file to the appropriate output file, with an 
    identifying header.  The output file is already open for writing.
    The input file is identified by its filename and containing
    directory and is not open.  The input file is deleted.
        fn: the name of the input file
        dir: the path of the containing directory
        of: the already opened file object
    """
    write_header(of, fn)
    path = os.path.join(dir, fn)
    for line in open(path):
        of.write(line)
    write_end_record(of, fn)
    os.remove(path)


def process_file_list(dir, list, ofn):
    """
    Handle a list of files by calling output_file() for each.
        dir: the path to the containing directory of both
             the input and output files.
        list: the list of files to be output to the merged file.
        ofn: the name of the to-be-created output file.
    """
    of = open(os.path.join(dir, ofn), 'w')
    for fn in list:
        output_file(fn, dir, of)
    of.close()


def handle_batch_errs(dir, list):
    """
    Handle *.e files; they're all empty.
    We can't keep torque from creating these files, but we don't
    need to leave them hanging around...
    """
    for fn in list:
        file_path = os.path.join(dir, fn)
        if os.path.getsize(file_path) == 0:
            os.remove(os.path.join(dir, fn))



def main():
    """
    The main... clean up the logs directory by consolidating the
    output and error log files created by the various batch jobs.
    """
    dir = sys.argv[1]
    (runs, o_s, errs, batch_stderr, shells, versions) = get_file_names(dir)

    process_file_list(dir, runs, 'concatenated_run_logs.txt')
    process_file_list(dir, o_s, 'concatenated_stdout.txt')
    process_file_list(dir, errs, 'concatenated_stderr.txt')
    process_file_list(dir, versions, 'concatenated_versions.txt')

    handle_batch_errs(dir, batch_stderr)

main()
