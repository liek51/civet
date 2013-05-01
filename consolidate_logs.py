#! /usr/bin/env python

""" 
consolidate_logs.py

A program to concatenate all the various log files into three,
with header sections separating the content fro different input
logs.
"""

import sys, os

if len(sys.argv) != 2:
    print >> sys.stderr, 'USAGE:', sys.argv[0], 'path-to-log-dir'
    sys.exit(1)

"""
Return lists of *-run.log, *.o *-err.log and *.e.
"""
def get_file_names(dir):
    runs = []
    o_s = []
    errs = []
    es = []
    statuses = []
    shells = []

    all = os.listdir(dir)
    for f in all:
        if f[-8:] == '-run.log':
            runs.append(f)
        elif f[-2:] == '.o':
            o_s.append(f)
        elif f[-8:] == '-err.log':
            errs.append(f)
        elif f[-2:] == '.e':
            es.append(f)
        elif f[-3:] == '.sh':
            shells.append(f)
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

    return(runs, o_s, errs, es, shells)


"""
Write a file header to an already open output file.
"""
def write_header(of, fn):
    print >> of, '#'*80
    print >> of, '#'*80
    print >> of, '#'*8
    print >> of, '#'*8, 'Log info from:'
    print >> of, '#'*8, '   ', fn
    print >> of, '#'*8
    print >> of, '#'*80
    print >> of, '#'*80


"""
Write a file end record to an already open output file.
"""
def write_end_record(of, fn):
    print >> of, '#'*80
    print >> of, '#'*80
    print >> of, '#'*8
    print >> of, '#'*8, 'End of log info from:'
    print >> of, '#'*8, '   ', fn
    print >> of, '#'*8
    print >> of, '#'*80
    print >> of, '#'*80
    print >> of

"""
Concatenate a file to the appropriate output file, with an 
identifying header.  The output file is already open for writing.
The input file is not open.
"""
def output_file(fn, dir, of):
    write_header(of, fn)
    path = os.path.join(dir, fn)
    for line in open(path):
        of.write(line)
    write_end_record(of, fn)
    os.remove(path)


"""
Handle a list of files.
"""
def process_file_list(dir, list, ofn):
    of = open(os.path.join(dir, ofn), 'w')
    for fn in list:
        output_file(fn, dir, of)
    of.close()


"""
Move the shell scripts down a level.
"""
def move_shell_scripts(dir, list):
    dest_dir = os.path.join(dir, 'submitted_shell_scripts')
    if not os.path.exists(dest_dir):
        os.mkdir(dest_dir)
    for fn in list:
        cur_path = os.path.join(dir, fn)
        dest_path = os.path.join(dest_dir, fn)
        os.rename(cur_path, dest_path)


"""
Handle *.e files; they're all empty.
"""
def handle_es(dir, list):
    for fn in list:
        os.remove(os.path.join(dir, fn))



"""
The main...
"""
def main():
    dir = sys.argv[1]
    (runs, o_s, errs, es, shells) = get_file_names(dir)

    process_file_list(dir, runs, 'concatenated_run_logs.txt')
    process_file_list(dir, o_s, 'concatenated_stdout.txt')
    process_file_list(dir, errs, 'concatenated_stderr.txt')
    move_shell_scripts(dir, shells)
    handle_es(dir, es)

main()
