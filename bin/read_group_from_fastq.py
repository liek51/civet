#! /usr/bin/env python

""" 
 read_group_from_fastq.py

 Using a fastq file's name and the contents of its first line, 
 build the option string needed for bwa to mark every read, assuming Illumina
 casava 1.8 conventions.

 Input: the fastq file specified as argv[1], the first command line argument.
 Output: the second command line argument, if specified, else, sys.stdout.

 Notes:
    We will usually be handling standard Illumina Casava 1.8+ output, which
    has a regular file naming format and read name format.  However, we
    want to be able to handle non-normal input as well.  To do so, since
    the formats will be unknown, we will fake the output, just enough to
    let the rest of the pipeline proceed.
"""

import sys
import os
import time

def make_fake():

    # Sleep for 2 seconds, to make sure that a previous invocation
    # will have a different time stamp.
    time.sleep(2)

    ts = time.strftime('%H%M%S')

    id = 'ID_' + ts
    lb = 'LIB_' + ts
    sm = 'SAMPLE_' + ts
    return (id, lb, sm)

if len(sys.argv) == 1:
    print >> sys.stderr, 'USAGE:', sys.argv[0], 'input-fastq [output-option-line-file]'
    sys.exit(1)
inf = open(sys.argv[1])
if len(sys.argv) == 3:
    of = open(sys.argv[2], 'w')
else:
    of = sys.stdout

fake_it = False

# First get the info from the filename
# When split on underscore, the file contains 
#   - customer sample ID in parts[0]
#   - the GES sample ID and barcode in parts 1,2,3 separated by underscore.
fn = os.path.split(sys.argv[1])[1]
fn_parts = fn.split('_')
if len(fn_parts) < 7:
    fake_it = True
else:
    cust_id = fn_parts[0]
    ges_id = '_'.join(fn_parts[1:4])

# Now the parts from the first readname--the first line of the file.
# When split on ':', the readname contains
# - the ID in the first four fields.
# Note: the leading '@' needs to be stripped.
if not fake_it:
    line = inf.readline()
    parts = line[1:].split(' ')
    if len(parts) >= 3:
        fake_it = True

if not fake_it:
    parts = [1].split(':')
    if len(parts) < 7:
        fake_it = True

if not fake_it:
    id = ':'.join(parts[:4])

# Here we've gathered the info from an expected file.  However, if any step
# failed, we need to fake it.
if fake_it:
    id, ges_id, cust_id = make_fake()

line = '@RG\\tID:{0}\\tLB:{1}\\tSM:{2}\\tPL:ILLUMINA'.format(id, ges_id, cust_id)
print >> of, line,
