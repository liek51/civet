#! /usr/bin/env python
 
# read_group_from_fastq.py
# Using a fastq file's name and the contents of its first line, 
# build the option string needed for bwa to mark every read, assuming Illumina
# casava 1.8 conventions.
#
# Input: the fastq file specified as argv[1], the first command line argument.
# Output: the second command line argument, if specified, else, sys.stdout.

import sys
import os

if len(sys.argv) == 1:
    print >> sys.stderr, 'USAGE:', sys.argv[0], 'input-fastq [output-option-line-file]'
    sys.exit(1)
inf = open(sys.argv[1])
if len(sys.argv) == 3:
    of = open(sys.argv[2], 'w')
else:
    of = sys.stdout

# First get the info from the filename
# When split on underscore, the file contains 
#   - customer sample ID in parts[0]
#   - the GES sample ID and barcode in parts 1,2,3 separated by underscore.
fn = os.path.split(sys.argv[1])[1]
fn_parts = fn.split('_')
cust_id = fn_parts[0]
ges_id = '_'.join(fn_parts[1:4])

# Now the parts from the first readname--the first line of the file.
# When split on ':', the readname contains
# - the ID in the first four fields.
# Note: the leading '@' needs to be stripped.
line = inf.readline()
parts = line[1:].split(':')
id = ':'.join(parts[:4])

line = '"@RG\tID:{0}\tLB:{1}\tSM:{2}\tPL:Illumina"'.format(id, ges_id, cust_id)
print >> of, line
