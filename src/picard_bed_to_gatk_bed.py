#! /usr/bin/env python

"""
Convert a target or bait bed file in picard format with SAM headers and 5
columns, into gatk format, with no headers and 3 columns.
"""

import sys
import os

if len(sys.argv) != 2:
    print >> sys.stderr, 'USAGE:', sys.argv[0], 'picard-formatted-bed-file'
    print >> sys.stderr, '    produces outputfile ending with "_gatk.bed"'
    sys.exit(1)

ifn = sys.argv[1]
base = os.path.split(ifn)[1]
if base[-4:] != '.bed':
    print >> sys.stderr, 'Input file does not end in ".bed". Exiting...'
    sys.exit(1)
ofn = ifn[:-4] + '_gatk.bed'

of = open(ofn, 'w')

for line in open(ifn):
    if line[0] == '@':
        continue
    parts = line.split('\t')
    if "_" in parts[0]:
        continue
    print >> of, ' '.join(parts[:3])

of.close()
