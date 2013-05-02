#! /usr/bin/env python

usage = """
Split an input bam file into per-chromosome chunks. Single
argument is the input bam file path.  Outputs will be
named by appending _<chr> to the path before the .bam. E.g.,
input (required, must end in '.bam'): /path/to/my.bam
outputs: /path/to/my_1.bam, /path/to/my_2.bam, ... /path/to/my_X.bam ...
"""

import sys
import os
import subprocess
import shlex

"""
Run an arbitrary command in an external process.
"""
def run_process(cmd, out=sys.stdout, err=sys.stderr):
    args = shlex.split(cmd)
    retcode = subprocess.call(args, stdout=out, stderr=err)
    
    if retcode != 0:
        print >> sys.stderr, 'ERROR: Pipeline failed while running', cmd, 'Terminating run.'
        sys.exit(retcode)

    return

chromosomes = ['1','2','3','4','5','6','7','8','9','10','11','12','13',
               '14','15','16','17','18','19','20','21','22','X','Y','M']

if len(sys.argv) != 2 or sys.argv[1][-4:] != '.bam':
    print >> sys.stderr, usage
    sys.exit(1)

inf = sys.argv[1]
base = inf[:-4]
for chr in chromosomes:
    outf = '{0}_{1}.bam'.format(base, chr)
    cmd = 'samtools view -b -o {0} {1} chr{2}'.format(outf, inf, chr)
    run_process(cmd)
    
# OK.  We've split our bam file by chromosome.  All set, right? Not quite.
# If the sample was female, then the chr Y file has chr1 in it.  I think
# that's a bug, but c'est la vie.  Check for it and delete the file, if so.
yf = base + '_Y.bam'
y = open(y)
line = y.readline()
parts = line.split('\t')
chr = parts[2]
if chr != 'chrY':
    if chr != 'chr1':
        print >> sys.stderr, 'Unexpected result: chrY file contains neither chrY nor chr1: ' +  yf
        sys.exit(1)
    os.remove(yf)
