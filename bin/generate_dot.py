#! /usr/bin/env python

from __future__ import print_function

import sys
import re

__author__ = 'asimons'

"""
Take in a civet pipeline_batch_id_list.txt file and produce a dot file
suitable for input to graphviz.
"""

deps = {}
names = {}

if len(sys.argv) == 1:
    print("You must specify a civet pipeline list file", file=sys.stderr)
    sys.exit(1)

pipename = None
for line in open(sys.argv[1]):
    # Line format is:
    # 516200.cadillac-master.jax.org  RNASeqSingleSamplePE.xml_S7_REMOVEFILES_T1_RemoveOtherFiles     ['516197', '516195']
    # The job at the start if the line depends on the jobs in the
    # brackets, and has the name that is between.
    job_info, dep_info = line.strip()[:-1].split('[')
    parts = job_info.strip().split()
    if pipename is None:
        pipename = parts[1].split('.')[0]
    job = parts[0].split('.')[0]

    name_precursor = parts[1]
    pat = r'.*_S[\d]+_(.*)_T[\d]+_(.*)'
    sub = r"\1\\n\2"
    names[job] = re.sub(pat, sub, name_precursor)
    if len(dep_info) > 4:
        depends_on = [x.strip()[1:-1] for x in dep_info.split(',')]
        for d in depends_on:
            if d not in deps:
                deps[d] = []
            deps[d].append(job)

ofn = '{0}_flow_diagram.dot'.format(pipename)
of = open(ofn, 'w')
print("digraph {0} {{".format(pipename), file=of)
for n in names:
    print('  {0} [label="{1}"]'.format(n, names[n]), file=of)
for j in deps:
    print('  {0} -> {1}'.format(j, ', '.join(deps[j])), file=of)
print("}", file=of)
print('Created: {0}'.format(ofn))
