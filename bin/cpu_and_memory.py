#! /usr/bin/env python
import sys
import os
import csv


def do_walk(root):
    lines = [['Run', 'Step', 'cpu', 'mem', 'vmem', 'wall']]

    run = ''
    for path, dirs, files in os.walk(root):
        if 'breakmer' in dirs:
            dirs.remove('breakmer')
        if 'logs' in dirs:
            run = os.path.split(path)[1]
        if 'concatenated_stdout.txt' in files:
            n = files.index('concatenated_stdout.txt')
            lines += process_file(run, path, files[n])
    return lines


def process_file(run, path, fn):
    ok = False
    lines = []
    name = ''
    for line in open(os.path.join(path, fn)):
        if ok:
            name = line.split()[-1]
            ok = False
        if 'Log info' in line:
            ok = True
        if 'cput' in line:
            resources = line.split()[-1]
            parts = resources.split(',')
            l = [run, name]
            for part in parts:
                l.append(part.split('=')[1])
            lines.append(l)
    return lines


lines = do_walk(sys.argv[1])
of = open('cpu_mem_stats_summary.txt', 'w')
w = csv.writer(of, delimiter='\t')
for l in lines:
    w.writerow(l)
of.close()
