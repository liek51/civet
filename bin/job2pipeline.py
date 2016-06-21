#! /usr/bin/env python
import os
import argparse

"""
A quick program to scan a directory tree for cluster job numbers in a tree of
pipeline run directories.
"""
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--all', action='store_true',
                        help='Find all occurrences. [Exit after first match.]')
    parser.add_argument('job_num', help="Cluster job number to find in the runs")
    parser.add_argument('tree', nargs='?', default=os.getcwd(),
                        help="Root of the directory tree of runs to search [CWD]")
    return parser.parse_args()

def main():
    args = parse_args()
    tree = args.tree
    for path, dirs, files in os.walk(tree):
        if 'logs' in dirs:
            dirs[:] = ['logs']
        files[:] = [x for x in files if x == 'pipeline_batch_id_list.txt']
        for f in files:
            full_path = os.path.join(path, f)
            for line in open(full_path):
                if args.job_num in line:
                    print path
                    if not args.all:
                        return

if __name__ == '__main__':
    main()
