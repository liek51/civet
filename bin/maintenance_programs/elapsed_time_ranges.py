#! /usr/bin/env python

# Given a directory containing civet pipeline runs, report the
# total elapsed time of successful runs.

import sys
import os
import re

def usage():
    if len(sys.argv) < 2:
        print >> sys.stderr, 'usage:', sys.argv[0], 'top-of-directory-tree ...'
        sys.exit(1)

def time_as_string(elapsed):
    minutes = int(elapsed / 60)
    seconds = int(elapsed - (minutes * 60))
    hours = int(minutes / 60)
    minutes = minutes - (hours * 60)
    days = int(hours / 24)
    hours = hours - (days * 24)
    return '{0}d {1}h {2}m {3}s'.format(days, hours, minutes, seconds)
 
def process_file(dir, fn):
    path = os.path.join(dir, fn)
    log_dir = os.path.split(dir)[0]
    run = os.path.split(log_dir)[0]
    run = os.path.split(run)[1]
    start = os.path.getmtime(log_dir)
    end = os.path.getmtime(path)
    elapsed = end - start
    #print '{0}\t{1}'.format(run, time_as_string(elapsed))
    return elapsed
    
def get_files(start_dir, max, min, sum, count):
    for (dirpath, dirnames, filenames) in os.walk(start_dir):
        if 'log' not in dirpath:
            continue
        for fn in filenames:
            if fn =='Consolidate_log_files-status.txt':
                elapsed = process_file(dirpath, fn)
                if elapsed > max:
                    max = elapsed
                if elapsed < min:
                    min = elapsed
                sum += elapsed
                count += 1
                break
    return max, min, sum, count

def main():
    usage()
    max = sum = count = 0
    min = 9999999999999999
    for dir in sys.argv[1:]:
        max, min, sum, count = get_files(dir, max, min, sum, count)
    print 'Max: ', time_as_string(max)
    print 'Average: ', time_as_string(sum/count)
    print 'Min: ', time_as_string(min)

main()
