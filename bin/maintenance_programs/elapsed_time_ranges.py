#! /usr/bin/env python

# Copyright (C) 2016  The Jackson Laboratory
#
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#


# Given a directory containing civet pipeline runs, report the
# total elapsed time of successful runs.

import sys
import os


def usage():
    if len(sys.argv) < 2:
        usage_msg()


def usage_msg():
    print >> sys.stderr, 'usage:', sys.argv[0], \
        '[-h hours] top-of-directory-tree ...'
    print >> sys.stderr, 'if -h hours is specified, list all '\
            'pipelines that took more than hours elapsed time'
    print >> sys.stderr, 'if hours is negative, list all pipelines ' \
            'that took less than abs(hours)'
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
    start = os.path.getctime(log_dir)
    end = os.path.getmtime(path)
    elapsed = end - start
    #print '{0}\t{1}'.format(run, time_as_string(elapsed))
    return elapsed
    

def get_files(start_dir, max, min, sum, count, long_secs):
    for (dirpath, dirnames, filenames) in os.walk(start_dir):
        if 'log' not in dirpath:
            continue
        for fn in filenames:
            if fn =='Consolidate_log_files-status.txt':
                elapsed = process_file(dirpath, fn)
                if elapsed < 120:
                    print 'Ignoring ', time_as_string(elapsed), dirpath
                    continue
                if long_secs is not None:
                    if (long_secs > 0 and elapsed > long_secs) or \
                        (long_secs < 0 and elapsed < -long_secs):
                        print time_as_string(elapsed), dirpath

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
    if sys.argv[1] == '-h':
        try:
            long_secs = int(sys.argv[2]) * 3600
        except ValueError:
            print >> sys.stderr, 'When -h is specified, the following '\
                    'token must be an integer.'
            usage_msg()

        dirs = sys.argv[3:]
    else:
        long_secs = None
        dirs = sys.argv[1:]
    for dir in dirs:
        max, min, sum, count = get_files(dir, max, min, sum,
                                         count, long_secs)

    try:
        avg = sum / count
        print 'Times for {0} runs:'.format(count)
        print 'Max: ', time_as_string(max)
        print 'Average: ', time_as_string(sum/count)
        print 'Min: ', time_as_string(min)
    except ZeroDivisionError:
        print 'No runs have completed.'

main()
