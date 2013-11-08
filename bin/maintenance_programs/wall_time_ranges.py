#! /usr/bin/env python

# Given a directory containing civet pipeline runs, report
# the max, min and average walltime for each cluster job
# in the pipeline.

import sys
import os
import re

def usage():
    if len(sys.argv) < 2:
        print >> sys.stderr, 'usage:', sys.argv[0], 'top-of-directory-tree ...'
        sys.exit(1)

class JobTimes(object):
    def __init__(self, job):
        self.job = job
        self.max = -1
        self.min = 999999999
        self.total = 0
        self.count = 0
        self.requested = 0
        pass
    def register_time(self, used_timestr, requested_timestr):
        secs = JobTimes.to_seconds(used_timestr)
        self.req = requested_timestr.split('=')[1]
        if secs < self.min:
            self.min = secs
        if secs > self.max:
            self.max = secs
        self.total += secs
        self.count += 1
        
    def __str__(self):
        max = JobTimes.from_seconds(self.max)
        min = JobTimes.from_seconds(self.min)
        avg = JobTimes.from_seconds(int(float(self.total)/float(self.count)))
        return '{0}\t{1}\t{2}\t{3}\t{4}\t{5}'.format(
             self.job, self.req, max, avg, min, self.count)

    @staticmethod
    def header():
        return 'Name\tRequested\tMax\tAverage\tMin\tCount'

    @staticmethod
    def to_seconds(timestr):
        secs = 0
        timestr = timestr.split('=')[1]
        parts = timestr.split(':')
        if len(parts) == 4:
            days = int(parts[0])
            secs += days * (24 * 3600)
            parts = parts[1:]
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = int(parts[2])
        secs += hours*3600 + minutes*60 + seconds
        return secs

    @staticmethod
    def from_seconds(insecs):
        days = insecs/(24*3600)
        rem = insecs - days*(24*3600)
        hours = rem/(3600)
        rem = rem - hours*3600
        minutes = rem/60
        seconds = rem - minutes*60
        if days:
            return '{0}:{1}:{2}:{3}'.format(days, hours, minutes, seconds)
        else:
            return '{0}:{1}:{2}'.format(hours, minutes, seconds)

def process_file(dir, fn, jobs):
    path = os.path.join(dir, fn)
    for line in open(path):
        if 'requested_walltime' in line:
            req = line.strip()
        elif 'walltime' in line:
            used = line.strip()
    
    job = re.sub('(.*)-status.txt', r'\1', fn)
    if job not in jobs:
        jobs[job] = JobTimes(job)
    jobs[job].register_time(used, req)

def get_files(start_dir, jobs):
    for (dirpath, dirnames, filenames) in os.walk(start_dir):
        if 'log' not in dirpath:
            continue
        for fn in filenames:
            if fn.endswith('-status.txt'):
                process_file(dirpath, fn, jobs)

def main():
    usage()
    jobs = {}
    for dir in sys.argv[1:]:
        get_files(dir, jobs)
    print JobTimes.header()
    for job in sorted(jobs.iterkeys()):
        print jobs[job]

main()