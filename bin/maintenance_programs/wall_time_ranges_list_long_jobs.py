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

# Given a directory containing civet pipeline runs, report
# the max, min and average walltime for each cluster job
# in the pipeline.

from __future__ import print_function

import sys
import os
import re
import argparse



class JobTimes(object):
    def __init__(self, job):
        self.job = job
        self.max = -1
        self.min = 999999999
        self.total = 0
        self.count = 0
        self.requested = 0
        self.max_requested = 0

    def register_time(self, used_timestr, requested_timestr, min_walltime):
        secs = JobTimes.to_seconds(used_timestr)
        req_secs = JobTimes.to_seconds(requested_timestr)
        if req_secs > self.max_requested:
            self.max_requested = req_secs
            self.req = requested_timestr.split('=')[1]
        if secs < self.min:
            self.min = secs
        if secs > self.max:
            self.max = secs
        self.total += secs
        self.count += 1
        
        # Was it longer enough to report?
        return (secs >= min_walltime, secs)

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
            return '{0}:{1:02d}:{2:02d}:{3:02d}'.format(days, hours,
                                                   minutes, seconds)
        else:
            return '{0:02d}:{1:02d}:{2:02d}'.format(hours, minutes,
                                                 seconds)


def process_file(dir, fn, jobs, longjobs, min_walltime):
    path = os.path.join(dir, fn)
    for line in open(path):
        if 'requested_walltime' in line:
            req = line.strip()
        elif 'walltime' in line:
            used = line.strip()
    
    job = re.sub('(.*)-status.txt', r'\1', fn)
    if job not in jobs:
        jobs[job] = JobTimes(job)
    lj = jobs[job].register_time(used, req, min_walltime)
    if lj[0]:
        longjobs.append('{0}\t{1}\t{2}'.
                        format(dir, job, JobTimes.from_seconds(lj[1])))


def get_files(start_dir, jobs, longjobs, min_walltime):
    for (dirpath, dirnames, filenames) in os.walk(start_dir):
        if 'log' not in dirpath:
            continue
        for fn in filenames:
            if fn.endswith('-status.txt'):
                process_file(dirpath, fn, jobs, longjobs, min_walltime)


def main():

    parser = argparse.ArgumentParser(description="Find long running jobs in a Civet run")

    parser.add_argument('--walltime', '-w', dest='walltime', action='store',
                        type=float, default=24,
                        help='Minimum walltime to report as a long running job (unit=hours, type=float, default=24)')
    parser.add_argument('directory', help="Path to Civet log directories", nargs='+')

    args = parser.parse_args(sys.argv[1:])

    walltime_sec = args.walltime * 24 * 60

    jobs = {}
    longjobs = []
    for dir in args.directory:
        get_files(dir, jobs, longjobs, walltime_sec)
    print(JobTimes.header())
    for job in sorted(jobs.iterkeys()):
        print(jobs[job])
    if longjobs:
        print('\nLong running jobs (> {} hours)'.format(args.walltime))
        for lj in longjobs:
            print(lj)

main()
