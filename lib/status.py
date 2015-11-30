#!/usr/bin/env python

import sys
import os

import job_runner.torque as batch_system
import job_runner.common
import glob

class JobStatus(object):

    def __init__(self, log_dir, name, id, job_manager):

        self.state = None
        self.exit_status = None
        self.walltime = None
        self.walltime_requested = None

        if os.path.exists(os.path.join(log_dir, name + job_runner.common.JOB_STATUS_SUFFIX)):
            status = job_runner.common.get_status_from_file(log_dir, name)

            if 'canceled' in status or 'cancelled' in status:
                self.state = "CANCELED"

            # put elif in here to catch jobs that were running at cancel time,
            # but were unable to see cancel.log and update their status appropriately.

            elif 'exit_status' in status:
                if status['exit_status'] == '0':
                    self.state = "COMPLETED"
                elif status['exit_status'] == '-11':
                    self.state = "FAILED (WALLTIME)"
                else:
                    self.state = "FAILED"

            if 'walltime' in status:
                self.walltime = status['walltime']
            if 'requested_walltime' in status:
                self.walltime_requested = status['requested_walltime']



        else:
            status = job_manager.query_job(id)

            if status:
                if status.state == 'R':
                    self.state = "RUNNING"
                elif status.state == 'H':
                    self.state = "HELD"
                elif status.state == 'W':
                    self.state = "WAITING"
                elif status.state == 'Q':
                    self.state = "QUEUED"
                elif status.state == 'C':
                    # as of Civet 1.7.0 this shouldn't happen, even for failed
                    # jobs. Let's just assume something bad happened even if the
                    # exit status is 0
                    self.state = "FAILED"
            else:
                # no information for job, as of Civet 1.7.0 this should only be
                # possible if the job is deleted (cancelled because of failed
                # dependency or qdel'd) prior to running or if the node it is
                # running on crashes.  As of Civet 1.7.0 all jobs that enter the
                # R state should produce a -status.txt file
                self.state = "DELETED"




class PipelineStatus(object):

    def __init__(self, log_dir):
        self.log_dir = log_dir
        self.job_stat = {}
        self.aborted = False

        self.complete_jobs_success = 0
        self.complete_jobs_failure = 0
        self.canceled_jobs = 0
        self.running_jobs = 0
        self.held_jobs = 0
        self.delayed_jobs = 0
        self.queued_jobs = 0
        self.deleted_jobs = 0

        try:
            batch_jobs = job_runner.common.jobs_from_logdir(log_dir)
        except IOError, e:
            raise ValueError("ERROR: {0} does not appear to be a valid pipeline log directory.\n".format(log_dir))

        jm = job_runner.torque.JobManager()

        self.status = "UNKNOWN"

        # check to see if the log directory was created with civet_run --no-submit
        if os.path.exists(os.path.join(log_dir, job_runner.common.NO_SUB_FLAG)):
            self.status = "NO_SUB"
            return


        # this works for older versions of Civet before the abort.log filename
        # was changed to include the name of the job that called abort_pipeline
        if os.path.exists(os.path.join(log_dir, "abort.log")):
            self.aborted = True

        # for newer versions of Civet:
        elif glob.glob(os.path.join(log_dir, "*-abort.log")):
            self.aborted = True

        self.total_jobs = len(batch_jobs)

        for job in batch_jobs:
            self.job_stat[job[1]] = JobStatus(log_dir, job[1], job[0], jm)
            state = self.job_stat[job[1]].state

            if state == "RUNNING":
                self.running_jobs += 1
            elif state == "HELD":
                self.held_jobs += 1
            elif state == "WAITING":
                self.delayed_jobs += 1
            elif state == "QUEUED":
                self.queued_jobs += 1
            elif "FAILED" in state:
                self.complete_jobs_failure += 1
            elif state == "COMPLETED":
                self.complete_jobs_success += 1
            elif state == "DELETED":
                self.deleted_jobs += 1
            elif state == "CANCELED":
                self.canceled_jobs += 1

        if self.canceled_jobs:
            self.status = "CANCELED"

        elif self.deleted_jobs:
            self.status = "TERMINATED"

        elif self.complete_jobs_success == self.total_jobs:
            self.status = "COMPLETE"

        elif self.complete_jobs_failure == 0:
            if self.running_jobs:
                self.status = "RUNNING"
            elif self.delayed_jobs:
                self.status = "WAITING"
            else:
                self.status = "QUEUED"

        else:
            self.status = "FAILED"




def main():
    """
    simple test driver for this module
    """

    if len(sys.argv) != 2:
        sys.stderr.write("usage:  {}  <log_directory>".format(sys.argv[0]))

    log_dir = sys.argv[1]

    status = PipelineStatus(log_dir)

    print status.status

    for name, status in status.job_stat.iteritems():
        print(name)
        print(status.walltime)

if __name__ == '__main__':
    main()