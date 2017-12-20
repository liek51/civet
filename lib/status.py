#!/usr/bin/env python

# Copyright 2016 The Jackson Laboratory
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function

import sys
import os
import glob
import json

import job_runner.torque as batch_system
import job_runner.common
from exec_modes import ToolExecModes


def format_state(state):
    if state == 'R':
        return "Running"
    elif state == 'Q':
        return "Queued (eligible to run)"
    elif state == 'H':
        return "Queued (waiting on dependency)"
    elif state == 'W':
        return "Queued (with delayed start)"

    return state


class ManagedJobStatus(object):
    """
    This is the class used to obtain stripped down information about a job
    submitted when running in the managed batch mode.  All the job manager cares
    about a submitted job is if it is "Submitted" (queued but not complete),
    "Failed" (complete, with non-zero exit status), "Complete" (complete, zero
    exit status), and "Deleted" (no record of submitted job).
    """
    def __init__(self, log_dir, name, batch_id, job_manager):

        if os.path.exists(os.path.join(log_dir, name + job_runner.common.JOB_STATUS_SUFFIX)):
            status = job_runner.common.get_status_from_file(log_dir, name)

            # it's possible for there be an empty or incomplete -status.txt
            # file if the compute node crashed with the job running
            # this will be the state if we can't determine otherwise
            self.state = "Deleted"

            if 'canceled' in status or 'cancelled' in status:
                self.state = "Deleted"
            elif 'exit_status' in status:
                if status['exit_status'] == '0':
                    self.state = "Complete"
                else:
                    self.state = "Failed"

        else:
            status = job_manager.query_job(str(batch_id))

            if status:
                if status.state == 'C':

                    if status.exit_status == '0':
                        # as of Civet 1.7.0 this shouldn't happen, even for
                        # failed jobs. If this happens, then the job completed
                        # without the job epilogue script running
                        self.state = "Complete"
                    else:
                        self.state = "Failed"
                else:
                    if status.state in ['Q', 'H', 'W', 'R']:
                        self.state = "Submitted"

            else:
                # no information for job, it wasn't in the queue and there is
                # no status file, but civet_managed_batch_master things it is
                # running. This should only happen if the node crashed or
                # someone qdel'd the job while it was still queued
                self.state = "Deleted"


class Status(object):

    def __init__(self, log_dir, name, id, deps, job_manager, running_at_cancel,
                 excution_mode):

        self.state = None
        self.exit_status = None
        self.walltime = None
        self.walltime_requested = None
        self.dependencies = deps
        self.id = id
        self.name = name

        if os.path.exists(os.path.join(log_dir, name + job_runner.common.JOB_STATUS_SUFFIX)):
            status = job_runner.common.get_status_from_file(log_dir, name)

            # with old versions of Civet, it's possible for there it be an empty
            # or incomplete -status.txt file if the job was canceled/qdel'd
            # this will be the state if we can't determine otherwise
            self.state = "DELETED"

            if 'canceled' in status or 'cancelled' in status:
                self.state = "CANCELED"
                self.state_at_cancel = format_state(status['state_at_cancel'])

            elif id in running_at_cancel:
                self.state = "CANCELED"
                self.state_at_cancel = "Running"

            elif 'exit_status' in status:
                if status['exit_status'] == '0':
                    self.state = "SUCCESS"
                elif status['exit_status'] == '-11':
                    self.state = "FAILED (WALLTIME)"
                elif status['exit_status'] == '271':
                    # running torque jobs canceled with qdel have exit status 271
                    self.state = "CANCELED"
                    self.state_at_cancel = "Running"
                else:
                    self.state = "FAILED"

            self.exit_status = status.get('exit_status', None)
            if self.exit_status:
                # exit status is a string pulled from a file, turn it into an integer
                self.exit_status = int(self.exit_status)

            if 'walltime' in status:
                self.walltime = status['walltime']
            if 'requested_walltime' in status:
                self.walltime_requested = status['requested_walltime']

        elif excution_mode == ToolExecModes.BATCH_MANAGED:
            # if the pipeline is being run in managed mode, there will be no
            # status information for unfinished jobs
            self.state = "MANAGED"

        else:
            status = job_manager.query_job(id)

            if status:
                if status.state == 'C':

                    if status.exit_status == '0':
                        # as of Civet 1.7.0 this shouldn't happen, even for
                        # failed jobs. If this happens, then the job completed
                        # without the job epilogue script running
                        self.state = "EXIT_NO_EPILOGUE"
                    elif status.exit_status is None:
                        self.state = "DELETED"
                    else:
                        self.state = "FAILED"
                else:
                    if status.state == 'Q':
                        self.state = "QUEUED"
                    elif status.state == 'H':
                        self.state = "HELD"
                    elif status.state == 'W':
                        self.state = "WAITING"
                    else:
                        self.state = "RUNNING"

                self.exit_status = status.exit_status
                if self.exit_status is not None:
                    self.exit_status = int(self.exit_status)
            else:
                # no information for job, as of Civet 1.7.0 this should only be
                # possible if the job is deleted (cancelled because of failed
                # dependency or qdel'd) prior to running or if the node it is
                # running on crashes.  As of Civet 1.7.0 all jobs that enter the
                # R state should produce a -status.txt file
                self.state = "DELETED"

    def __str__(self):
        return "{}, {}, {}, {}".format(self.state, self.exit_status,
                                       self.walltime, self.walltime_requested)

    def to_json_serializable(self):
       return self.__dict__


class PipelineStatus(object):

    def __init__(self, log_dir, job_manager=batch_system.JobManager()):
        self.log_dir = log_dir
        self.jobs = []
        self.aborted = False

        self.complete_jobs_success = 0
        self.complete_jobs_failure = 0
        self.canceled_jobs = 0
        self.running_jobs = 0
        self.held_jobs = 0
        self.delayed_jobs = 0
        self.queued_jobs = 0
        self.deleted_jobs = 0
        self.managed_unknown = 0
        self.cancel_message = None
        self.jobs_running_at_cancel = []
        self.execution_mode = None

        try:
            batch_jobs = job_runner.common.jobs_from_logdir(log_dir)
        except IOError, e:
            raise ValueError("ERROR: {0} does not appear to be a valid pipeline log directory.\n".format(log_dir))

        # check to see if the log directory was created with civet_run --no-submit
        if os.path.exists(os.path.join(log_dir, job_runner.common.NO_SUB_FLAG)):
            self.status = "NO_SUB"
            return

        if os.path.exists(os.path.join(log_dir,
                                       job_runner.common.MANAGED_MODE_FLAG)):
            self.execution_mode = ToolExecModes.BATCH_MANAGED
        else:
            self.execution_mode = ToolExecModes.BATCH_STANDARD

        jm = job_manager

        self.status = "UNKNOWN"

        # this works for older versions of Civet before the abort.log filename
        # was changed to include the name of the job that called abort_pipeline
        if os.path.exists(os.path.join(log_dir, "abort.log")):
            self.aborted = True

        # for newer versions of Civet:
        elif glob.glob(os.path.join(log_dir, "*-abort.log")):
            self.aborted = True

        self.total_jobs = len(batch_jobs)

        self.canceled = False
        if os.path.exists(os.path.join(log_dir, job_runner.common.CANCEL_LOG_FILENAME)):
            self.canceled = True
            cancel_info = dict(line.strip().split('=') for line in open(os.path.join(log_dir, job_runner.common.CANCEL_LOG_FILENAME)))
            if 'DATESTAMP' in cancel_info:
                self.cancel_message = "PIPELINE WAS CANCELED by user at {}\n".format(cancel_info['DATESTAMP'])
            else:
                self.cancel_message = "PIPELINE WAS CANCELED by user.\n"
            self.jobs_running_at_cancel = cancel_info.get('RUNNING_JOBS', [])

        for job in batch_jobs:
            job_status = Status(log_dir, job[1], job[0], job[2], jm,
                                self.jobs_running_at_cancel, self.execution_mode)
            self.jobs.append(job_status)

            if job_status.state == "RUNNING":
                self.running_jobs += 1
            elif job_status.state == "HELD":
                self.held_jobs += 1
            elif job_status.state == "WAITING":
                self.delayed_jobs += 1
            elif job_status.state == "QUEUED":
                self.queued_jobs += 1
            elif "FAILED" in job_status.state:
                self.complete_jobs_failure += 1
            elif job_status.state == "SUCCESS":
                self.complete_jobs_success += 1
            elif job_status.state == "DELETED":
                self.deleted_jobs += 1
            elif job_status.state == "CANCELED":
                self.canceled_jobs += 1
            elif job_status.state == "MANAGED":
                self.managed_unknown += 1

        if self.total_jobs == 0:
            self.status = "SUBMIT_ERROR"

        elif self.complete_jobs_success == self.total_jobs:
            self.status = "COMPLETE"

        elif self.canceled:
            self.status = "CANCELED"

        elif self.complete_jobs_failure:
            self.status = "FAILED"

        elif self.deleted_jobs:
            self.status = "TERMINATED"

        elif self.complete_jobs_failure == 0:
            if self.running_jobs:
                self.status = "RUNNING"
            elif self.delayed_jobs:
                self.status = "WAITING"
            else:
                self.status = "QUEUED"

        else:
            self.status = "UNKNOWN"

    def __str__(self):
        return str(self.__dict__)

    def to_json_serializable(self):
        return {
            'log_dir': self.log_dir,
            'status': self.status,
            'jobs': [j.to_json_serializable() for j in self.jobs],
            'aborted': self.aborted,
            'complete_jobs_success': self.complete_jobs_success,
            'complete_jobs_failure': self.complete_jobs_failure,
            'canceled_jobs': self.canceled_jobs,
            'running_jobs': self.running_jobs,
            'held_jobs': self.held_jobs,
            'dealyed_jobs': self.delayed_jobs,
            'queued_jobs': self.queued_jobs,
            'deleted_jobs': self.deleted_jobs,
            'cancel_message': self.cancel_message,
            'jobs_running_at_cancel': self.jobs_running_at_cancel,
            'managed_pending_jobs': self.managed_unknown,
            'execution_mode': self.execution_mode
        }

    @staticmethod
    def get_job_manager():
        return batch_system.JobManager()

def main():
    """
    simple test driver for this module
    """

    if len(sys.argv) != 2:
        sys.stderr.write("usage:  {}  <log_directory>".format(sys.argv[0]))

    log_dir = sys.argv[1]

    status = PipelineStatus(log_dir)

    print(status.status)

    for job in status.jobs:
        print("{}:  {}".format(job.name, job))

if __name__ == '__main__':
    main()