"""
Copyright (C) 2016  The Jackson Laboratory

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import os
import inspect

BATCH_ID_LOG = "pipeline_batch_id_list.txt"
JOB_STATUS_SUFFIX = "-status.txt"
CANCEL_LOG_FILENAME = "cancel.log"
NO_SUB_FLAG = "NO_SUBMIT"

CIVET_HOME = os.path.normpath(os.path.join(os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0])), "../../"))


def get_status_from_file(logdir, job_name):
    return dict(line.strip().split('=') for line in open(os.path.join(logdir, job_name + JOB_STATUS_SUFFIX)))
    
def jobs_from_logdir(logdir):
    batch_jobs = []
    for line in open(os.path.join(logdir, BATCH_ID_LOG)):
        batch_jobs.append(line.strip().split('\t'))
        
    return batch_jobs
