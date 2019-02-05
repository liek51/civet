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

import os
import inspect

BATCH_ID_LOG = "pipeline_batch_id_list.txt"
JOB_STATUS_SUFFIX = "-status.txt"
CANCEL_LOG_FILENAME = "cancel.log"
NO_SUB_FLAG = "NO_SUBMIT"
MANAGED_MODE_FLAG = "MANAGED_BATCH"
GCP_MODE_FLAG = "CLOUD_GCP"

CIVET_HOME = os.path.normpath(os.path.join(os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0])), "../../"))


def get_status_from_file(logdir, job_name):
    try:
        return dict(line.strip().split('=') for line in open(os.path.join(logdir, job_name + JOB_STATUS_SUFFIX)))
    except ValueError:
        return None


def jobs_from_logdir(logdir):
    batch_jobs = []
    for line in open(os.path.join(logdir, BATCH_ID_LOG)):
        batch_jobs.append(line.strip().split('\t'))
        
    return batch_jobs
