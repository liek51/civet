#!/usr/bin/env python

# Copyright 2017 The Jackson Laboratory
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

import os
import inspect
import logging

from job_runner.torque import TorqueJobRunner as BatchRunner

import config

# we need to get the path to the civet_managed_batch_master executable, which
# may not be in the user's path if they are calling this script directly.
# since it is in the Civet bin directory, which is up two directories from
# this file in the Civet install, we can work out the path to the
# civet_managed_batch_master command.
# it's sort of ugly
__my_dir = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile(inspect.currentframe()))[0]))
__cmd_dir = os.path.join(__my_dir, '../../bin')


def submit_management_job(task_db, queue, walltime_hours, max_queued, log_level):

    manager_cmd = os.path.abspath(os.path.join(__cmd_dir, 'civet_managed_batch_master'))

    queue_string = '--queue {}'.format(queue) if queue else ''

    modules_commands = []
    if config.purge_user_modulefiles:
        modules_commands.append("module purge")

    task = {

        'cmd': "\n".join(modules_commands) + "\n" + config.civet_python +
               " " + manager_cmd +
               " {} --max-walltime {} --max-queued {} --log-level {} {}".format(
                   queue_string, walltime_hours, max_queued, log_level, task_db),
        'walltime': "{}:00:00".format(walltime_hours),
        'name': "civet_manager",
        'queue': queue

    }
    logging.debug("About to start manager job with: {}".format(task))
    return BatchRunner.submit_simple_job(task)


