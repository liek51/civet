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

import os
import sys
import inspect

# add some magic so when we run this without PYTHONPATH it can find the project
# lib dir (this usually happens when testing without using a Civet modulefile)
cmd_folder = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile(inspect.currentframe()))[0]))
lib_folder = os.path.join(cmd_folder, '../')
if lib_folder not in sys.path:
    sys.path.insert(0, lib_folder)

from job_runner.torque import *
from job_runner.batch_job import *


def submit_manager_job(task_database, manager_log_dir):
    # TODO implement me
    pass