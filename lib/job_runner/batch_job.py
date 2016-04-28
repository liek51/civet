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
import string
import re


class BatchJob(object):
    """
    a container for a batch job
    
    cmd     : a string containing the command (with arguments) to execute
    workdir : the job's working directory (default is current working directory)
    nodes   : number of nodes to request from resource manager
    ppn     : processors per node to request from resource manager
    walltime : walltime to request
    modules  : modules to load, pass a list to load multiple module files
    depends_on  : list of job IDs that this job has a dependency on
    name        : name for batch job
    stdout_path : path to final location for job's stdout spool (default = job runner default)
    stderr_path : path to final location for job's stderr spool (default = job runner default)
    files_to_check : files to validate before running command
    version_cmds : list of command lines to report version of the executables being used
    error_strings: A list of strings Civet will search for in the job's stderr.
                   If the string is found, the job is considered to have failed.
    mail_option  : parameter to pass to resource manager's mail options for the job
    email_list        :  email address to send resource manager notification emails
    files_to_test: list of file paths to check for before running command. if 
                   the file test returns true, the job will exit with success 
    file_test_logic : logic used to join file tests geneerated.  Can be "AND" or "OR"
    mem     : batch job mem attribute (in GB)
    date_time: datetime job will be eligible at this time (for delayed job)
    info: extra information recorded as comment in generated batch script
    """

    DEFAULT_WALLTIME = "01:00:00"

    def __init__(self, cmd, workdir=None, nodes=1, ppn=1,
                 walltime=DEFAULT_WALLTIME, modules=[], depends_on=[],
                 name=None, stdout_path=None, stderr_path=None,
                 files_to_check=None, version_cmds=None, error_strings=None,
                 mail_option="n", email_list=None, files_to_test=[],
                 file_test_logic="AND", mem=None, date_time=None, info=None,
                 tool_path=None):

        #initialize some of the hidden properties
        self._name = None
        self._workdir = None
        self._mail_option = None
        self._mem = None
        self._file_test_logic = None

        self.cmd = cmd
        self.ppn = ppn
        self.nodes = nodes
        self.modules = modules
        self.depends_on = depends_on
        self.stdout_path = stdout_path
        self.stderr_path = stderr_path
        self.mail_option = mail_option
        self.email_list = email_list
        self.workdir = workdir
        self.walltime = walltime
        self.name = name
        self.files_to_check = files_to_check
        self.version_cmds = version_cmds
        self.error_strings = error_strings
        self.files_to_test = files_to_test
        self.file_test_logic = file_test_logic
        self.mem = mem
        self.date_time = date_time
        self.info = info
        self.tool_path = tool_path

    @property
    def workdir(self):
        return self._workdir

    # setter for workdir, sets to the current working directory if a directory
    # is not passed
    @workdir.setter
    def workdir(self, d):
        if d:
            self._workdir = os.path.abspath(d)
        else:
            self._workdir = os.getcwd()

    @property
    def name(self):
        return self._name

    # setter for the job name, throw an exception if the name passed has invalid
    # characters
    @name.setter
    def name(self, name):
        if name:
            chars = []
            for c in name:
                chars.append(c)
                if c not in string.digits + string.letters + "_-.":
                    raise ValueError("Invalid job name: '{0}'. "
                                     "Illegal character {1} {2}".format(name, c,
                                                                        chars))
        self._name = name

    @property
    def mail_option(self):
        return self._mail_option

    @mail_option.setter
    def mail_option(self, val):
        if val and re.match(r'[^abe]', val) and val != 'n':
            raise ValueError("Invalid mail_option. Must be n|{abe}|None")
        self._mail_option = val

    @property
    def file_test_logic(self):
        return self._file_test_logic

    @file_test_logic.setter
    def file_test_logic(self, val):
        if not val or val.upper() not in ["AND", "OR"]:
            raise ValueError(
                'Invalid exit_test_bool option ({0}). Must be "AND" or "OR"'.format(val))
        self._file_test_logic = val.upper()

    @property
    def mem(self):
        return self._mem

    @mem.setter
    def mem(self, m):
        if m is not None:
            if not m.isdigit():
                raise ValueError(
                    'Invalid mem request.  Must be a positive integer.')

            # Civet requires tools request memory in gigabytes.  We add the 
            # correct Torque size suffix
            self._mem = m + 'gb'
        else:
            self._mem = None

    @staticmethod
    def adjust_walltime(walltime, multiplier):
        walltime_in_seconds = BatchJob.walltime_string_to_seconds(walltime)
        walltime_in_seconds *= multiplier
        return BatchJob.walltime_seconds_to_string(int(walltime_in_seconds))

    @staticmethod
    def walltime_string_to_seconds(walltime):

        seconds = 0
        parts = walltime.split(':')[::-1]
        num_parts = len(parts)

        if num_parts < 1:
            return None

        seconds += int(parts[0])
        if num_parts >= 2:
            seconds += int(parts[1])*60
        if num_parts >= 3:
            seconds += int(parts[2])*3600
        if num_parts == 4:
            seconds += float(parts[3])*24*3600

        return seconds

    @staticmethod
    def walltime_seconds_to_string(walltime):
        time = int(walltime)
        hours = time // 3600
        minutes = (time - hours * 3600) // 60
        seconds = (time - hours * 3600 - minutes * 60)

        return "{}:{:02}:{:02}".format(hours, minutes, seconds)

