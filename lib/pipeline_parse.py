#! /bin/env python

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

# pipeline_parse.py
# Process the XML file describing the overall pipeline.

from __future__ import print_function

import sys
import datetime
import os
import getpass
import xml.etree.ElementTree as ET
import json

from foreach import *
from pipeline_file import *
from step import *
from tool import *

from job_runner.torque import *
from job_runner.batch_job import *
import job_runner.common
import utilities
import civet_exceptions
import config
from exec_modes import ToolExecModes


class Pipeline(object):
    """
    A singleton module for information about the overall pipeline.
    
    For ease of access from other pipeline modules this class is inserted 
    in the Python modules chain using sys.modules. This technique was
    gleaned  from (URL on two lines... beware)

    http://stackoverflow.com/questions/880530/
    can-python-modules-have-properties-the-same-way-that-objects-can

    This is done at the end of the file, after the full definition
    of the class
    """
    
    BATCH_ERROR = 100
    
    valid_tags = [
        'file',
        'dir',
        'string',
        'foreach',
        'step',
        'filelist',
        'description'
    ]

    valid_attributes = [
        'tool_search_path',
        'path',
        'name',
        'display_name'
    ]

    def __init__(self):
        pass

    def parse_XML(self, xmlfile, params, skip_validation=False, queue=None,
                  submit_jobs=True, completion_mail=True, search_path="",
                  user_override_file=None, keep_temp=False, release_jobs=True,
                  force_conditional_steps=False, delay=None, email_address=None,
                  error_email_address=None, walltime_multiplier=1,
                  write_pipeline_files=False,
                  tool_exec_mode=ToolExecModes.BATCH_STANDARD,
                  error_email=True):

        try:
            self._parse_XML(xmlfile, params, skip_validation, queue, submit_jobs,
                            completion_mail, search_path, user_override_file,
                            keep_temp, release_jobs, force_conditional_steps,
                            delay, email_address, error_email_address,
                            walltime_multiplier, write_pipeline_files,
                            tool_exec_mode, error_email)
        except civet_exceptions.ParseError as e:
            print("\nError parsing XML:  {}".format(e), file=sys.stderr)
            sys.exit(1)
        except civet_exceptions.MissingFile as e:
            print(e, file=sys.stderr)
            sys.exit(1)

    def _parse_XML(self, xmlfile, params, skip_validation=False, queue=None,
                   submit_jobs=True, completion_mail=True, search_path="",
                   user_override_file=None, keep_temp=False, release_jobs=True,
                   force_conditional_steps=False, delay=None, email_address=None,
                   error_email_address=None, walltime_multiplier=1,
                   write_pipeline_files=False,
                   tool_exec_mode=ToolExecModes.BATCH_STANDARD,
                   error_email=True, job_name_prefix="CIVET__"):
        try:
            pipe = ET.parse(xmlfile).getroot()
        except ET.ParseError as e:
            raise civet_exceptions.ParseError("XML ParseError when parsing {}: {}".format(xmlfile, e))

        self.job_name_prefix = job_name_prefix if job_name_prefix is not None else ""

        # Register the directory of the master (pipeline) XML.
        # We'll use it to locate tool XML files.
        self.master_XML_dir = os.path.abspath(os.path.dirname(xmlfile))

        # Save some that we only need for reporting use, later.
        self.xmlfile = xmlfile
        self.params = params
        self.search_path = search_path
        self.user_override_file = user_override_file

        # search path for tool XML files
        self.user_search_path = search_path
        self.default_tool_search_path = ""
        
        # option overrides
        self.option_overrides = {}

        # used while generating a summary of all file ids and their final path
        # for logging
        self.file_summary = {}
        
        override_file = os.path.splitext(xmlfile)[0] + '.options'
        if os.path.exists(override_file):
            self.parse_override_file(override_file, "pipeline")
            
        if user_override_file and os.path.exists(user_override_file):
            self.parse_override_file(user_override_file, "user")

        self.execution_mode = tool_exec_mode

        # Register the parameters that may be file paths
        PipelineFile.register_params(params)
        
        # The outermost tag must be pipeline; it must have a name
        # and must not have text
        if pipe.tag != "pipeline":
            raise civet_exceptions.ParseError("Outermost tag of pipeline definition must be <pipeline></pipeline>")

        if 'name' not in pipe.attrib:
            raise civet_exceptions.ParseError("<pipeline> 'name' attribute is required")

        self.name = pipe.attrib['name']
        if pipe.text.strip():
            raise civet_exceptions.ParseError("<pipeline> tag may not contain text")

        # We need to process all our files before we process anything
        # else. Stash anything not a file and process it in a later pass.
        pending = []

        # Set up for some properties
        self._output_dir = None
        self._log_dir = None
        self._job_runner = None
        self.validation_file = os.path.splitext(xmlfile)[0] + '_validation.data'
        self.queue = queue
        self.submit_jobs = submit_jobs
        self.completion_mail = completion_mail
        self.error_email = error_email
        self.keep_temp = keep_temp
        self.release_jobs = release_jobs
        self.force_conditional_steps = force_conditional_steps
        self.skip_validation = skip_validation
        self.delay = delay
        self.walltime_multiplier = walltime_multiplier
        if email_address:
            self.email_address = os.path.expandvars(email_address)
        else:
            self.email_address = getpass.getuser()
        if error_email_address:
            self.error_email_address = os.path.expandvars(error_email_address)
        else:
            self.error_email_address = self.email_address

        if self.delay:
            try:
                hours, minutes = utilities.parse_delay_string(self.delay)
            except ValueError as e:
                message = "Error parsing delay parameter '{}'. {}".format(self.delay, e)
                sys.exit(message)
            self.delay_timestamp = datetime.datetime.now() + datetime.timedelta(hours=hours, minutes=minutes)

        if 'tool_search_path' in pipe.attrib:
            self.default_tool_search_path = pipe.attrib['tool_search_path']

        if 'path' in pipe.attrib:
            path_dirs = []
            file_dir = os.path.abspath(self.master_XML_dir)
            for d in pipe.attrib['path'].split(':'):
                if os.path.isabs(d):
                    path_dirs.append(d)
                else:
                    path_dirs.append(os.path.join(file_dir, d))
            self.path = ':'.join(path_dirs)
        else:
            self.path = None

        self.display_name = pipe.attrib.get('display_name', None)

        # And track the major components of the pipeline
        self.description = None
        self._steps = []
        self._files = {}
        self.foreach_barriers = {}
        self.foreach_tasks = {}

        # create some implicitly defined file IDs
        PipelineFile.add_simple_dir("PIPELINE_ROOT", self.master_XML_dir,
                                    self._files, input=True)


        # Walk the child tags.
        for child in pipe:
            t = child.tag
            if t not in Pipeline.valid_tags:
                msg = "{}: Illegal tag: {}".format(os.path.basename(self.xmlfile), t)
                raise civet_exceptions.ParseError(msg)

            if t == 'step' or t == 'foreach':
                pending.append(child)
            elif t == 'description':
                # currently only used by the Civet UI, ignored by the civet
                # framework, but we will make sure the tag only occurs once
                if self.description:
                    raise civet_exceptions.ParseError("a pipeline can only contain one <description> tag")
                else:
                    # since we aren't actually using the description, just
                    # set it to True for now
                    self.description = True
            else:
                # <file> <dir> <filelist> and <string> are all handled by PipelineFile
                PipelineFile.parse_xml(child, self._files)
        
        # Here we have finished parsing the files in the pipeline XML.
        # Time to fix up various aspects of files that need to have
        # all files done first.

        try:
            PipelineFile.finalize_file_paths(self._files)
        except civet_exceptions.ParseError as e:
            # add the xml file path to the exception message
            msg = "{}:  {}".format(os.path.basename(self.xmlfile), e)
            raise civet_exceptions.ParseError(msg)

        if write_pipeline_files:
            sumarize_files(self._files, 'pipeline_files')
            with open(os.path.join(self.log_dir, "pipeline_files.json"), 'w') as f:
                f.write(json.dumps(self.file_summary, indent=4, sort_keys=True))

        # Now that our files are all processed and fixed up, we can
        # process the rest of the XML involved with this pipeline.
        for child in pending:
            t = child.tag
            if t == 'step':
                self._steps.append(Step(child, self._files))
            elif t == 'foreach':
                self._steps.append(ForEach(child, self._files))

    @property
    def user_search_path(self):
        return self._user_search_path

    @user_search_path.setter
    def user_search_path(self, val):
        if val is None:
            self._user_search_path = ""
        else:
            self._user_search_path = val

    @property
    def default_tool_search_path(self):
        return self._default_tool_search_path

    @default_tool_search_path.setter
    def default_tool_search_path(self, val):
        if val is None:
            self._default_tool_search_path = ""
        else:
            paths = []
            for path in val.split(':'):
                paths.append(os.path.join(self.master_XML_dir, path))

            self._default_tool_search_path = ':'.join(paths)
    
    @property
    def log_dir(self):
        if not self._log_dir:
            self._log_dir = os.path.join(PipelineFile.get_output_dir(),
                  'logs', datetime.datetime.now().strftime('%Y%m%d_%H%M%S'))
            utilities.make_sure_path_exists(self._log_dir)
        return self._log_dir

    def write_command_info(self):
         # Capture the CWD and the command line that invoked us.
        with open(os.path.join(self.log_dir, 'command_line.txt'), 'w') as of:
            of.write('User:\n')
            of.write(getpass.getuser() + '\n\n')
            of.write('Working directory at time of pipeline submission:\n')
            of.write(os.getcwd() + '\n\n')
            of.write('Command line used to invoke the pipeline:\n')
            of.write(' '.join(sys.argv) + '\n\n')
            of.write('Execution mode: \n')
            of.write('  {}\n\n'.format(ToolExecModes.to_str(self.execution_mode)))
            of.write('Parameters to parse_XML:\n')
            of.write('  xmlfile: {0}\n'.format(self.xmlfile))
            of.write('  params: {0}\n'.format(self.params))
            of.write('  skip_validation: {0}\n'.format(
                self.skip_validation))
            of.write('  queue: {0}\n'.format(self.queue))
            if self.execution_mode == ToolExecModes.BATCH_STANDARD:
                of.write('  submit_jobs: {0}\n'.format(self.submit_jobs))
            of.write('  completion_mail: {0}\n'.format(
                self.completion_mail))
            of.write('  search_path: {0}\n'.format(self.search_path))
            of.write('  user_override_file: {0}\n'.format(
                self.user_override_file))
            of.write('  keep_temp: {0}\n'.format(self.keep_temp))
            if self.execution_mode == ToolExecModes.BATCH_STANDARD:
                of.write('  release_jobs: {0}\n'.format(self.release_jobs))
            of.write('  force_conditional_steps: {0}\n'.format(
                self.force_conditional_steps))
            if self.execution_mode == ToolExecModes.BATCH_STANDARD:
                of.write('  delay: {0}\n'.format(self.delay))
            of.write('  email_address: {0}\n'.format(self.email_address))
            of.write('  error_email_address: {0}\n'.format(
                self.error_email_address))
            of.write('  walltime_multiplier: {0}\n'.format(
                self.walltime_multiplier))

        #capture the overrides loaded into a log file:
        with open(os.path.join(self.log_dir, 'option_overrides.txt'), 'w') as of:
            for prefix, overrides in self.option_overrides.iteritems():
                for opt, (val,source) in overrides.iteritems():
                    of.write("{0}.{1}={2}  #{3}\n".format(prefix, opt,
                                                          val, source))

    def submit(self, silent=False):
        """
        Submit a constructed pipeline to the batch system for execution
        :return:
        """
        if not silent:
            print('Executing pipeline ' + self.name)

        self.write_command_info()

        # Most of the dependencies are file-based; a job can run
        # as soon as the files it needs are ready.  However, we
        # have a final bookkeeping job that consolidates the log
        # files, etc.  That one needs to run last.  So we track 
        # all the batch job ids that are related to this pipeline.
        self.all_batch_jobs = []

        # Check that all files marked "input" exist.
        missing = self.check_files_exist()
        if missing:
            print("The following required files are missing:\n    "
                  + "\n    ".join(missing), file=sys.stderr)
            sys.exit(1)

        invocation = 0
        for step in self._steps:
            invocation += 1
            name_prefix = '{}{}_{}{}'.format(self.job_name_prefix, self.name,
                                             step.code, invocation)
            job_id = step.submit(name_prefix, silent)
            for j in job_id:
                self.all_batch_jobs.append(j)

        # Submit last cleanup / bookkeeping job
        self.submit_cleanup_job()


        # We're done submitting all the jobs.  Release them (if necessary) and 
        # get on with it. This is the last action of the pipeline
        # submission process. WE'RE DONE!
        if self.release_jobs:
            self.job_runner.release_all()

        # Let the people know where they can see their logs.
        if not silent:
            print('Log directory:  ' + self.log_dir)

        return {
            'log_dir': self.log_dir,
            'output_dir': PipelineFile.get_output_dir(),
            'job_ids': self.all_batch_jobs
        }

    def abort_submit(self, message, status=1, json_output=False):
        """
        Abort pipeline submission.  Deletes any jobs already queued, prints error
        message, and exits.

        :param message: error string to be presented to user
        :param status: return value to use for program exit
        :return:
        """
        if self.all_batch_jobs:
            job_manager = JobManager()
            job_manager.delete_all_jobs(self.all_batch_jobs)

        sys.stderr.write("Aborting pipeline submission:"
                         "  {0}\n".format(message))

        if json_output:
            print(json.dumps({
                'log_dir': self.log_dir,
                'output_dir': PipelineFile.get_output_dir(),
                'job_ids': []
            }, indent=2, sort_keys=True))

        sys.exit(status)

    def create_task_list(self):
        invocation = 0
        all_tasks = []
        for step in self._steps:
            invocation += 1
            name_prefix = '{}{}_{}{}'.format(self.job_name_prefix, self.name,
                                             step.code, invocation)
            step_tasks = step.create_tasks(name_prefix, self.execution_mode)
            all_tasks.extend(step_tasks)

        all_tasks.append(self._create_cleanup_task(all_tasks))
        return all_tasks

    def prepare_managed_tasks(self):

        self.execution_mode = ToolExecModes.BATCH_MANAGED

        print('Preparing pipeline ' + self.name)

        self.write_command_info()
        self._write_managed_flag()

        tasks = self.create_task_list()

        # create the pipeline_batch_id_list.txt file (normally created when
        # submitting jobs -- this is needed by civet_status
        with open(os.path.join(self.log_dir, job_runner.common.BATCH_ID_LOG), mode='w') as job_list:
            idx = 0
            for task in tasks:
                job_list.write(task['name'] + ".managed_task" + '\t' + task['name'] + '\t[' + ", ".join(task['dependencies']) + ']\n')
                idx += 1

        return tasks

    def _write_managed_flag(self):
        open(os.path.join(self.log_dir, job_runner.common.MANAGED_MODE_FLAG),
             'w').close()

    @property
    def job_runner(self):
        # The pipeline will use a single Torque job runner.
        if not self._job_runner:
            self._job_runner = TorqueJobRunner(self.log_dir,
                                               validate=(not self.skip_validation),
                                               validation_file=self.validation_file,
                                               pipeline_bin=os.path.abspath(os.path.join(self.master_XML_dir, "bin")),
                                               queue=self.queue, submit=self.submit_jobs,
                                               pipeline_path=self.path,
                                               send_failure_email=self.error_email)
        return self._job_runner

    def collect_files_to_validate(self):
        fns = []
        for step in self._steps:
            sfns = step.collect_files_to_validate()
            for fn in sfns:
                if fn not in fns:
                    fns.append(fn)
        return fns

    def collect_version_commands(self):
        vcs = []
        for step in self._steps:
            svcs = step.collect_version_commands()
            for vc in svcs:
                if vc not in vcs:
                    vcs.append(vc)
        return vcs

    def check_files_exist(self):
        missing = []
        for step in self._steps:
            smissing = step.check_files_exist()
            for fn in smissing:
                if fn not in missing:
                    missing.append(fn)
        return missing
        
    def parse_override_file(self, file, source=""):
        with open(file) as f:
            for line in f:
                line = line.strip()

                # skip comment lines
                if len(line) == 0 or line[0] == '#':
                    continue

                # trim off in line comments
                # TODO swith to a regex that will match space or tabs before the #
                line = line.split(' #')[0]
                prefix = line.split('.', 1)[0]
                opt, val = line.split('.', 1)[1].split('=')
                opt = opt.strip()
                val = val.strip()
                if prefix not in self.option_overrides:
                    self.option_overrides[prefix] = {}
                if opt in self.option_overrides[prefix] and self.option_overrides[prefix][opt][1] == source:
                    print("\nWARNING: duplicate option in option file. {}.{}={} "
                          "overwrites previous value of {}\n".format(prefix, opt, val, self.option_overrides[prefix][opt][0]), file=sys.stderr)
                self.option_overrides[prefix][opt] = (val, source)

    def _create_cleanup_cmd(self):
        cmd = []
        # 1. deletes all the temp files.
        if not self.keep_temp:
            tmps = []
            # This job is about deleting files...
            # For each temp file, depend on the job(s) that use it in any
            # way, either creating it or consuming it.
            # We can't just wait for the last job to complete, because it is
            # possible to construct pipelines where the last submitted job
            # completes before an earlier submitted job.
            for fid in self._files:
                f = self._files[fid]
                if f.is_temp:
                    tmps.append(f.path)
            if len(tmps):
                # Use rm -f because if a command is executed conditionally
                # due to if_exists and if_not_exists, a temp file may not
                # exist.  Without -f the rm command would fail, causing
                # the entire pipeline to fail.
                # must be recursive because some temp files are actually
                # directories
                cmd.append('rm -rf ' + ' '.join(tmps))

        # 2. Consolidate all the log files.
        consolidate_script_path = os.path.join(common.CIVET_HOME,
                                               'bin/consolidate_logs.py')
        cmd.append('{} {} {}'.format(config.civet_python,
                                     consolidate_script_path,
                                     self._log_dir))
        cmd.append('CONSOLIDATE_STATUS=$?')

        # consolidate log job needs to run last -- make sure it depends on all
        # of the child nodes in the dependency graph

        # 3. And (finally) send completion email
        if self.completion_mail:
            cmd.append("echo 'The pipeline running in:\n    " +
                       PipelineFile.get_output_dir() +
                       "\nhas completed.'" +
                       " | mailx -s 'Pipeline completed' " + self.email_address)
        cmd.append('bash -c "exit ${CONSOLIDATE_STATUS}"')
        return '\n'.join(cmd)

    def submit_cleanup_job(self):

        cmd = self._create_cleanup_cmd()

        batch_job = BatchJob(cmd, workdir=PipelineFile.get_output_dir(),
                             depends_on=self.all_batch_jobs,
                             name="{}{}_rm_temps_consolidate_logs".format(
                                 self.job_name_prefix, self.name),
                             mail_option='a',
                             email_list=self.error_email_address,
                             walltime="00:10:00")
        try:
            self.job_runner.queue_job(batch_job)
        except Exception as e:
                sys.stderr.write(str(e) + '\n')
                sys.exit(self.BATCH_ERROR)

    def _create_cleanup_task(self, all_tasks):

        # Get the current symbols in the pipeline...
        import pipeline_parse as PL

        cmd = self._create_cleanup_cmd()

        task = {}
        task['name'] = "{}{}_rm_temps_consolidate_logs".format(
            self.job_name_prefix, self.name)
        task['command'] = cmd
        task['walltime'] = "00:10:00"
        task['mem'] = 1
        task['threads'] = 1

        task['dependencies'] = [t['name'] for t in all_tasks]

        batch_job = BatchJob(cmd,
                             workdir=PipelineFile.get_output_dir(),
                             walltime=task['walltime'],
                             name=task['name'],
                             email_list=PL.error_email_address,
                             stdout_path=os.path.join(PL.log_dir,
                                                      task['name'] + ".o"),
                             stderr_path=os.path.join(PL.log_dir,
                                                      task['name'] + ".e"))

        task['script_path'] = PL.job_runner.write_script(batch_job)
        task['stdout_path'] = batch_job.stdout_path
        task['stderr_path'] = batch_job.stderr_path
        task['epilogue_path'] = PL.job_runner.epilogue_filename
        task['batch_env'] = PL.job_runner.generate_env(
            PipelineFile.get_output_dir())
        task['email_list'] = PL.error_email_address
        task['mail_options'] = batch_job.mail_option
        task['queue'] = PL.job_runner.queue

        return task


sys.modules[__name__] = Pipeline()
