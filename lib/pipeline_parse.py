#! /bin/env python

#
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
#

# pipeline_parse.py
# Process the XML file describing the overall pipeline.

import sys
import datetime
import os
import getpass
import xml.etree.ElementTree as ET

from foreach import *
from pipeline_file import *
from step import *
from tool import *
from tool_logger import *

from job_runner.torque import *
from job_runner.batch_job import *
import utilities
import civet_exceptions
import config


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
        'version'
    ]

    valid_attributes = [
        'tool_search_path',
        'path'
    ]

    valid_version_attributes = [
        'directory'
    ]

    valid_directory_versions = [1, 2]

    def __init__(self):
        pass

    def parse_XML(self, xmlfile, params, skip_validation=False, queue=None, 
                  submit_jobs=True, completion_mail=True, search_path="",
                  user_override_file=None, keep_temp=False, release_jobs=True,
                  force_conditional_steps=False, delay=None, email_address=None,
                  error_email_address=None, walltime_multiplier=1):
        pipe = ET.parse(xmlfile).getroot()

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
        
        override_file = os.path.splitext(xmlfile)[0] + '.options'
        if os.path.exists(override_file):
            self.parse_override_file(override_file, "pipeline")
            
        if user_override_file and os.path.exists(user_override_file):
            self.parse_override_file(user_override_file, "user")
            
           
        # Register the parameters that may be file paths
        PipelineFile.register_params(params)
        
        # The outermost tag must be pipeline; it must have a name
        # and must not have text
        if pipe.tag != "pipeline":
            raise civet_exceptions.ParseError("Outermost tag of pipeline definition must be <pipeline></pipeline>")
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
        self.directory_version = 1

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

        
        # And track the major components of the pipeline
        self._steps = []
        self._files = {}
        self.foreach_barriers = {}

        # Walk the child tags.
        for child in pipe:
            t = child.tag
            if t not in Pipeline.valid_tags:
                msg = "{}: Illegal tag: {}".format(os.path.basename(self.xmlfile), t)
                raise civet_exceptions.ParseError(msg)
            if t == 'step' or t == 'foreach':
                pending.append(child)
            elif t == 'version':
                self.parse_version_tag(child)
            else:
                # <file> <dir> <filelist> and <string> are all handled by PipelineFile
                PipelineFile.parse_XML(child, self._files)
        
        # Here we have finished parsing the files in the pipeline XML.
        # Time to fix up various aspects of files that need to have
        # all files done first.

        try:
            PipelineFile.fix_up_files(self._files)
        except civet_exceptions.ParseError as e:
            # fix_up_files can throw a civet_exceptions.ParseError, however
            # it doesn't know what file it is in at the time,  so we catch it
            # here, add the filename to the message, and raise an exception
            msg = "{}:  {}".format(os.path.basename(self.xmlfile), e)
            raise civet_exceptions.ParseError(msg)

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
            if self.directory_version == 1:
                self._log_dir = os.path.join(PipelineFile.get_output_dir(),
                      'logs', datetime.datetime.now().strftime('%Y%m%d_%H%M%S'))
            elif self.directory_version == 2:
                self._log_dir = os.path.join(PipelineFile.get_output_dir(), 'logs')
            utilities.make_sure_path_exists(self._log_dir)
        return self._log_dir

    def submit(self):
        """
        Submit a constructed pipeline to the batch system for execution
        :return:
        """
        print 'Executing pipeline', self.name

        # Capture the CWD and the command line that invoked us.
        of = open(os.path.join(self.log_dir, 'command_line.txt'), 'w')
        of.write('User:\n')
        of.write(getpass.getuser() + '\n\n')
        of.write('Working directory at time of pipeline submission:\n')
        of.write(os.getcwd() + '\n\n')
        of.write('Command line used to invoke the pipeline:\n')
        of.write(' '.join(sys.argv) + '\n\n')
        of.write('Parameters to parse_XML:\n')
        of.write('  xmlfile: {0}\n'.format(self.xmlfile))
        of.write('  params: {0}\n'.format(self.params))
        of.write('  skip_validation: {0}\n'.format(
            self.skip_validation))
        of.write('  queue: {0}\n'.format(self.queue))
        of.write('  submit_jobs: {0}\n'.format(self.submit_jobs))
        of.write('  completion_mail: {0}\n'.format(
            self.completion_mail))
        of.write('  search_path: {0}\n'.format(self.search_path))
        of.write('  user_override_file: {0}\n'.format(
            self.user_override_file))
        of.write('  keep_temp: {0}\n'.format(self.keep_temp))
        of.write('  release_jobs: {0}\n'.format(self.release_jobs))
        of.write('  force_conditional_steps: {0}\n'.format(
            self.force_conditional_steps))
        of.write('  delay: {0}\n'.format(self.delay))
        of.write('  email_address: {0}\n'.format(self.email_address))
        of.write('  error_email_address: {0}\n'.format(
            self.error_email_address))
        of.write('  walltime_multiplier: {0}\n'.format(
            self.walltime_multiplier))

        of.close()
        
        #capture the overrides loaded into a log file:
        of = open(os.path.join(self.log_dir, 'option_overrides.txt'),
                  'w')
        for prefix, overrides in self.option_overrides.iteritems():
            for opt, (val,source) in overrides.iteritems():
                of.write("{0}.{1}={2}  #{3}\n".format(prefix, opt,
                                                      val, source))
        of.close()

        # Most of the dependencies are file-based; a job can run
        # as soon as the files it needs are ready.  However, we
        # have a final bookkeeping job that consolidates the log
        # files, etc.  That one needs to run last.  So we track 
        # all the batch job ids that are related to this pipeline.
        self.all_batch_jobs = []
        
        # Check that all files marked "input" exist.
        missing = self.check_files_exist()
        if missing:
            print >> sys.stderr, ('The following required files are '
                                  'missing:')
            print >> sys.stderr, '    ' + '\n    '.join(missing)
            sys.exit(1)

        invocation = 0
        for step in self._steps:
            invocation += 1
            name_prefix = '{0}_{1}{2}'.format(self.name, step.code, invocation)
            job_id = step.submit(name_prefix)
            for j in job_id:
                self.all_batch_jobs.append(j)

        # Submit two last bookkeepingjobs

        # 1. deletes all the temp files.
        if not self.keep_temp:
            tmps = []
            # This job is about deleting files...
            # For each temp file, depend on the job(s) that use it in any
            # way, either creating it or consuming it.
            # We can't just wait for the last job to complete, because it is 
            # possible to construct pipelines where the last submitted job
            # completes before an earlier submitted job.
            depends = []
            for fid in self._files:
                f = self._files[fid]
                if f.is_temp:
                    tmps.append(f.path)
                    if f.consumer_jobs:
                        for j in f.consumer_jobs:
                            if j not in depends:
                                depends.append(j)
                    if f.creator_job:
                        if f.creator_job not in depends:
                            depends.append(f.creator_job)
            # Use rm -f because if a command is executed conditionally
            # due to if_exists and if_not_exists, a temp file may not
            # exist.  Without -f the rm command would fail, causing
            # the entire pipeline to fail.
            # must be recursive because some temp files are actually directories
            cmd = 'rm -rf ' + ' '.join(tmps)
            if len(tmps):
                batch_job = BatchJob(cmd, workdir=PipelineFile.get_output_dir(),
                                     depends_on=depends,
                                     name='Remove_temp_files',
                                     email_list=self.error_email_address)
                try:
                    job_id = self.job_runner.queue_job(batch_job)
                except Exception as e:
                    sys.stderr.write(str(e) + '\n')
                    sys.exit(self.BATCH_ERROR)
                self.all_batch_jobs.append(job_id)

        # 2. Consolidate all the log files.
        cmd = []
        cmd.append('consolidate_logs.py {0}'.format(self._log_dir))
        cmd.append('CONSOLIDATE_STATUS=$?')
        
        # 3. And (finally) send completion email
        if self.completion_mail:
            cmd.append("echo 'The pipeline running in:\n    " +
                       PipelineFile.get_output_dir() +
                       "\nhas completed.'" +
                       " | mailx -s 'Pipeline completed' " + self.email_address)
            # Mask any potential mail failures.
            cmd.append("true")
        cmd.append('bash -c "exit ${CONSOLIDATE_STATUS}"')
        cmd = '\n'.join(cmd)

        # do we need to load a modulefile to execute the Python consolidate log
        # script ?
        if config.civet_job_python_module:
            mod_files = [config.civet_job_python_module]
        else:
            mod_files = []

        batch_job = BatchJob(cmd, workdir=PipelineFile.get_output_dir(),
                             depends_on=self.all_batch_jobs, 
                             name="Consolidate_log_files",
                             modules=mod_files, mail_option='a',
                             email_list=self.error_email_address,
                             walltime="00:10:00")
        try:
            self.job_runner.queue_job(batch_job)
        except Exception as e:
                sys.stderr.write(str(e) + '\n')
                sys.exit(self.BATCH_ERROR)

        # We're done submitting all the jobs.  Release them (if necessary) and 
        # get on with it. This is the last action of the pipeline
        # submission process. WE'RE DONE!
        if self.release_jobs:
            self.job_runner.release_all()

        # Let the people know where they can see their logs.
        print 'Log directory: ', self.log_dir

    def abort_submit(self, message, status=1):
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

        sys.stderr.write("Aborting pipeline submission\n"
                         "\t{0}\n".format(message))
        sys.exit(status)



    @property
    def job_runner(self):
        # The pipeline will use a single Torque job runner.
        if not self._job_runner:
            self._job_runner = TorqueJobRunner(self.log_dir,
                                               validation_cmd="validate -m "
                                               + self.validation_file,
                                               pipeline_bin=os.path.abspath(os.path.join(self.master_XML_dir, "bin")),
                                               queue=self.queue, submit=self.submit_jobs,
                                               pipeline_path=self.path)
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
                if len(line) == 0 or line[0] == '#':
                    continue
                line = line.split(' #')[0]
                prefix = line.split('.', 1)[0]
                opt, val = line.split('.', 1)[1].split('=')
                if prefix not in self.option_overrides:
                    self.option_overrides[prefix] = {}
                self.option_overrides[prefix][opt.strip()] = (val.strip(), source)


    def parse_version_tag(self, tag):
        for attr in tag.attrib:
            if attr not in self.valid_version_attributes:
                msg = "{}: Illegal version attribute '{}'\n\n{}".format(os.path.basename(self.xmlfile), attr, ET.tostring(tag))
                raise civet_exceptions.ParseError(msg)
            if attr == 'directory':
                version = int(tag.attrib[attr])
                if version not in self.valid_directory_versions:
                    msg = "{}: Invalid directory version '{}'\n\n{}".format(os.path.basename(self.xmlfile), version, ET.tostring(tag))
                    raise civet_exceptions.ParseError(msg)
                self.directory_version = version


sys.modules[__name__] = Pipeline()
