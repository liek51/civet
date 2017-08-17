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

# Standard imports
from __future__ import print_function

import sys
import os
import re
import tempfile
import xml.etree.ElementTree as ET

# pipeline components
from job_runner.batch_job import *
import pipeline_parse as PL
import civet_exceptions
from pipeline_file import *
import config


class ToolExecModes(object):
    BATCH_STANDARD = 1
    BATCH_MANAGED = 2
    CLOUD = 3


class Tool(object):
    # This script parses all of a tool definition.  Tools may be invoked
    # by the pipeline.
    # Tools are in separate files, so that we can substitute alternate
    # tools to perform the step.  Also, this allows a tool definition
    # to be part of multiple pipelines.
    # The only access a tool has to the pipeline's files is through
    # the ins and outs file id lists.
    # 
    # Each tool definition will create a temporary script which will be
    # submitted to the cluster as a single job.  This is performed in the
    # job manager component.
    #
    validTags = [
        'command',
        'description',
        'dir',
        'option',
        'file',
        'validate',
        'module',
        'string',
        ]

    validAtts = [
        'error_strings',
        'name',
        'threads',
        'tool_config_prefix',
        'walltime',
        'mem',
        'exit_if_exists',
        'exit_test_logic',
        'path',
        ]

    def __init__(self, xml_file, ins, outs, pipeline_files, name=None, walltime=None, tool_config_prefix=None):
        # Don't understand why this has to be here as well to get some
        # symbols. But it seems to be needed.
        import pipeline_parse as PL

        self.options = {}
        self.commands = []
        self.tempfile_ids = []
        self.ins = ins
        self.outs = outs
        self.skip_validation = PL.skip_validation
        self.option_overrides = {}
        self.thread_option_max = 0
        self.modules = []
        self.name_from_pipeline = name

        self.verify_files = []
        self.tool_files = {}
        self.pipeline_files = pipeline_files

        self.docker_image = None

        # check the search path for the XML file, otherwise fall back to
        # the same directory as the pipeline XML.  CLIA pipelines do not pass
        # in a search path, so the tool XML needs to be in the same directory
        # as the pipeline XML
        self.xml_file = self.search_for_xml(xml_file)
        if not self.xml_file:
            msg = "Could not find tool XML file: {0}".format(xml_file)
            raise civet_exceptions.ParseError(msg)

        bad_inputs = []
        bad_outputs = []

        # ADD PIPELINE_ROOT to tool_files
        self.tool_files['PIPELINE_ROOT'] = pipeline_files['PIPELINE_ROOT']

        for n in range(len(ins)):
            try:
                f = pipeline_files[ins[n]]
                self.tool_files['in_' + str(n+1)] = f
            except KeyError as e:
                bad_inputs.append(ins[n])

        for n in range(len(outs)):
            try:
                f = pipeline_files[outs[n]]
                self.tool_files['out_' + str(n+1)] = f
            except KeyError as e:
                bad_outputs.append(outs[n])

        msg = ["{}: Tool input error".format(self.name_from_pipeline)]
        for f in bad_inputs:
            msg.append("\tunknown file ID: {}".format(f))
        for f in bad_outputs:
            msg.append("\tunknown file ID: {}".format(f))

        if bad_inputs or bad_outputs:
            raise civet_exceptions.ParseError("\n".join(msg))




        # Verify that the tool definition file has not changed.
        self.verify_files.append(os.path.abspath(self.xml_file))

        try:
            tool = ET.parse(self.xml_file).getroot()
        except ET.ParseError as e:
            raise civet_exceptions.ParseError("XML ParseError when parsing {}: {}".format(xml_file, e))

        atts = tool.attrib

        # Validate the attributes
        for a in atts:
            if a not in Tool.validAtts:
                msg = ("Unknown attribute in tool '{}': {}\n"
                       "Valid Attributes: '{}'".format(self.name_from_pipeline,
                                                       a,
                                                       ", ".join(Tool.validAtts)))
                raise civet_exceptions.ParseError(msg)

        # The name attribute is required.  All others are optional.
        try:
            self.name = atts['name'].replace(' ', '_')
        except KeyError:
            raise civet_exceptions.ParseError("'{}' is mising required attribute 'name'".format(os.path.basename(self.xml_file)))

        if 'error_strings' in atts:
            self.error_strings = []
            # The error strings are a comma-sep list of strings
            # to search for.  Spaces have to be quoted or escaped.
            estrings = atts['error_strings'].split(',')
            for es in estrings:
                self.error_strings.append(es.strip())
        else:
            self.error_strings = None

        if tool_config_prefix:
            self.config_prefix = tool_config_prefix
        elif 'tool_config_prefix' in atts:
            self.config_prefix = atts['tool_config_prefix']
        else:
            self.config_prefix = None

        if self.config_prefix in PL.option_overrides:
            self.option_overrides = PL.option_overrides[self.config_prefix]

        if 'threads' in atts:
            try:
                self.default_threads = int(atts['threads'])
            except ValueError:
                msg = "{}: tool threads attribute must be an integer. Value was '{}'".format(os.path.basename(self.xml_file), atts['threads'])
                raise civet_exceptions.ParseError(msg)
        else:
            self.default_threads = 1

        if 'walltime' in self.option_overrides:
            # walltime set in an option override file takes the highest priority
            self.walltime = self.option_overrides['walltime'][0].replace('"', '')
        elif walltime:
            # did we pass a walltime as a parameter? this would be set in the
            # <tool> tag in the pipeline and should have priority over the
            # walltime in the tool XML file
            self.walltime = walltime
        elif 'walltime' in atts:
            # walltime set in the tool's XML file
            self.walltime = atts['walltime']
        else:
            # no walltime set anywhere.  Use a Civet default walltime.
            self.walltime = BatchJob.DEFAULT_WALLTIME
            
        if 'exit_if_exists' in atts and not PL.force_conditional_steps:
            # this is going to have to be fixed later, since it may contain
            # files that need to be expanded to a real path
            self.exit_if_exists = atts['exit_if_exists']
        else:
            self.exit_if_exists = None
            
        if 'exit_test_logic' in atts:
            #if this is invalid, then BatchJob __init__() will throw a ValueError
            #should be "and" or "or" (case insensitive)
            self.exit_test_logic = atts['exit_test_logic']
            if self.exit_test_logic.upper() not in ['AND', 'OR']:
                msg = "'{}': exit_test_logic attribute must be 'AND' or 'OR' (case insensitive). Value was: {}".format(os.path.basename(self.xml_file), self.exit_test_logic)
                raise civet_exceptions.ParseError(msg)
        else:
            self.exit_test_logic = "AND"  #default to AND
            
        if 'mem' in atts:
            self.mem = atts['mem']
            if not self.mem.isdigit() and self.mem <= 0:
                msg = "'{}': mem attribute must be a positive integer: {}".format(os.path.basename(self.xml_file), self.mem)
                raise civet_exceptions.ParseError(msg)
        else:
            self.mem = None

        if 'path' in atts:
            path_dirs = []
            file_dir = os.path.dirname(os.path.abspath(self.xml_file))
            for d in atts['path'].split(':'):
                if os.path.isabs(d):
                    path_dirs.append(d)
                else:
                    path_dirs.append(os.path.join(file_dir, d))
            self.path = ':'.join(path_dirs)
        else:
            self.path = None
            


        # We can't process any non-file tags until all our files
        # are processed and fixed up.  Rather than force an order
        # in the user's file, we simply stash the other tags in
        # a "pending tags" list.
        pending = []

        # Now process our child tags
        for child in tool:
            t = child.tag
            if t not in Tool.validTags:
                msg = "'{}': Unknown tag {}\n\n{}".format(os.path.basename(self.xml_file), t, ET.tostring(child))
                raise civet_exceptions.ParseError(msg)

            if t == 'file' or t == 'dir' or t == 'string':
                # Register the file in the tool's file dictionary
                self.file(child)
            else:
                pending.append(child)

        # Now we can fix up our files.
        try:
            PipelineFile.finalize_file_paths(self.tool_files)
        except civet_exceptions.ParseError as e:
            # finalize_file_paths can throw a civet_exceptions.ParseError, however
            # it doesn't know what file it is in at the time,  so we catch it
            # here, add the filename to the message, and raise an exception
            msg = "{}:  {}".format(os.path.basename(self.xml_file), e)
            raise civet_exceptions.ParseError(msg)
        
        sumarize_files(self.tool_files, self.name_from_pipeline)

        # Now we can process self.exit_if_exists
        if self.exit_if_exists:
            files_to_test = []
            for f in self.exit_if_exists.split(","):
                f = f.strip()
                if f not in self.tool_files:
                    raise civet_exceptions.ParseError("unkown file ID in exit_if_exists attribute: {}".format(f))
                files_to_test.append(self.tool_files[f].path)
            self.exit_if_exists = files_to_test  
                    

        # Now, finally, we can process the rest of the tags.
        for child in pending:
            t = child.tag
            if t == 'description':
                # This one is so simple we process it inline here, instead of 
                # having a different class to process it.
                self.description = child.text
            elif t == 'option':
                Option(child, self)
            elif t == 'command':
                Command(child, self)
            elif t == 'module':
                self.modules.append(child.text)
            elif t == 'validate':
                a = child.attrib
                if 'id' in a:
                    try:
                        name = self.tool_files[a['id']].path
                    except KeyError:
                        msg = "{}: Unknown file ID '{}'\n\n{}".format(os.path.basename(self.xml_file), a['id'], ET.tostring(child))
                        raise civet_exceptions.ParseError(msg)
                    # If not key error; let the exception escape.
                else:
                    name = child.text
                self.verify_files.append(name)
            else:
                print('Unprocessed tag:' + t, file=sys.stderr)

        # Do we need to adjust the walltime?
        if PL.walltime_multiplier > 0 and PL.walltime_multiplier != 1:
            self.walltime = BatchJob.adjust_walltime(self.walltime, PL.walltime_multiplier)

    def search_for_xml(self, xml_file):
        # get current pipeline symbols
        import pipeline_parse as PL

        #is the path absolute?
        if os.path.isabs(xml_file):
            if os.path.exists(xml_file):
                return xml_file
            else:
                return None
    
        # search PL.user_search_path
        for path in ':'.join([PL.user_search_path, PL.default_tool_search_path]).split(':'):
            if path and os.path.exists(os.path.join(path, xml_file)):
                return os.path.join(path, xml_file)

        # didn't find it.  Check PL.master_XML_dir
        if os.path.exists(os.path.join(PL.master_XML_dir, xml_file)):
            return os.path.join(PL.master_XML_dir, xml_file)
        
        # not in search path or pipeline directory
        return None
    
    def file(self, e):
        atts = e.attrib

        id = atts['id']
        # Ensure that the id is unique.
        if id in self.options:
            raise civet_exceptions.ParseError("{}: file id duplicates an option"
                                              "name: ".format(os.path.basename(self.xml_file), self.id))
        if id in self.tool_files:
            raise civet_exceptions.ParseError("{}: file id is a duplicate: {}".format(os.path.basename(self.xml_file), self.id))
        

        PipelineFile.parse_xml(e, self.tool_files)

        # Track all the tool temporary files, so that we can
        # delete them at the end of the tool's execution.
        if self.tool_files[id].is_temp:
            self.tempfile_ids.append(id)

    def collect_files_to_validate(self):
        v = self.verify_files
        for c in self.commands:
            p = c.program
            if p:
                v.append(p)
        return v

    def collect_version_commands(self):
        vcs = []
        for c in self.commands:
            if c.real_version_command:
                vc = c.real_version_command
                if vc not in vcs:
                    vcs.append(vc)
        return vcs

    def _build_multi_command(self, rm_tmp=True):

        multi_command_list = [c.real_command for c in self.commands]

        # Tack on a final command to delete our temp files.
        if rm_tmp and self.tempfile_ids:
            # Convert from file ids to paths.
            for n in range(len(self.tempfile_ids)):
                self.tempfile_ids[n] = (
                    self.tool_files[self.tempfile_ids[n]].path)

            # Use rm -f because if a command is executed conditionally
            # due to if_exists and if_not_exists, a temp file may not
            # exist.  Without -f the rm command would fail, causing
            # the entire pipeline to fail. We also need -r to take care of
            # <dir> tags declared temp
            rm_cmd = 'rm -rf ' + ' '.join(self.tempfile_ids)
            multi_command_list.append(rm_cmd)

        return '  && \\\n'.join(multi_command_list)

    def _build_veryify_file_list(self):
        verify_file_list = None

        if not self.skip_validation:
            verify_file_list = self.verify_files
            #do we need to load a Python modulefile?
            need_python = True
            for m in self.modules:
                if m.startswith('python'):
                    need_python = False
            if need_python:
                if config.civet_job_python_module:
                    self.modules.append(config.civet_job_python_module)
                verify_file_list.append('python')

        return verify_file_list

    def submit(self, name):
        """
        Submit the commands that comprise the tool as a single cluster job.


        :param name:  a unique (to the pipeline) job name.
        :return: job_id: a value which can be passed in as a depends_on list
                element in a subsequent tool submission.
        """

        # Get the current symbols in the pipeline...
        import pipeline_parse as PL


        # 
        # Now it is time to fix up the commands and write the script file.
        # We couldn't do this before, because we have to ensure that ALL 
        # pipeline XML processing is done. (Tempfiles need an output dir,
        # and might have been specified before the output dir.)
        # Tempfiles specified in the pipeline have already been fixed up 
        # and have paths.  Here in the tool, they appear as normal files.
        # This is different from tempfiles specified in the tool; they
        # really are temp, and can be stored locally on the node and cleaned
        # up on tool exit.
        #
        for c in self.commands:
            c.fixupOptionsFiles()
            # Add the command names to the verify_files list
            p = c.program
            if p and (p not in self.verify_files):
                self.verify_files.append(c.program)

        multi_command = self._build_multi_command()

        # Determine what jobs we depend on based on our input files.
        depends_on = []
        for fid in self.ins:
            f = self.pipeline_files[fid]
            if f.creator_job:
                j = f.creator_job
                if j not in depends_on:
                    depends_on.append(j)
            if f.is_list:
                if f.foreach_dep:
                    depends_on.append(PL.foreach_barriers[f.foreach_dep])

        # Do the actual batch job submission
        submit_threads = self.thread_option_max if self.thread_option_max else self.default_threads
        verify_file_list = self._build_veryify_file_list()

        if PL.delay:
            date_time = PL.delay_timestamp
        else:
            date_time = None

        batch_job = BatchJob(multi_command,
                             workdir=PipelineFile.get_output_dir(),
                             files_to_check=verify_file_list,
                             ppn=submit_threads, walltime=self.walltime,
                             modules=self.modules, depends_on=depends_on,
                             name=name, error_strings=self.error_strings,
                             version_cmds=self.collect_version_commands(),
                             files_to_test=self.exit_if_exists,
                             file_test_logic=self.exit_test_logic, mem=self.mem,
                             date_time=date_time,
                             email_list=PL.error_email_address,
                             info=("Tool Definition File: " +
                                   os.path.abspath(self.xml_file)),
                             tool_path=self.path)
    
        try:
            job_id = PL.job_runner.queue_job(batch_job)
        except Exception as e:
            PL.abort_submit(e, PL.BATCH_ERROR)


        # Any files that we created and that will be passed to other jobs
        # need to be marked with our job id.  It is OK if we overwrite
        # a previous job.
        for fid in self.outs:
            f = self.pipeline_files[fid]
            f.set_creator_job(job_id)

        # Mark the files we depend on so that they're not cleaned up too 
        # early.  Really only needs to be done for temp files, but for
        # simplicity, we mark them all.
        for fid in self.ins:
            f = self.pipeline_files[fid]
            f.add_consumer_job(job_id)

        print("{0}: {1}".format(job_id, self.name_from_pipeline))
        return job_id

    def check_files_exist(self):
        missing = []
        for fid in self.tool_files:
            f = self.tool_files[fid]
            if f.is_input:
                if f.is_list:
                    for lf in f.path.split(','):
                        if not os.path.exists(lf):
                            missing.append(lf)
                elif not os.path.exists(f.path):
                    missing.append(f.path)
        return missing

    def create_task(self, task_name, execution_mode):

        # Get the current symbols in the pipeline...
        import pipeline_parse as PL

        task = {}

        for c in self.commands:
            c.fixupOptionsFiles()

        # build the command to run the tool
        multi_command = self._build_multi_command()

        task['name'] = task_name
        task['command'] = multi_command
        task['mem'] = self.mem
        task['walltime'] = self.walltime
        task['threads'] = self.thread_option_max if self.thread_option_max else self.default_threads

        if execution_mode == ToolExecModes.CLOUD:
            task['docker_image'] = self.docker_image
            task['input_files'] = []
            task['output_files'] = []

            task['dependencies'] = []
            for input_name in self.ins:
                inf = self.pipeline_files[input_name]
                if inf.creator_job and inf.creator_job not in task['dependencies']:
                    task['dependencies'].append(inf.creator_job)

                task['input_files'].append({'id': input_name, 'local': inf.path,
                                            'cloud': inf.cloud_path})

            for output_name in self.outs:
                outf = self.pipeline_files[output_name]
                # Any files that we created and that will be passed to other jobs
                # need to be marked with our job id.  It is OK if we overwrite
                # a previous job.
                outf.set_creator_job(task_name)
                task['output_files'].append({'id': output_name, 'local': outf.path,
                                             'cloud': outf.cloud_path})

        elif execution_mode == ToolExecModes.BATCH_MANAGED:

            # Do the actual batch job submission
            submit_threads = task['threads']
            verify_file_list = self._build_veryify_file_list()

            task['dependencies'] = []
            for input_name in self.ins:
                inf = self.pipeline_files[input_name]
                if inf.creator_job and inf.creator_job not in task['dependencies']:
                    task['dependencies'].append(inf.creator_job)


            for output_name in self.outs:
                outf = self.pipeline_files[output_name]
                # Any files that we created and that will be passed to other jobs
                # need to be marked with our job id.  It is OK if we overwrite
                # a previous job.
                outf.set_creator_job(task_name)

            batch_job = BatchJob(multi_command,
                             workdir=PipelineFile.get_output_dir(),
                             files_to_check=verify_file_list,
                             ppn=submit_threads, walltime=self.walltime,
                             modules=self.modules,
                             name=task_name, error_strings=self.error_strings,
                             version_cmds=self.collect_version_commands(),
                             files_to_test=self.exit_if_exists,
                             file_test_logic=self.exit_test_logic, mem=self.mem,
                             email_list=PL.error_email_address,
                             info=("Tool Definition File: " +
                                   os.path.abspath(self.xml_file)),
                             tool_path=self.path,
                             stdout_path = os.path.join(PL.log_dir, task_name + ".o"),
                             stderr_path = os.path.join(PL.log_dir, task_name + ".e"))


            task['script_path'] = PL.job_runner.write_script(batch_job)
            task['stdout_path'] = batch_job.stdout_path
            task['stderr_path'] = batch_job.stderr_path
            task['epilogue_path'] = PL.job_runner.epilogue_filename
            task['batch_env'] = PL.job_runner.generate_env(batch_job)
            task['email_list'] = PL.error_email_address
            task['mail_options'] = batch_job.mail_option
            task['module_files'] = self.modules
            task['log_dir'] = PL.job_runner.execution_log_dir
            task['queue'] = PL.job_runner.queue

        return task


class Option(object):
    def __init__(self, e, tool):
        self.command_text = ''
        self.value = ''
        self.binary = False

        name = e.attrib['name'].strip()
        self.name = name
        if 'command_text' in e.attrib:
            self.command_text = e.attrib['command_text'].strip()
        if 'value' in e.attrib:
            if name in tool.option_overrides:
                value = tool.option_overrides[name][0]
            else:
                value = e.attrib['value'].strip()
        elif 'from_file' in e.attrib:
            fid = e.attrib['from_file']
            try:
                fn = tool.tool_files[fid].path
            except KeyError:
                msg = "{}: Unknown file ID '{}' in option 'from_file' attribute:\n\n{}".format(os.path.basename(tool.xml_file), fid, ET.tostring(e))
                raise civet_exceptions.ParseError(msg)
            value = '$(cat ' + fn + ') '
        elif 'threads' in e.attrib and e.attrib['threads'].upper() == 'TRUE':
            if name in tool.option_overrides:
                try:
                    value = int(tool.option_overrides[name][0])
                except ValueError:
                    msg = "{}: Invalid value for option override '{}' (must be integer): {}".format(os.path.basename(tool.xml_file), name, tool.option_overrides[name][0])
                    raise civet_exceptions.ParseError(msg)
            else:
                value = tool.default_threads

            if value > tool.thread_option_max:
                tool.thread_option_max = value

            # now that we've made sure it is an integer and we've set
            # thread_option_max we need to turn it back into a string for
            # substitution in the command line
            value = str(value)

        if 'binary' in e.attrib:
            self.binary = True

            if value.upper() == 'TRUE' or value == '1':
                value = True
            elif value.upper() == 'FALSE' or value == '0':
                value = False
            else:
                msg = "{}: invalid value '{}' for binary option, must be 'True' or 'False'\n\n{}".format(os.path.basename(tool.xml_file), value, ET.tostring(e))
                raise civet_exceptions.ParseError(msg)


        self.isFile = False
        self.value = value

        # We don't allow the same option name in a tool twice
        if self.name in tool.options:
            msg = "{}: Option {} is a duplicate".format(os.path.basename(tool.xml_file), self.name)
            raise civet_exceptions.ParseError(msg)
        if self.name in tool.tool_files:
            msg = "{}: Option {} is a duplicate of a file ID".format(os.path.basename(tool.xml_file), self.name)
            raise civet_exceptions.ParseError(msg)
        
        # some attributes are mutually exclusive
        if 'binary' in e.attrib and 'from_file' in e.attrib:
            msg = ("{}: Option {}: binary and from_file attributes are mutually"
                   " exclusive\n\n{}".format(os.path.basename(tool.xml_file),
                                             self.name, ET.tostring(e)))
            raise civet_exceptions.ParseError(msg)
        if 'binary' in e.attrib and 'threads' in e.attrib:
            msg = ("{}: Option {}: binary and threads attributes are mutually"
                   " exclusive\n\n{}".format(os.path.basename(tool.xml_file),
                                             self.name, ET.tostring(e)))
            raise civet_exceptions.ParseError(msg)
        if 'value' in e.attrib and 'from_file' in e.attrib:
            msg = ("{}: Option {}: value and from_file attributes are mutually"
                   " exclusive\n\n{}".format(os.path.basename(tool.xml_file),
                                             self.name, ET.tostring(e)))
            raise civet_exceptions.ParseError(msg)
        if 'value' in e.attrib and 'threads' in e.attrib:
            msg = ("{}: Option {}: value and threads attributes are mutually"
                   " exclusive\n\n{}".format(os.path.basename(tool.xml_file),
                                             self.name, ET.tostring(e)))
            raise civet_exceptions.ParseError(msg)
        if 'from_file' in e.attrib and 'threads' in e.attrib:
            msg = ("{}: Option {}: from_file and threads attributes are mutually"
                   " exclusive\n\n{}".format(os.path.basename(tool.xml_file),
                                             self.name, ET.tostring(e)))
            raise civet_exceptions.ParseError(msg)
        tool.options[name] = self

    def __repr__(self):
        return ' '.join(['Option:', 'n', self.name, 'c', self.command_text, 'v', self.value])

    def __str__(self):
        return self.__repr__()


class Command(object):
    validAtts = [
        'delimiters',
        'program',
        'stderr_id',
        'stdout_id',
        'if_exists',
        'if_not_exists',
        'if_exists_logic'
        ]

    def __init__(self, e, tool):
        # Stash the options and tool_files dictionaries.  We'll need
        # them to fix up the command lines.
        # tool is a reference to the tool object that will contain
        # this command.
        self.tool = tool
        self.options = tool.options
        self.tool_files = tool.tool_files
        self.version_command = None
        self.real_version_command = None
        self.if_exists_files = []
        self.if_not_exists_files = []
        
        # get current pipeline symbols
        import pipeline_parse as PL

        atts = e.attrib
        for a in atts:
            if a not in Command.validAtts:
                msg = "{}: Unknown attribute in command tag: {}\n\n{}".format(os.path.basename(tool.xml_file), a, ET.tostring(e))
                raise civet_exceptions.ParseError(msg)
        # The program attribute is required.  The remainder are optional.
        try:
            self.program = atts['program']
        except KeyError:
            msg = "{}: program attribute is required for <command> tag.\n\n{}"
            raise civet_exceptions.ParseError(msg.format(tool.xml_file, ET.tostring(e)))

        # Delimiters are optional (and unusual!)
        if 'delimiters' in atts:
            self.delims = atts['delimiters']
            if len(self.delims) != 2:
                msg = "{}: command tag delimiters must be exactly two characters.\n\n{}".format(os.path.basename(tool.xml_file), ET.tostring(e))
                raise civet_exceptions.ParseError(msg)
        else:
            self.delims = '{}'
        delim_1 = self.delims[0]
        delim_2 = self.delims[1]
        if delim_1 in '|':
            delim_1 = '\\' + delim_1
        if delim_2 in '|':
            delim_2 = '\\' + delim_2
        self.replacePattern = re.compile(delim_1 + '(.*?)' + delim_2)

        # Capture desired output redirection
        if 'stdout_id' in atts:
            self.stdout_id = atts['stdout_id']
        else:
            self.stdout_id = None
        if 'stderr_id' in atts:
            self.stderr_id = atts['stderr_id']
        else:
            self.stderr_id = None
            
        if 'if_exists' in atts and not PL.force_conditional_steps:
            for f in atts['if_exists'].split(','):
                f = f.strip()
                if f not in self.tool_files:
                    msg = "{}: unknown file ID in command 'if_exists' attribute: {}\n\n{}".format(os.path.basename(tool.xml_file), f, ET.tostring(e))
                    raise civet_exceptions.ParseError(msg)
                self.if_exists_files.append(self.tool_files[f].path)
                
        if 'if_not_exists' in atts and not PL.force_conditional_steps:
            for f in atts['if_not_exists'].split(','):
                f = f.strip()
                if f not in self.tool_files:
                    msg = "{}: unknown file ID in command 'if_not_exists' attribute: {}\n\n{}".format(os.path.basename(tool.xml_file), f, ET.tostring(e))
                    raise civet_exceptions.ParseError(msg)
                self.if_not_exists_files.append(self.tool_files[f].path)
                
        if 'if_exists_logic' in atts:
            logic_type = atts['if_exists_logic'].strip().upper()
            if logic_type not in ['AND', 'OR']:
                msg = "{}: value of 'if_exists_logic' must be 'AND' or 'OR'\n\n{}".format(os.path.basename(tool.xml_file), ET.tostring(e))
                raise civet_exceptions.ParseError(msg)
            self.if_exists_logic = logic_type
        else:
            self.if_exists_logic = 'AND'

        # The command text can be either in the command element's text,
        # or as the "tail" of the child <version_command> tag. Sigh.
        # Therefore we'll process it in parts.
        if e.text:
            command_text = e.text
        else:
            command_text = ""

        # Only allow one child in a Command tag
        child_found = False
        for child in e:
            if child_found:
                msg = "{}: only one subtag allowed in command tag:\n\n{}".format(os.path.basename(tool.xml_file), ET.tostring(e))
                raise civet_exceptions.ParseError(msg)
            child_found = True
            t = child.tag

            if t != 'version_command':
                msg = "{}: unknown child tag '{}' in command:\n\n{}".format(os.path.basename(tool.xml_file), t, ET.tostring(e))
                raise civet_exceptions.ParseError(msg)
            self.version_command = re.sub('\s+', ' ', child.text).strip()

            # Get any command text that the parser considers part of this
            # child.
            if child.tail:
                command_text += child.tail

            

        # Strip out excess white space in the command
        if command_text:
            self.command_template = re.sub('\s+', ' ', command_text).strip()
        else:
            self.command_template = ''

        tool.commands.append(self)

    def fixupOptionsFiles(self):
        # The token replacement function is an inner function, so that
        # it has access to the self attributes.
        def tokenReplace(m):
            # get current pipeline symbols
            import pipeline_parse as PL
            tok = m.group(1)
            if tok in self.options:
                o = self.options[tok]
                if o.binary:
                    if o.value:
                        return o.command_text
                    else:
                        return ''
                elif len(o.command_text) > 0 and (o.command_text[-1] == '=' or o.command_text[-1] == ':'):
                    return o.command_text + o.value
                else:
                    return o.command_text + ' ' + o.value
            if tok in self.tool_files:
                f = self.tool_files[tok]
                if f.is_list:
                    if f.list_from_param:
                        return f.path.replace(',', ' ')
                    else:
                        # Emit the code to invoke a file filter.
                        return "$(process_filelist.py {0} '{1}')".format(f.in_dir, f.pattern)
                return f.path

            # We didn't match a known option, or a file id. Put out an error.
            print("\n\nUNKNOWN OPTION OR FILE ID: {} in file {}".format(tok, self.tool.xml_file), file=sys.stderr)
            print('Tool files: {}'.format(self.tool_files), file=sys.stderr)
            print('Options: {}\n\n'.format(self.options), file=sys.stderr)
            PL.abort_submit('UNKNOWN OPTION OR FILE ID: ' + tok)

        # Fix up a command by replacing all the delimited option names and
        # file ids with the real option text and file paths.
        self.real_command = (self.program + ' ' +
                             self.replacePattern.sub(tokenReplace,
                                                     self.command_template))
                                                     

        # Similarly, fix up a version_command by replacing all the delimited 
        # option names and file ids with the real option text and file paths.
        if self.version_command:
            self.real_version_command = (self.replacePattern.sub(tokenReplace,
                                         self.version_command))

        # Set up to capture output and error redirection, if requested.
        if self.stdout_id:
            self.real_command += ' > ' + self.tool_files[self.stdout_id].path
        if self.stderr_id:
            self.real_command += ' 2> ' + self.tool_files[self.stderr_id].path
            
        #need to wrap the command in logic to process "if_exits" or 
        #"if_not_exits" tests
        tests = []
        if self.if_exists_files:
            for f in self.if_exists_files:
                tests.append(' -e "{0}" '.format(f))
        if self.if_not_exists_files:
            for f in self.if_not_exists_files:
                tests.append(' ! -e "{0}" '.format(f))
        if tests:
            # the value of the if_exists_logic was validated above to
            # be either AND or OR (case insensitive, converted to upper)
            if self.if_exists_logic == 'AND':
                file_test_operator = ' && '
            else:
                file_test_operator = ' || '

        
            self.real_command = "if [[ {1} ]]; then {0}; fi".format(self.real_command, file_test_operator.join(tests))
