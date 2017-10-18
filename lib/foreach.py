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
import xml.etree.ElementTree as ET

# Pipeline components
from step import *
from pipeline_file import *
from job_runner.torque import *
from civet_exceptions import *
from exec_modes import ToolExecModes



class ForEach(object):
    validTags = [
        'file',
        'related',
        'step',
    ]

    valid_attributes = [
        'dir',
        'id'
    ]

    # to prevent a user from flooding the system we impose a limit on the
    # maximum number of jobs that can be created by one foreach instance
    # when we are executing in the standard batch mode. Does not currently apply
    # for managed or cloud run-times
    MAX_JOBS = 500

    def __init__(self, e, pipeline_files):
        self.file = None
        self.relatedFiles = {}
        self.steps = []
        self.pipelineFiles = pipeline_files
        self.code = "FE"
        att = e.attrib

        if 'id' not in att:
            msg = "The foreach tag must specify an id:\n\n{}".format(ET.tostring(e))
            raise ParseError(msg)

        if 'dir' not in att:
            msg = "The foreach tag must specify a dir attribute:\n\n{}".format(ET.tostring(e))
            raise ParseError(msg)

        if len(att) != 2:
            msg = ("foreach tag has unrecognized attribute:\n\n"
                   "{}\n\nValid Attributes: 'id, dir'".format(ET.tostring(e).rstrip()))
            raise ParseError(msg)

        self.id = att['id']
        self.dir = att['dir']

        for child in e:
            t = child.tag
            if t not in ForEach.validTags:
                msg = ("Unknown tag in foreach '{}': \n\n"
                       "{}\n\nValid Tags: '{}'".format(self.id,
                                                       ET.tostring(child).rstrip(),
                                                       ", ".join(ForEach.validTags)))
                raise ParseError(msg)
            if t == 'step':
                self.steps.append(child)
            elif t == 'file':
                if self.file:
                    msg = "The foreach tag must contain exactly one file tag:\n\n{}".format(ET.tostring(e))
                    raise ParseError(msg)
                self.file = ForEachFile(child, self.pipelineFiles)
            else:
                ForEachRelated(child, self.relatedFiles, pipeline_files)

        if not self.file:
            msg = "The foreach tag must contain a file tag:\n\n{}".format(ET.tostring(e))
            raise ParseError(msg)

        if self.file.id in self.relatedFiles:
            msg = ("A foreach file's id must not be the same as a related "
                   "file's id.\n"
                   "'{}' is used as a file and related file id in '{}'.\n\n{}")
            raise ParseError(msg.format(self.file.id, self.id, ET.tostring(e)))

    def _get_matched_files(self):
        matched_files = []
        all_files = os.listdir(self.pipelineFiles[self.dir].path)
        for fn in all_files:
            if self.file.pattern.match(fn):
                matched_files.append(fn)

        return matched_files

    def _add_related_files(self, filename):

        # cleanups are file ids we will clean up from the file namespace after
        # we process this foreach file.
        cleanups = []
        files_to_delete = []

        for related_id in self.relatedFiles:
            rel = self.relatedFiles[related_id]
            rfn = rel.pattern.sub(rel.replace, filename)
            if rel.is_input and not rel.indir:
                #if no dir we assume related input files are in the same
                #directory as the foreach file it is related to
                directory = self.dir
            elif rel.indir:
                directory = rel.indir
            else:
                #related file is an output file and indir not specified
                #write it to the default output directory
                directory = None
            #TODO see comments for PipelineFile above. this is wicked ugly
            PipelineFile(rel.id, rfn, self.pipelineFiles,  True, False,
                         rel.is_input, False, False, None, None, None,
                         None, None, None, directory, False, False, None)
            cleanups.append(rel.id)
            if rel.is_temp:
                files_to_delete.append(rel.id)

        # finalize all of the files we just added
        PipelineFile.finalize_file_paths(self.pipelineFiles)

        return cleanups, files_to_delete

    def submit(self, name_prefix):

        import pipeline_parse as PL

        # Get file list
        # for file in list
            # build related filenames
            # register file and related in pipeline files list
            # submit step(s)
            # clean up files from pipeline files list

        matched_files = self._get_matched_files()
        job_ids = []
        iteration = 0

        # figure out if this foreach loop will exceed the limit
        total_jobs = 0
        for s in self.steps:
            for child in s:
                if child.tag == 'tool':
                    total_jobs += 1
        total_jobs *= len(matched_files)

        if total_jobs > ForEach.MAX_JOBS:
            PL.abort_submit("error submitting foreach: {} jobs exceed limit "
                            "(max = {})\n".format(total_jobs, ForEach.MAX_JOBS))

        for fn in matched_files:
            iteration += 1
            cleanups = []
            files_to_delete = []
            iteration_ids = []
            #TODO this is impossible to make sense of, create a static method in
            #PipelineFile that only takes the id, path, file list, and directory
            PipelineFile(self.file.id, fn, self.pipelineFiles, True, False,
                         True, False, False, None, None, None, None,
                         None, None, self.dir, False, False, None)
            cleanups.append(self.file.id)

            for id in self.relatedFiles:
                rel = self.relatedFiles[id]
                rfn = rel.pattern.sub(rel.replace, fn)
                if rel.is_input and not rel.indir:
                    #if no dir we assume related input files are in the same
                    #directory as the foreach file it is related to
                    directory = self.dir
                elif rel.indir:
                    directory = rel.indir
                else:
                    #related file is an output file and indir not specified
                    #write it to the default output directory
                    directory = None
                #TODO see comments for PipelineFile above. this is wicked ugly
                PipelineFile(rel.id, rfn, self.pipelineFiles,  True, False,
                             rel.is_input, False, False, None, None, None,
                             None, None, None, directory, False, False, None)
                cleanups.append(rel.id)
                if rel.is_temp:
                    files_to_delete.append(rel.id)
            PipelineFile.finalize_file_paths(self.pipelineFiles)

            step_iteration = 0
            for s in self.steps:
                step_iteration += 1
                step = Step(s, self.pipelineFiles)
                prefix = "{}-{}_S{}".format(name_prefix, iteration, step_iteration)
                for jid in step.submit(prefix):
                    job_ids.append(jid)
                    iteration_ids.append(jid)

            #submit a job that deletes all of the temporary files
            tmps = []
            for id in files_to_delete:
                tmps.append(self.pipelineFiles[id].path)

            if len(tmps):
                cmd = 'rm -f ' + ' '.join(tmps)
                cleanup_job = BatchJob(cmd, workdir=PipelineFile.get_output_dir(),
                               depends_on=iteration_ids,
                               name="{}-{}_temp_file_cleanup".format(name_prefix, iteration),
                               walltime="00:30:00",
                               email_list=PL.error_email_address)

                try:
                    cleanup_job_id = PL.job_runner.queue_job(cleanup_job)
                except Exception as e:
                    PL.abort_submit(e, PL.BATCH_ERROR)

                PL.all_batch_jobs.append(cleanup_job_id)

            for jid in cleanups:
                del self.pipelineFiles[jid]



        #enqueue "barrier" job here
        barrier_job = BatchJob('echo "placeholder job used for synchronizing foreach jobs"',
                               workdir=PipelineFile.get_output_dir(),
                               depends_on=job_ids,
                               name="{0}_barrier".format(name_prefix),
                               walltime="00:02:00",
                               email_list=PL.error_email_address)
        try:
            job_id = PL.job_runner.queue_job(barrier_job)
        except Exception as e:
            sys.stderr.write(str(e) + '\n')
            sys.exit(PL.BATCH_ERROR)
        PL.all_batch_jobs.append(job_id)
        PL.foreach_barriers[self.id] = job_id
        return job_ids

    def check_files_exist(self):
        missing = []
        if not os.path.exists(self.pipelineFiles[self.dir].path):
            missing.append(self.pipelineFiles[self.dir].path)
        return missing

    def create_tasks(self, name_prefix, execution_mode):
        import pipeline_parse as PL

        matched_files = self._get_matched_files()
        iteration = 0
        tasks = []

        for fn in matched_files:
            iteration += 1
            cleanups = []
            files_to_delete = []
            iteration_tasks = []
            #TODO this is impossible to make sense of, create a static method in
            #PipelineFile that only takes the id, path, file list, and directory
            PipelineFile(self.file.id, fn, self.pipelineFiles, True, False,
                         True, False, False, None, None, None, None,
                         None, None, self.dir, False, False, None)
            cleanups.append(self.file.id)

            cleanups_new, files_to_delete_new = self._add_related_files(fn)
            cleanups.extend(cleanups_new)
            files_to_delete.extend(files_to_delete_new)

            step_iteration = 0
            for s in self.steps:
                step_iteration += 1
                step = Step(s, self.pipelineFiles)
                prefix = "{}-{}_S{}".format(name_prefix, iteration, step_iteration)
                for task in step.create_tasks(prefix, execution_mode):
                    tasks.append(task)
                    iteration_tasks.append(task['name'])

            #create a task that deletes all of the temporary files
            tmps = []
            for fid in files_to_delete:
                tmps.append(self.pipelineFiles[fid].path)

            if len(tmps):
                task = {}
                cmd = 'rm -f ' + ' '.join(tmps)

                task['name'] = "{}-{}_temp_file_cleanup".format(name_prefix, iteration)
                task['command'] = cmd
                task['mem'] = "1"
                task['walltime'] = "00:30:00"
                task['threads'] = 1

                if execution_mode == ToolExecModes.CLOUD:
                    # TODO
                    raise Exception("TODO Finish foreach for cloud")

                elif execution_mode == ToolExecModes.BATCH_MANAGED:

                    task['dependencies'] = iteration_tasks

                    # need to make a BatchJob object so we can generate a job
                    # script for this
                    batch_job = BatchJob(cmd,
                                     workdir=PipelineFile.get_output_dir(),
                                     ppn=task['threads'], walltime=task["walltime"],
                                     name=task["name"],
                                     mem=task["mem"],
                                     email_list=PL.error_email_address,
                                     stdout_path = os.path.join(PL.log_dir, task["name"] + ".o"),
                                     stderr_path = os.path.join(PL.log_dir, task["name"] + ".e"))

                    task['script_path'] = PL.job_runner.write_script(batch_job)
                    task['stdout_path'] = batch_job.stdout_path
                    task['stderr_path'] = batch_job.stderr_path
                    task['epilogue_path'] = PL.job_runner.epilogue_filename
                    task['batch_env'] = PL.job_runner.generate_env(PipelineFile.get_output_dir())
                    task['email_list'] = PL.error_email_address
                    task['mail_options'] = batch_job.mail_option
                    task['module_files'] = []
                    task['log_dir'] = PL.job_runner.execution_log_dir
                    task['queue'] = PL.job_runner.queue

                tasks.append(task)

            # need to get rid of all the Files created for this foreach iteration
            for fid in cleanups:
                del self.pipelineFiles[fid]

        PL.foreach_tasks[self.id] = [task['name'] for task in tasks]
        return tasks

        
class ForEachFile(object):
    requiredAtts = [
        'id',
        'pattern' ]

    def __init__(self, e, pipeline_files):
        atts = e.attrib
        for a in atts:
            if a not in ForEachFile.requiredAtts:
                msg = ("Unknown attribute in foreach file: {}\n\n"
                       "{}\n\nValid Attributes: '{}'".format(a, ET.tostring(e).rstrip(),
                                                             ", ".join(ForEachFile.requiredAtts)))
                raise ParseError(msg)
        for a in ForEachFile.requiredAtts:
            if a not in atts:
                msg = ("foreach file tag missing required attribute:\n\n{}\n\n"
                       "Required Attributes: '{}'".format(ET.tostring(e).rstrip(),
                                                          ", ".join(ForEachFile.requiredAtts)))
                raise ParseError(msg)
        self.id = atts['id']
        if self.id in pipeline_files:
            msg = "a foreach file's id must not be the same as a pipeline file id: {}\n\n{}".format(self.id, ET.tostring(e))
            raise ParseError(msg)
        self.pattern = re.compile(atts['pattern'])
        

class ForEachRelated(object):
    requiredAtts = [
        'id',
        'input',
        'pattern',
        'replace']
    optionalAtts = [
        'in_dir',
        'temp'
        ]

    def __init__(self, e, related_files, pipeline_files):
        atts = e.attrib
        for a in atts:
            if a not in ForEachRelated.requiredAtts and a not in ForEachRelated.optionalAtts:
                msg = ("foreach related tag has unrecognized attribute: {}\n\n"
                       "{}\n\nValid Attributes: '{}'".format(a, ET.tostring(e).rstrip(),
                                                             ", ".join(ForEachRelated.requiredAtts + ForEachRelated.optionalAtts)))
                raise ParseError(msg)
        for a in ForEachRelated.requiredAtts:
            if a not in atts:
                msg = ("foreach related tag missing required attribute:\n\n{}\n\n"
                        "Required Attributes: '{}'".format(ET.tostring(e).rstrip(),
                                                           ", ".join(ForEachRelated.requiredAtts)))
                raise ParseError(msg)

        self.id = atts['id']

        if self.id in related_files:
            msg = ("foreach tag contains duplicate related file id: {}\n\n{}".format(self.id, ET.tostring(e)))
            raise ParseError(msg)
        if self.id in pipeline_files:
            msg = "a foreach related file's id must not be the same as a pipeline file id: {}\n\n{}".format(self.id, ET.tostring(e))
            raise ParseError(msg)

        self.is_input = atts['input'].upper() == 'TRUE'
        self.pattern = re.compile(atts['pattern'])
        self.replace = atts['replace']

        if 'in_dir' in atts:
            self.indir = atts['in_dir']
        else:
            self.indir = None
        self.is_temp = False
        if 'temp' in atts:
            self.is_temp = atts['temp'].upper() == 'TRUE'
        related_files[self.id] = self
