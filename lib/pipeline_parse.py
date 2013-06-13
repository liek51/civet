#! /bin/env python

# pipeline_parse.py
# Process the XML file describing the overall pipeline.

import sys
import re
import datetime
import xml.etree.ElementTree as ET

from foreach import *
from pipeline_file import *
from tool import *
from tool_logger import *

from job_runner.torque import *
from job_runner.batch_job import *
import utilities


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
    valid_tags = [
        'file',
        'dir',
        'foreach',
        'step' ]

    def __init__(self):
        pass
        
    def parse_XML(self, xmlfile, params):
        pipe = ET.parse(xmlfile).getroot()

        # Register the directory of the master (pipeline) XML.
        # We'll use it to locate tool XML files.
        self.master_XML_dir = os.path.split(xmlfile)[0]

        # Register the parameters that may be file paths
        PipelineFile.register_params(params)
        
        # The outermost tag must be pipeline; it must have a name
        # and must not have text
        assert pipe.tag == 'pipeline'
        self.name = pipe.attrib['name']
        assert not pipe.text.strip()

        # We need to process all our files before we process anything
        # else. Stash anything not a file and process it in a later pass.
        pending = []

        # Set up for some properties
        self._output_dir = None
        self._log_dir = None
        self._job_runner = None
        self.validation_file = os.path.splitext(xmlfile)[0] + '_validation.data'
        
        # And track the major components of the pipeline
        self._steps = []
        self._files = {}

        # Walk the child tags.
        for child in pipe:
            t = child.tag
            assert t in Pipeline.valid_tags, ' illegal tag:' + t
            if t == 'step' or t == 'foreach':
                pending.append(child)
            else:
                PipelineFile.parse_XML(child, self._files)
        
        # Here we have finished parsing the files in the pipeline XML.
        # Time to fix up various aspects of files that need to have
        # all files done first.

        PipelineFile.fix_up_files(self._files)

        # Now that our files are all processed and fixed up, we can
        # process the rest of the XML involved with this pipeline.
        for child in pending:
            t = child.tag
            if t == 'step':
                self._steps.append(Step(child, self._files, skip_validation))
            elif t == 'foreach':
                self._steps.append(ForEach(child, self._files, skip_validation))

    @property
    def log_dir(self):
        if not self._log_dir:
            self._log_dir = os.path.join(PipelineFile.get_output_dir(), 
                'logs', datetime.datetime.now().strftime('%Y%m%d_%H%M%S'))
        return self._log_dir

    def submit(self, skip_validation=False):
        print 'Executing pipeline', self.name

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
            name = '{0}_S{1}'.format(self.name, invocation)
            job_id = step.submit(name)
            for j in job_id:
                self.all_batch_jobs.append(j)

        # Submit two last bookkeepingjobs

        # 1. deletes all the temp files.
        tmps = []
        # This job is about deleting files... Don't bother looking for 
        # the file's creator_job.  What we really want are consumer jobs.
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
        cmd = 'rm ' + ' '.join(tmps)
        batch_job = BatchJob(cmd, workdir=PipelineFile.get_output_dir(),
                             depends_on=depends,
                             name='Remove_temp_files')
        job_id = self.job_runner.queue_job(batch_job)
        self.all_batch_jobs.append(job_id)

        # 2. Consolidate all the log files.
        cmd = 'consolidate_logs.py {0}'.format(self._log_dir)
        batch_job = BatchJob(cmd, workdir=PipelineFile.get_output_dir(),
                             depends_on=self.all_batch_jobs, 
                             name='Consolidate_log_files',
                             modules=['python/2.7.3'])
        self.job_runner.queue_job(batch_job)

        # We're done submitting all the jobs.  Release them and get
        # on with it. This is the last action of the pipeline
        # submission process. WE'RE DONE!
        self.job_runner.release_all()

        # Let the people know where they can see their logs.
        print 'Log directory: ', self.log_dir

    @property
    def job_runner(self):
        # The pipeline will use a single Torque job runner.
        if not self._job_runner:
            self._job_runner =  TorqueJobRunner(self.log_dir, 
                                                validation_cmd="validate -m "
                                                + self.validation_file,
                                                pipeline_bin=os.path.abspath(os.path.join(self.master_XML_dir, bin)))
        return self._job_runner

    def collect_files_to_validate(self):
        fns = []
        for step in self._steps:
            sfns = step.collect_files_to_validate()
            for fn in sfns:
                if fn not in fns:
                    fns.append(fn)
        return fns

    def check_files_exist(self):
        missing = []
        for step in self._steps:
            smissing = step.check_files_exist()
            for fn in smissing:
                if fn not in missing:
                    missing.append(fn)
        return missing


sys.modules[__name__] = Pipeline()
