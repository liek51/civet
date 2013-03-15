#! /bin/env python

# pipeline_parse.py
# Process the XML file describing the overall pipeline.

import sys
import re
import datetime
import xml.etree.ElementTree as ET

from foreach import *
from tool import *
from tool_logger import *
from pipeline_file import *

# from job_runner.torque import *

import utilities


class Pipeline(object):
    """
    A singleton module for information about the overall pipeline.
    
    For ease of access from other pipeline modules this class is inserted 
    in the Python modules chain using sys.modules. This technique was gleaned 
    from (URL on two lines... beware)

    http://stackoverflow.com/questions/880530/
    can-python-modules-have-properties-the-same-way-that-objects-can

    This is done at the end of the file, after the full definition of the class
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
        self._name = pipe.attrib['name']
        assert not pipe.text.strip()

        # We need to process all our files before we process anything else.
        # Stash anything not a file and process it in a later pass.
        pending = []

        # Set up for some properties
        self._output_dir = None
        self._log_dir = None
        self._job_runner = None
        
        # And track the major components of the pipeline
        self._steps = []
        self._files = {}

        # Walk the child tags.
        for child in pipe:
            # a pipeline can only contain step, input, output, 
            # outputdir or tempfile
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

        # Now that our files are all processed and fixed up, we can process
        # the rest of the XML involved with this pipeline.
        for child in pending:
            t = child.tag
            if t == 'step':
                self._steps.append(Step(child, self._files))
            elif t == 'foreach':
                self._steps.append(ForEach(child, self._files))

    @property
    def log_dir(self):
        if not self._log_dir:
            self._log_dir = os.path.join(self.output_dir, 'logs', datetime.datetime.now().strftime('%Y%m%d_%H%M%S'))
        return self._log_dir
        
    def submit(self):
        print 'Executing pipeline', self._name
        
        # FIXME!
        # We should check that all the input files and input directories
        # exist before going farther.  Fail early.
        depends_on = []
        invocation = 0
        for step in self._steps:
            invocation += 1
            name = '{0}_Step_{1}'.format(self._name, invocation)
            job_id = step.submit(depends_on, name)
            depends_on = [job_id]

        # We're done submitting all the jobs.  Release them and get on with it.
        # This is the last action of the pipeline submission process. WE'RE DONE!
        ### FIXME self.job_runner.release_all()

        # Let the people know where they can see their logs.
        print 'Log directory: ', self.log_dir

"""  FIXME
    @property
    def job_runner(self):
        # The pipeline will use a single Torque job runner.
        if not self._job_runner:
            self._job_runner =  TorqueJobRunner(self.log_dir, 
                                                validation_cmd="/hpcdata/asimons/validate")
        return self._job_runner
"""

sys.modules[__name__] = Pipeline()
