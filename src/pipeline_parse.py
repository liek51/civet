#! /bin/env python

# pipeline_parse.py
# Process the XML file describing the overall pipeline.

import sys
import re
import datetime
import xml.etree.ElementTree as ET


print '\n\n\n\n\n', dir(ET), '\n\n\n\n\n\n'

from foreach import *
from global_data import *
from tool import *
from tool_logger import *
from pipeline_file import *
from job_runner.torque import *



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
        'input',
        'inputdir',
        'foreach',
        'output',
        'outputdir',
        'tempfile',
        'step' ]

    def __init__(self):
        print 'in init', dir(ET)
        pass
        
    def parse_XML(self, xmlfile, params):
        print 'In pipeline.parse_XML', dir(ET)
        pipe = ET.parse(xmlfile).getroot()
        
        # Register the directory of the master (pipeline) XML.
        # We'll use it to locate tool XML files.
        global_data['masterXMLdir'] = os.path.split(xmlfile)[0]
        
        # The outermost tag must be pipeline; it must have a name
        # and must not have text
        assert pipe.tag == 'pipeline'
        self._name = pipe.attrib['name']
        assert not pipe.text.strip()

        self._steps = []
        self._files = {}

        # An output directory where log files and rul files will be
        # written, if not otherwise specified.  Defaults to '.'.
        self._output_dir = None

        self._job_runner = None
        
        # Walk the child tags.
        for child in pipe:
            # a pipeline can only contain step, input, output, 
            # outputdir or tempfile
            t = child.tag
            assert t in Pipeline.valid_tags, ' illegal tag:' + t
            if t == 'step':
                self._steps.append(Step(child, self._files))
            elif t == 'foreach':
                self._steps.append(ForEach(child, self._files))
            else:
                PipelineFile.parse_XML(child, self._files)
        
        # Here we have finished parsing the pipeline XML Time to fix up 
        # the file paths that were passed in as positional...
        self.fixup_positional_files(params)
        
        # Register ourself for later retrieval
        Pipeline.instance = self

    @property
    def output_dir(self):
        if not self._output_dir:
            for fid in self._files:
                f = self._files[fid]
                if f.is_output_dir():
                    self._output_dir = f.path
                    break
            self._output_dir = '.'
        return self._output_dir
        
    @property
    def log_dir(self):
        if not self._log_dir:
            self._log_dir = os.path.join(self.output_dir, 'logs', datetime.datetime.now().strftime('%Y%m%d_%H%m%S'))
        return self._log_dir
        
    def fixup_positional_files(self, params):
        """
        Some files in the XML file are identified by their position on the
        command line.  Turn them into real file names.
        """
        
        pLen = len(params)
        #print 'params len is ', pLen, ':', params
        #print self._files
        for fid in self._files:
            f = self._files[fid]
            if not f._isPath:
                #print 'Fixing up', fid, 'path index is', f.path,
                if f.path > pLen:
                    print >> sys.stderr, 'You did not specify enough files for this pipeline. Exiting.'
                    sys.exit(1)
                f.path = params[f.path - 1]
                f.isPath = True

    def log_dir(self):
        """
        Creates an output log directory for the pipeline run, based on 
        the pipeline name and current date and time.
        """
        self._logdir
        
    def submit(self):
        print 'Executing pipeline', self._name
        
        # FIXME!
        # We should check that all the input files and input directories
        # exist before going farther.  Fail early.
        depends_on = None
        invocation = 0
        for step in self._steps:
            invocation += 1
            name = '{0}_Step_{1}'.format(self._name, invocation)
            depends_on = step.submit(depends_on, name)

    @property
    def job_runner(self):
        # The pipeline will use a single Torque job runner.
        if not self._job_runner:
            self._job_runner =  TorqueJobRunner(Pipeline.instance.log_dir)
        return self._job_runner
sys.modules[__name__] = Pipeline()
print '******DONE PROCESSING*****', __name__, sys.modules[__name__]
