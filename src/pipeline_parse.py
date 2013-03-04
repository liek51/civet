#! /bin/env python

# pipeline_parse.py
# Process the XML file describing the overall pipeline.

import sys
import re
import datetime
import xml.etree.ElementTree as ET
from foreach import *
from global_data import *
from tool import *
from tool_logger import *
from pipeline_file import *
from torque import *



class Pipeline():
    """
    """
    
    instance = None

    valid_ags = [
        'input',
        'inputdir',
        'foreach',
        'output',
        'outputdir',
        'tempfile',
        'step' ]

    def __init__(self, xmlfile, params):
        pipe = ET.parse(xmlfile).getroot()
        
        # Register the directory of the master (pipeline) XML.
        # We'll use it to locate tool XML files.
        globalData['masterXMLdir'] = os.path.split(xmlfile)[0]
        
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
            assert t in Pipeline.validTags, ' illegal tag:' + t
            if t == 'step':
                self._steps.append(Step(child, self._files))
            elif t == 'foreach':
                self._steps.append(ForEach(child, self._files))
            else:
                PipelineFile.parseXML(child, self._files)
        
        # Here we have finished parsing the pipeline XML Time to fix up 
        # the file paths that were passed in as positional...
        self.fixup_positional_files(params)
        
        # Register ourself for later retrieval
        if Pipeline.instance:
            raise Exception('Pipeline already initialized.')
        Pipeline.instance = self

    @property
    def output_dir(self):
        if not self._output_dir:
            for f in files:
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
            if not f.isPath:
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
        
    def execute(self):
        print 'Executing pipeline', self._name
        
        # FIXME!
        # We should check that all the input files and input directories
        # exist before going farther.  Fail early.
        
        for step in self._steps:
            print >> sys.stderr, step
            step.execute()

    @property
    def job_runner(self):
        # The pipeline will use a single Torque job runner.
        if not self._job_runner:
            self._job_runner =  TorqueJobRunner(Pipeline.instance.log_dir)
        return self._job_runner


def dumpElement(element, indent):
    print (' ' * indent * 4) + element.tag, element.attrib
    if element.text:
        print (' ' * indent * 4) + '  ' + element.text
    for child in element:
        dumpElement(child, indent+1)
        
def main():
    # The name of the pipeline description is passed on the command line.
    #
    # This is a sample.  Requires two args.  Real one would take a variable list.
    if len(sys.argv) < 3:
        print >> sys.stderr, "This test version requires two arguments: XML, input file."
        sys.exit(1)
    pipeline = Pipeline(sys.argv[1], sys.argv[2:])
    pipeline.execute()

if __name__ == "__main__":
    main()