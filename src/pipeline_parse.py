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
from job_runner.torque import *
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
        'input',
        'inputdir',
        'foreach',
        'output',
        'outputdir',
        'tempfile',
        'step' ]

    def __init__(self):
        pass
        
    def parse_XML(self, xmlfile, params):
        pipe = ET.parse(xmlfile).getroot()

        # Register the directory of the master (pipeline) XML.
        # We'll use it to locate tool XML files.
        self.master_XML_dir = os.path.split(xmlfile)[0]
        
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
        
        # Here we have finished parsing the pipeline XML Time to fix up 
        # the file paths that were passed in as positional...
        self.fixup_positional_files(params)
        self.paths_for_temp_files()

        # Now that our files are all processed and fixed up, we can process
        # the rest of the XML involved with this pipeline.
        for child in pending:
            t = child.tag
            if t == 'step':
                self._steps.append(Step(child, self._files))
            elif t == 'foreach':
                self._steps.append(ForEach(child, self._files))

    @property
    def output_dir(self):
        if not self._output_dir:
            self._output_dir = '.'
            for fid in self._files:
                f = self._files[fid]
                if f.is_output_dir():
                    self._output_dir = f.path
                    break
        return self._output_dir

    @property
    def log_dir(self):
        if not self._log_dir:
            self._log_dir = os.path.join(self.output_dir, 'logs', datetime.datetime.now().strftime('%Y%m%d_%H%M%S'))
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
            if not f.is_path and not f.is_temp:
                # print >> sys.stderr, 'Fixing up', fid, 'path index is', f.path,
                if f.path > pLen:
                    print >> sys.stderr, 'You did not specify enough files for this pipeline. Exiting.'
                    sys.exit(1)
                f.path = params[f.path - 1]
                f.isPath = True
            # Now that we know we have paths, make sure any output dirs exist.

            if f.is_output_dir():
                utilities.make_sure_path_exists(f.path)

    def paths_for_temp_files(self):
        """
        Temp files at the pipeline level can span across tool invocations,
        which means across Torque jobs.  Because of that, they need to live
        in the real cluster filesystem, not in a machine private temp directory.
        We'll clean them up at the end of the run.
        """
        for fid in self._files:
            f = self._files[fid]
            if not f.is_temp:
                continue
            if f.path:
                print >> sys.stderr, "HUH? temp file already has a path."
            # Create a tempfle using python/OS techniques, to get a unique
            # temporary name.
            t = tempfile.NamedTemporaryFile(dir=self.output_dir, delete=False)
            name = t.name
            t.close()
            f.path = name
            f.is_path = True
            print >> sys.stderr, f.id, self._files[fid].path

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
        self.job_runner.release_all()

        # Let the people know where they can see their logs.
        print 'Log directory: ', self.log_dir

    @property
    def job_runner(self):
        # The pipeline will use a single Torque job runner.
        if not self._job_runner:
            self._job_runner =  TorqueJobRunner(self.log_dir, 
                                                validation_cmd="/hpcdata/asimons/validate")
        return self._job_runner

sys.modules[__name__] = Pipeline()
