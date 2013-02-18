#! /bin/env python

# pipeline_parse.py
# Process the XML file describing the overall pipeline.

import sys
import re
import xml.etree.ElementTree as ET
from foreach import *
from global_data import *
from tool import *
from tool_logger import *
from pipeline_file import *

class Pipeline():
    validTags = [
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
        self.name = pipe.attrib['name']
        assert not pipe.text.strip()

        self.steps = []
        self.files = {}
        # Walk the child tags.
        for child in pipe:
            # a pipeline can only contain step, input, output, 
            # outputdir or tempfile
            t = child.tag
            assert t in Pipeline.validTags, ' illegal tag:' + t
            if t == 'step':
                self.steps.append(Step(child, self.files))
            elif t == 'foreach':
                self.steps.append(ForEach(child, self.files))
            else:
                PipelineFile.parseXML(child, self.files)
        
        # Here we have finished parsing the pipeline XML Time to fix up 
        # the file paths that were passed in as positional...
        self.fixupPositionalFiles(params)

    def fixupPositionalFiles(self, params):
        pLen = len(params)
        #print 'params len is ', pLen, ':', params
        #print self.files
        for fid in self.files:
            f = self.files[fid]
            if not f.isPath:
                #print 'Fixing up', fid, 'path index is', f.path,
                if f.path > pLen:
                    print >> sys.stderr, 'You did not specify enough files for this pipeline. Exiting.'
                    sys.exit(1)
                f.path = params[f.path - 1]
                f.isPath = True

    def execute(self):
        print 'Executing pipeline', self.name
        
        # FIXME!
        # We should check that all the input files and input directories
        # exist before going farther.  Fail early.
        for step in self.steps:
            print >> sys.stderr, step
            step.execute()


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