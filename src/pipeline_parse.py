#! /bin/env python

# pipeline_parse.py
# Process the XML file describing the overall pipeline.

import sys
import re
import xml.etree.ElementTree as ET
from foreach import *
from tool import *
from tool_logger import *

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
            if not f.pathIsPath:
                #print 'Fixing up', fid, 'path index is', f.path,
                if f.path > pLen:
                    print >> sys.stderr, 'You did not specify enough files for this pipeline. Exiting.'
                    sys.exit(1)
                f.path = params[f.path - 1]
                #print ' path is:', f.path

    def execute(self):
        print 'Executing pipeline', self.name
        for step in self.stepes:
            step.execute(self.files)


class Step():
    validTags = [
        'tool' ]
    def __init__(self, e, files):
        print 'In step:', e.tag, e.attrib
        # Every step requires a name.
        assert len(e.attrib) == 1, "Step must have (only) a name attribute"
        self.name = e.attrib['name']
        self.tools = []
        for child in e:
            t = child.tag
            # print 'Step child:', t, child.attrib
            assert t in Step.validTags, 'Illegal tag in step'
            self.tools.append(PipelineTool(child))

    def execute(self, files):
        print '    Executing step', self.name
        for tool in self.tools:
            tool.execute(files)

            
class PipelineTool():
    def __init__(self, e):
        print 'In PTool:', e.tag, e.attrib
        att = e.attrib
        self.input = []
        self.output = []
        # Every tool requires a name and a description, which is 
        # the path to the tool's XML file.
        self.name = att['name']
        self.description = att['description']
        if 'input' in att:
            self.input = att['input'].split(',')
            for n in range(len(self.input)):
                self.input[n] = self.input[n].strip()
        if 'output' in att:
            self.output = att['output'].split(',')
            for n in range(len(self.output)):
                self.output[n] = self.output[n].strip()

        self.tool = Tool(self.description)
        
    def execute(self, files):
        print '        Executing tool', self.name


class PipelineFile():
    # Initialize ourselves from an XML tag that represents some king
    # of file.  Arguments: XML element representing the file, and a
    # hash of processed files
    validFileTags = [
        'input',
        'output',
        'tempfile',
        'outputdir' ]
        
    def __init__(self, id, path, type, isFile, isTemp, isInput, isDir, files, isPath):
        self.id = id
        self.path = path
        self.filetype = type
        self.isFile = isFile
        self.isTemp = isTemp
        self.isInput = isInput
        self.isDir = isDir
        self.isPath = isPath

        if self.id in files:
            # We've already seen this file ID.
            # Make sure they're compatible
            self.compatible(files[self.id])
        else:
            # Register this file in the files/options namespace
            files[self.id] = self


    @staticmethod        
    def parseXML(e, files):
        print 'In PFile.parseXML:', e.tag, e.attrib
        t = e.tag
        att = e.attrib
        # Make sure that we have the right kind of tag.
        assert t in PipelineFile.validFileTags, 'Illegal pipeline file tag: ' + t

        # id attribute is required, make sure we're not already in, or,
        # if we are, that we have the same attributes.
        id = att['id']

        # We are a file...
        isFile = True

        # Init some variables.
        pathIsPath = False
        path = None
        fileType = None
        
        # What kinf of file?
        isTemp = e.tag == 'tempfile'
        isInput = e.tag == 'input'
        isDir = e.tag == 'outputdir'
        
        # All except directories require a type 
        if not isDir:
            fileType = e.attrib['type']

        # All except temp files need either a filespec or parameter
        if not isTemp:
            if 'filespec' in att:
                path = att['filespec']
                pathIsPath = True
            if 'parameter' in att:
                assert not path, 'Must not have both filespec and parameter attributes.'
                path = int(att['parameter'])

        PipelineFile(id, path, filetype, isFile, isTemp, isInput, isDir, files, pathIsPath)


    @staticmethod
    def fromFilename(id, name, isInput, files):
        PipelineFile(id, name, None, True, False, isInput, False, files, True)

    def compatible(self, o):
        # We have a file whose ID we've already seen. 
        # Make sure they're compatible.
        
        # Second instance must not have a path, be a tempfile, or a
        # directory.
        assert not self.path
        assert not self.isTemp
        assert not self.isDir
            
        # Same type of file 
        assert self.fileType == o.fileType

    def __repr__(self):
        return self.__str__()
    def __str__(self):
        return 'File: %s\tp: %s\tt: %s\tiP: %r\tiI: %r\tit: %r\tiD: %r' % (self.id, self.path, self.fileType, self.pathIsPath, self.isInput, self.isTemp, self.isDir)
        
# Eventually moved to tool_parse!!!  FIXME

class Tool():
    def __init__(self, xmlfile):
        tool = ET.parse(xmlfile).getroot()

class ToolOption():
    def __init__(self, element):
        try:
            name = e.attrib['name']
            command_text = e.attrib['command_text']
            value = e.attrib['value']
        except:
            dumpElement(e, 0)
            return
        self.name = name
        self.isFile = False
        self.command_text = command_text
        self.value = value
        
        # We don't allow the same option name in a tool twice
        assert name not in FileOrOption.names
        FileOrOption.names[name] = self

    def __str__(self):
        return '\t'.join(['Option: ' + self.name, self.command_text, self.value])

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