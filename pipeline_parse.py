#! /bin/env python

# pipeline_parse.py
# Process the XML file describing the overall pipeline.

import sys
import xml.etree.ElementTree as ET

class Pipeline():
	def __init__(self, xmlfile, params):
		pipe = ET.parse(xmlfile).getroot()
		
		# The outermost tag must be pipeline; it must have a name
		# and must not have text
		assert pipe.tag == 'pipeline'
		self.name = pipe.attrib['name']
		assert not pipe.text.strip()

		self.processes = []
		self.files = {}
		# Walk the child tags.
		for child in pipe:
			# a pipeline can only contain process, input, output, 
			# outputdir or tempfile
			t = child.tag
			assert t == 'input' or t == 'output' or t == 'outputdir' or t == 'tempfile' or t == 'process', ' illegal tag: ' + t
			if t == 'process':
				self.processes.append(Process(child, self.files))
			else:
				PipelineFile(child, self.files)
		
		# Here we have finished parsing the pipeline XML Time to fix up 
		# the file paths that were passed in as positional...
		self.fixupPositionalFiles(params)

	def fixupPositionalFiles(self, params):
		pLen = len(params)
		print self.files
		for f in self.files:
			if not f.pathIsPath:
				if f.path >= pLen:
					print >> sys.stderr, 'You did not specify enough files for this pipeline. Exiting.'
					sys.exit(1)
				f.path = params[f.path]
class Process():
	def __init__(self, e, files):
		print 'In process:', e.tag, e.attrib
		# Every process requires a name.
		assert len(e.attrib) == 1, "Process must (only) a name attribute"
		self.name = e.attrib['name']
		self.tools = []
		for child in e:
			t = child.tag
			# print 'Process child:', t, child.attrib
			assert t == 'input' or t == 'output' or t == 'tool', 'Illegal tag in process'
			if t == 'tool':
				self.tools.append(PipelineTool(child))
			else:
				PipelineFile(child, files)

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

class PipelineFile():
	# Initialize ourselves from an XML tag that represents some king
	# of file.
	def __init__(self, e, files):
		print 'In PFile:', e.tag, e.attrib
		t = e.tag
		att = e.attrib
		# Make sure that we have the right kind of tag.
		assert t == 'input' or t == 'output' or t == 'tempfile' or t == 'outputdir', 'Illegal tag: ' + t

		# id attribute is required, make sure we're not already in, or,
		# if we are, that we have the same attributes.
		self.id = att['id']

		# We are a file...
		self.isFile = True

		# Init some variables.
		self.pathIsPath = False
		self.path = None
		self.fileType = None
		
		# What kinf of file?
		self.isTemp = e.tag == 'tempfile'
		self.isInput = e.tag == 'input'
		self.isDir = e.tag == 'outputdir'
		
		# All except directories require a type 
		if not self.isDir:
			self.fileType = e.attrib['type']

		# All except temp files need either a filespec or parameter
		if not self.isTemp:
			if 'filespec' in att:
				self.path = att['filespec']
				self.pathIsPath = True
			if 'parameter' in att:
				assert not self.path, 'Must not have both filespec and parameter attributes.'
				self.path = att['parameter']

		if self.id in files:
			# We've already seen this file ID.
			# Make sure they're compatible
			self.compatible(files[self.id])
		else:
			# Register this file in the files/options namespace
			files[self.id] = self

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

		

	def __str__(self):
		return 'File: %s\tp: %s\tt: %s\tiP: %r\tiI: %r\tit: %r\tiD: %r' % (self.id, self.path, self.fileType, self.pathIsPath, self.isInput, self.isTemp, self.isDir)
		
# Eventually moved to tool_parse!!!  FIXME
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
	pipeline = Pipeline(sys.argv[1], sys.argv[2:])
	
if __name__ == "__main__":
	main()