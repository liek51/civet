#! /bin/env python

# pipeline_parse.py
# Process the XML file describing the overall pipeline.

import sys
import xml.etree.ElementTree as ET

class FileOrOption():
	names = {}
	def __init__(self):
		pass

	@staticmethod
	def get(name):
		if name in names:
			return names[name]
		return None

class PipelineFile(FileOrOption):
	def __init__(self, id, pathOrPosition, pathIsPath, isInput, isTemp):
		self.id = id
		self.isFile = True
		self.path = pathOrPosition
		self.pathIsPath = pathIsPath
		self.isInput = isInput
		self.isTemp = isTemp
		FileOrOption.names[id] = self

	def __str__(self):
		return 'File: %s\tp: %s\tiP: %r\tiI: %r\tit: %r' % (self.id, self.path, self.pathIsPath, self.isInput, self.isTemp)
		
class ToolOption(FileOrOption):
	def __init__(self, name, command_text, value):
		self.name = name
		self.isFile = False
		self.command_text = command_text
		self.value = value
		FileOrOption.names[name] = self

	def __str__(self):
		return '\t'.join(['Option: ' + self.name, self.command_text, self.value])
		
def processFile(e):
	pathIsPath = False
	path = None
	id = e.attrib['id']
	isTemp = e.tag == 'tempfile'
	isInput = e.tag == 'input'
	if not isTemp:
		if 'filespec' in e.attrib:
			path = e.attrib['filespec']
			pathIsPath = True
		if 'parameter' in e.attrib:
			if path:
				# FIXME 
				# throw an exception; can't have both spec and param
				pass
			else:
				path = e.attrib['parameter']
	PipelineFile(id, path, pathIsPath, isInput, isTemp)

def processOption(e):
	try:
		name = e.attrib['name']
		command_text = e.attrib['command_text']
		value = e.attrib['value']
	except:
		dumpElement(e, 0)
		return
	ToolOption(name, command_text, value)
	
def tagDispatcher(element):
	tag = element.tag
	if tag == 'input' or tag == 'output' or tag == 'tempfile':
		processFile(element)
	elif tag == 'option':
		processOption(element)
	else:
		print 'Skipping tag ', tag
	print len(FileOrOption.names)
		
def parse(fn):
	root = ET.parse(fn).getroot()
	return root

def processTree(element):
	tagDispatcher(element)
	for child in element:
		processTree(child)

def dumpElement(element, indent):
	print (' ' * indent * 4) + element.tag, element.attrib
	if element.text:
		print (' ' * indent * 4) + '  ' + element.text
	for child in element:
		dumpElement(child, indent+1)
		
def main():
	# For initial testing only, parse the file passed on the command line.
	root = parse(sys.argv[1])
	dumpElement(root, 0)
	
	# Test dispatching and parsing elements
	processTree(root)
	for name in FileOrOption.names:
		print FileOrOption.names[name]
	
if __name__ == "__main__":
	main()