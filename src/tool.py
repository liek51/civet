# Standard imports
import sys
import os
import re
import xml.etree.ElementTree as ET

# pipeline components
from global_data import *

class Tool():
    validTags = [
        'command',
        'description',
        'option',
        'tempfile' ]
    validAtts = [
        'name',
        'tool_config_prefix',
        'threads' ]
        
    def __init__(self, xmlfile, ins, outs, pipelineFiles):
        self.options = {}
        self.commands = []
        self.pipelineFiles = pipelineFiles
        # First try to find the xml file in the current working directory,
        # If not found, look in the same directory as the master pipeline directory.
        # FIXME: We may not want to do this for the CLIA certified pipeline!!!
        if not os.path.exists(xmlfile):
            xmlfile = os.path.join(globalData['masterXMLdir'], xmlfile)
        if not os.path.exists(xmlfile):
            print >> sys.stderr, 'ERROR: Could not find tool XML file:', xmlfile, '\nExiting...'
            sys.exit(1)

        tool = ET.parse(xmlfile).getroot()
        atts = tool.attrib
        # Validate the attributes
        for a in atts:
            assert a in Tool.validAtts, 'unknown attribute in tool tag: ' + a
        
        self.name = atts['name']
        if 'tool_config_prefix' in atts:
            self.configPrefix = atts['tool_config_prefix']
        if 'threads' in atts:
            self.threads = atts['threads']
        else:
            self.threads = '1'

        # Now process our child tags
        for child in tool:
            t = child.tag
            assert t in Tool.validTags, 'unknown child tag in tool tag: ' + t
            if t == 'description':
                # This one is so simple we process it inline here, instead of 
                # having a different class to process it.
                self.description = child.text
            elif t == 'option':
                Option(child, self.options, self.pipelineFiles)
            elif t == 'command':
                Command(child, self.commands)
            else:
                print >> sys.stderr, 'Unprocessed tag:', t

    def replaceOptions(command):
        
    def logVersion(self):
        pass
    def validate(self):
        # Determine whether this tool is unchanged from those that were certified for CLIA.
        pass
    def execute(self):
        # actually run the tool
        for c in self.commands:
            print '            executing command:', c.program, c.commandString
    def getCommand(self):
        # return the command as a string
        pass
    def logCommand(self):
        # write the command to be executed to a log file.
        # Not sure this is the place to have this, or whether some place else
        # simply calls getCommand and writes it.
        pass
        
class Option():
    def __init__(self, e, options, files):
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
        assert self.name not in options, 'Option ' + self.name + 'is a duplicate'
        assert self.name not in files, 'Option ' + self.name + 'is a duplicate of a file ID'
        options[name] = self

    def __str__(self):
        return '\t'.join(['Option: ' + self.name, self.command_text, self.value])

class Command():
    validAtts = [
        'delimiters',
        'program',
        'stderr_id',
        'stdout_id',
        ]
    def __init__(self, e, commands):
        self.versionCommand = None
        atts = e.attrib
        for a in atts:
            assert a in Command.validAtts, 'Unknown attribute in command tag: ' + a
        # The program attribute is required.  The remainder are optional.
        self.program = atts['program']
        if 'delimiters' in atts:
            self.delims = atts['delimiters']
        else:
            self.delims = '{}'
        self.replacePattern = re.compile(self.delims[0] + '(.*?)' + self.delims[1])
        if 'stderr_id' in atts:
            self.stderrId = atts['stderr_id']
        if 'stdout_id' in atts:
            self.stdoutId = atts['stdout_id']
        for child in e:
            t = child.tag
            assert t == 'version_command', 'unknown child tag in a command tag: ' + t
            assert not self.versionCommand
            self.versionCommand = re.sub('\s+', ' ', child.text).strip()
        self.commandString = re.sub('\s+', ' ', e.text).strip()
        commands.append(self)

class TempFile:
    foo = 1
