# Standard imports
import sys
import os
import re
import tempfile
import xml.etree.ElementTree as ET

# pipeline components
from pipeline_parse import Pipeline
from global_data import *
from job_runner.torque import *

class Tool():
    # This script parses all of a tool definition.  Tools may be invoked
    # by the pipeline.
    # Tools are in separate files, so that we can substitute alternate
    # tools to perform the step.  Also, this allows a tool definition
    # to be part of multipile pipelines.
    # The only access a tool has to the pipeline's files is through
    # the ins and outs file id lists.
    # 
    # Each tool definition will create a temporary script which will be
    # submitted to the cluster as a single job.  This is performed in the
    # torque component.
    #
    validTags = [
        'command',
        'description',
        'option',
        'tempfile',
        'validate',
        'module',
        ]

    validAtts = [
        'name',
        'threads',
        'tool_config_prefix',
        'wallclock',
        ]

    def __init__(self, xmlfile, ins, outs, pipelineFiles):
        self.options = {}
        self.commands = []
        self.modules = []
        self.verify_files = ['python']
        self.toolFiles = {}
        self.pipelineFiles = pipelineFiles
        for n in range(len(ins)):
            ToolFile('in_' + str(n+1), pipelineFiles[ins[n]].path,
                     self.toolFiles)
        for n in range(len(outs)):
            ToolFile('out_' + str(n+1), pipelineFiles[outs[n]].path,
                     self.toolFiles)
        # First try to find the xml file in the current working directory,
        # If not found, look in the same directory as the master pipeline
        # directory.
        # FIXME: We may not want to do this for the CLIA certified pipeline!!!
        if not os.path.exists(xmlfile):
            xmlfile = os.path.join(global_data['masterXMLdir'], xmlfile)
        if not os.path.exists(xmlfile):
            print >> sys.stderr, ('ERROR: Could not find tool XML file:'
                                  , xmlfile, '\nExiting...')
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
        if 'walltime' in atts:
            self.walltime = atts['walltime']
        else:
            self.walltime = '01:00:00'

        # Now process our child tags
        for child in tool:
            t = child.tag
            assert t in Tool.validTags, 'unknown child tag in tool tag: ' + t
            if t == 'description':
                # This one is so simple we process it inline here, instead of 
                # having a different class to process it.
                self.description = child.text
            elif t == 'option':
                Option(child, self.options, self.toolFiles)
            elif t == 'command':
                Command(child, self.commands, self.options, self.toolFiles)
            elif t == 'tempfile':
                # Register the tempfile in the tool's file dictionary
                self.tempFile(child)
            elif t == 'module':
                self.modules.append(child.text)
            elif t == 'validate':
                self.validate_files.append(child.text)
            else:
                print >> sys.stderr, 'Unprocessed tag:', t

        # We've processed all the XML.
        # Now it is time to fix up the commands and write the script file.
        #
        for c in self.commands:
            c.fixupOptionsFiles()
            # Add the command names to the verify_files list
            self.verify_files.append(c.program)

        #with open (self.name + '_tool_script.sh', 'w') as of:
        #    print >> of, '#! /bin/bash'
        #    for c in self.commands:
        #        print >> of, c.realCommand


    def tempFile(self, e):
        validAtts = [
            'id',
            'directory',
            ]
        atts = e.attrib
        assert len(atts) >= 1, 'tempfile tag allows only one id attribute.'
        for a in atts:
            assert a in validAtts, 'tempfile tag has unknown attribute: ' + a

        id = atts['id']
        # Ensure that the id is unique.
        assert id not in self.options, ('tempfile id duplicates an option'
                                        'name: ' + self.id)
        assert id not in self.toolFiles, ('tempfile id is a duplicate: ' +
                                          self.id)
        
        if 'directory' in atts:
            dir = atts['directory']
        else:
            dir = None
        # Create a tempfle using python/OS techniques, to get a unique
        # temporary name.
        t = tempfile.NamedTemporaryFile(dir=dir, delete=False)
        name = t.name
        t.close()
        # At this point, the temp file exists, but will most likely be written
        # over with a new file from a command.
        # also at this point, it is just another tool file.
        ToolFile(id, name, self.toolFiles, temp=True)

    def logVersion(self):
        pass

    def validate(self):
        # Determine whether this tool is unchanged from those that were
        # certified for CLIA.
        pass

    def submit(self, depends_on, name_prefix):
        """
        Submit the commands that comprise the tool as a single cluster job.

        Args:
            depends_on: a list of previously submitted job ids which must
                complete before this job can run.
            name_prefix: a string, which when combined with this tool's
                name attribute, will result in a unique (to the pipeline)
                job name for the cluster.
        Returns:
            job_id: a value which can be passed in as a depends_on list 
                element in a subsequent tool sumbission.
        """
        # actually run the tool
        multi_command_list = []
        for c in self.commands:
            print '            executing command:', c.real_command
            multi_command_list.append(c.real_command)
        multi_command = '\n'.join(multi_command_list)
        pipeline = Pipeline.instance
        print 'Batch Job:'
        print '    cmd:', multi_command
        print '    workdir:', pipeline.output_dir
        print '    files_to_verify:', self.verify_files
        print '    ppn:', self.threads
        print '    walltime:', self.walltime
        print '    modules:', self.modules
        print '    depends_on:', depends_on
        print '    name:', name_prefix + self.name
        """
        batch_job = BatchJob(
            multi_command, workdir=pipeline.output_dir, files_to_verify=self.verify_files, 
            ppn=self.threads, walltime = self.walltime, modules=self.modules,
            depends_on=depends_on, name=name_prefix + self.name)
    
        return pipeline.job_runner.queue_job(batch_job)
        """
        return 1
        
    def getCommand(self):
        # return the command as a string
        pass

    def logCommand(self):
        # write the command to be executed to a log file.
        # Not sure this is the place to have this, or whether some place else
        # simply calls getCommand and writes it.
        pass


class ToolFile():
    def __init__(self, id, path, toolFiles, temp=False):
        self.id = id
        self.path = path
        self.temp = temp
        toolFiles[self.id] = self


        
class Option():
    def __init__(self, e, options, toolFiles):
        try:
            name = e.attrib['name']
            command_text = e.attrib['command_text']
            value = e.attrib['value']
        except:
            dumpElement(e, 0)
            return
        self.name = name
        self.isFile = False
        self.commandText = command_text
        self.value = value
        
        # We don't allow the same option name in a tool twice
        assert self.name not in options, 'Option ' + self.name + 'is a duplicate'
        assert self.name not in toolFiles, 'Option ' + self.name + 'is a duplicate of a file ID'
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
    def __init__(self, e, commands, options, toolFiles):
        # Stash the options and toolFiles dictionaries.  We'll need
        # them to fix up the command lines.
        self.options = options
        self.toolFiles = toolFiles
        self.versionCommand = None
        atts = e.attrib
        for a in atts:
            assert a in Command.validAtts, 'Unknown attribute in command tag: ' + a
        # The program attribute is required.  The remainder are optional.
        self.program = atts['program']

        # Delimiters are optional (and unusual!)
        if 'delimiters' in atts:
            self.delims = atts['delimiters']
            assert len(self.delims) == 2, 'command tag delimiters must be exactly two characters.'
        else:
            self.delims = '{}'
        self.replacePattern = re.compile(self.delims[0] + '(.*?)' + self.delims[1])

        # Capture desired output redirection
        if 'stdout_id' in atts:
            self.stdoutId = atts['stdout_id']
        else:
            self.stdoutId = None
        if 'stderr_id' in atts:
            self.stderrId = atts['stderr_id']
        else:
            self.stderrId = None

        for child in e:
            t = child.tag
            assert t == 'version_command', ('unknown child tag in a command'
                                            ' tag: ' + t)
            assert not self.versionCommand
            self.versionCommand = re.sub('\s+', ' ', child.text).strip()

        # Strip out excess white space in the command
        self.commandTemplate = re.sub('\s+', ' ', e.text).strip()
        commands.append(self)

    def fixupOptionsFiles(self):
        # The token replacement function is an inner function, so that
        # it has access to the self attributes.
        def tokenReplace(m):
            tok = m.group(1)
            if tok in self.options:
                o = self.options[tok]
                return o.commandText + ' ' + o.value
            if tok in self.toolFiles:
                return self.toolFiles[tok].path
            return 'UNKNOWN OPTION OR FILE ID: ' + tok
            
        # Fix up a command by replacing all the delimited option names and
        # file ids with the real option text and file paths.
        self.real_command = (self.program + ' ' +
                             self.replacePattern.sub(tokenReplace,
                                                     self.commandTemplate))

        # FIXME!
        # We may not want to do this, depending on whether the submission
        # mechanism has a different way to redirect I/O.  But for now we have
        # it.
        if self.stdoutId:
            self.real_command += ' > ' + self.toolFiles[self.stdoutId].path
        if self.stderrId:
            self.real_command += ' 2> ' + self.toolFiles[self.stderrId].path
        print >> sys.stderr, 'Created command', self.real_command


