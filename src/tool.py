# Standard imports
import sys
import os
import re
import tempfile
import xml.etree.ElementTree as ET

# pipeline components
from job_runner.batch_job import *
import pipeline_parse as PL

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
        'walltime',
        ]

    def __init__(self, xmlfile, ins, outs, pipelineFiles):
        # Does placing the import here help?
        import pipeline_parse as PL
        self.options = {}
        self.commands = []
        self.modules = ['python/2.7.3']
        self.verify_files = ['python']
        self.tool_files = {}
        self.pipelineFiles = pipelineFiles
        for n in range(len(ins)):
            f = pipelineFiles[ins[n]]
            ToolFile('in_' + str(n+1), f.path, 
                     self.tool_files)
        for n in range(len(outs)):
            f = pipelineFiles[outs[n]]
            ToolFile('out_' + str(n+1), f.path,
                     self.tool_files)

        # First try to find the xml file in the current working directory,
        # If not found, look in the same directory as the master pipeline
        # directory.
        # FIXME: We may not want to do this for the CLIA certified pipeline!!!
        if not os.path.exists(xmlfile):
            xmlfile = os.path.join(PL.master_XML_dir, xmlfile)
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
            self.config_prefix = atts['tool_config_prefix']
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
                Option(child, self.options, self.tool_files)
            elif t == 'command':
                Command(child, self.commands, self.options, self.tool_files)
            elif t == 'tempfile':
                # Register the tempfile in the tool's file dictionary
                self.tempFile(child)
            elif t == 'module':
                self.modules.append(child.text)
            elif t == 'validate':
                self.validate_files.append(child.text)
            else:
                print >> sys.stderr, 'Unprocessed tag:', t

    def tempFile(self, e):
        validAtts = [
            'id',
            'directory',
            ]
        atts = e.attrib
        for a in atts:
            assert a in validAtts, 'tempfile tag has unknown attribute: ' + a

        id = atts['id']
        # Ensure that the id is unique.
        assert id not in self.options, ('tempfile id duplicates an option'
                                        'name: ' + self.id)
        assert id not in self.tool_files, ('tempfile id is a duplicate: ' +
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
        ToolFile(id, name, self.tool_files, temp=True)

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
        # Get the current symbols in the pipeline...
        import pipeline_parse as PL

        """
        print >> sys.stderr, "dumping tool files"
        for fid in self.tool_files:
            f = self.tool_files[fid]
            print >> sys.stderr, fid, f.path
        """

        # 
        # Now it is time to fix up the commands and write the script file.
        # We couldn't do this before, because we have to ensure that ALL 
        # pipeline XML processing is done. (tempfiles need an output dir,
        # and might have been specified before the output dir.
        # Tempfiles specified in the pipeline have already been fixed up 
        # and have paths.  Here in the tool, they appear as normal files.
        # This is different from tempfiles specified in the tool; they
        # really are temp, and can be stored locally on the node and cleaned
        # up on tool exit.
        #
        for c in self.commands:
            c.fixupOptionsFiles()
            # Add the command names to the verify_files list
            p = c.program
            if p not in self.verify_files:
                self.verify_files.append(c.program)

        # actually run the tool
        name = '{0}_{1}'.format(name_prefix, self.name)
        multi_command_list = []
        for c in self.commands:
            multi_command_list.append(c.real_command)
        multi_command = '\n'.join(multi_command_list)
        """
        print 'Batch Job:\n' + multi_command
        print '    workdir:', PL.output_dir
        print '    files_to_verify:', self.verify_files
        print '    ppn:', self.threads
        print '    walltime:', self.walltime
        print '    modules:', self.modules
        print '    depends_on:', depends_on
        print '    name:', name
        """

        # Do the actual batch job sumbission
        batch_job = BatchJob(
            multi_command, workdir=PL.output_dir, files_to_check=self.verify_files, 
            ppn=self.threads, walltime = self.walltime, modules=self.modules,
            depends_on=depends_on, name=name)
    
        job_id = PL.job_runner.queue_job(batch_job)

        print 'Job', self.name, 'submitted as job id:', job_id
        return job_id
        
    def getCommand(self):
        # return the command as a string
        pass

    def logCommand(self):
        # write the command to be executed to a log file.
        # Not sure this is the place to have this, or whether some place else
        # simply calls getCommand and writes it.
        pass


class ToolFile():
    def __init__(self, id, path, tool_files, temp=False):
        self.id = id
        self.path = path
        self.temp = temp
        tool_files[self.id] = self

    def __repr__(self):
        return ' '.join(['id', self.id, 'path', str(self.path), 'temp', str(self.temp)])
        
class Option():
    def __init__(self, e, options, tool_files):
        self.command_text = ''
        self.value = ''
        try:
            name = e.attrib['name'].strip()
            command_text = e.attrib['command_text'].strip()
            value = e.attrib['value'].strip()
        except:
            dumpElement(e, 0)
            return
        self.name = name
        self.isFile = False
        self.command_text = command_text
        self.value = value

        # We don't allow the same option name in a tool twice
        assert self.name not in options, 'Option ' + self.name + 'is a duplicate'
        assert self.name not in tool_files, 'Option ' + self.name + 'is a duplicate of a file ID'
        options[name] = self
        

    def __repr__(self):
        return ' '.join(['Option:', 'n', self.name, 'c', self.command_text, 'v', self.value])

    def __str__(self):
        return self.__repr__()

class Command():
    validAtts = [
        'delimiters',
        'program',
        'stderr_id',
        'stdout_id',
        ]
    def __init__(self, e, commands, options, tool_files):
        # Stash the options and tool_files dictionaries.  We'll need
        # them to fix up the command lines.
        self.options = options
        self.tool_files = tool_files
        self.version_command = None
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
            self.stdout_id = atts['stdout_id']
        else:
            self.stdout_id = None
        if 'stderr_id' in atts:
            self.stderr_id = atts['stderr_id']
        else:
            self.stderr_id = None

        for child in e:
            t = child.tag
            assert t == 'version_command', ('unknown child tag in a command'
                                            ' tag: ' + t)
            assert not self.version_command
            self.version_command = re.sub('\s+', ' ', child.text).strip()

        # Strip out excess white space in the command
        self.command_template = re.sub('\s+', ' ', e.text).strip()
        commands.append(self)

    def fixupOptionsFiles(self):
        # The token replacement function is an inner function, so that
        # it has access to the self attributes.
        def tokenReplace(m):
            tok = m.group(1)
            if tok in self.options:
                o = self.options[tok]
                return o.command_text + ' ' + o.value
            if tok in self.tool_files:
                return self.tool_files[tok].path
            return 'UNKNOWN OPTION OR FILE ID: ' + tok
            
        # Fix up a command by replacing all the delimited option names and
        # file ids with the real option text and file paths.
        self.real_command = (self.program + ' ' +
                             self.replacePattern.sub(tokenReplace,
                                                     self.command_template))

        # Set up to capture output and error redirection, if requested.
        if self.stdout_id:
            self.real_command += ' > ' + self.tool_files[self.stdout_id].path
        if self.stderr_id:
            self.real_command += ' 2> ' + self.tool_files[self.stderr_id].path


