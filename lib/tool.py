# Standard imports
import sys
import os
import re
import tempfile
import xml.etree.ElementTree as ET

# pipeline components

from job_runner.batch_job import *
import pipeline_parse as PL
from pipeline_file import *

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
        'dir',
        'option',
        'file',
        'validate',
        'module',
        ]

    validAtts = [
        'error_strings',
        'name',
        'threads',
        'tool_config_prefix',
        'walltime',
        ]

    def __init__(self, xml_file, ins, outs, pipeline_files, skip_validation=False):
        # Don't understand why this has to be here as well to get some
        # symbols. But it seems to be needed.
        import pipeline_parse as PL

        self.options = {}
        self.commands = []
        self.tempfile_ids = []
        self.ins = ins
        self.outs = outs
        self.skip_validation=skip_validation
        self.verify_replacement_pattern = None

        # Any pipeline will rely on having these modules loaded.
        # Other modules must be specified in the tool descriptions.
        self.modules = ['python/2.7.3']

        self.verify_files = ['python']
        self.tool_files = {}
        self.pipeline_files = pipeline_files
        for n in range(len(ins)):
            f = pipeline_files[ins[n]]
            self.tool_files['in_' + str(n+1)] = f
        for n in range(len(outs)):
            f = pipeline_files[outs[n]]
            self.tool_files['out_' + str(n+1)] = f

        # First try to find the xml file in the current working directory,
        # If not found, look in the same directory as the master pipeline
        # directory.
        # FIXME: We may not want to do this for the CLIA certified pipeline!!!
        if not os.path.exists(xml_file):
            xml_file = os.path.join(PL.master_XML_dir, xml_file)
        if not os.path.exists(xml_file):
            print >> sys.stderr, ('ERROR: Could not find tool XML file:'
                                  , xml_file, '\nExiting...')
            sys.exit(1)

        #print >> sys.stderr, '***Parsing tool file:', xml_file
        #print >> sys.stderr, self.ins

        self.xml_file = xml_file

        # Verify that the tool definition file has not changed.
        self.verify_files.append(os.path.abspath(xml_file))
       
        tool = ET.parse(xml_file).getroot()
        atts = tool.attrib
        # Validate the attributes
        for a in atts:
            assert a in Tool.validAtts, 'unknown attribute in tool tag: ' + a

        # The name attribute is required.  All others are optional.
        self.name = atts['name']

        if 'error_strings' in atts:
            self.error_strings = []
            # The error strings are a comma-sep list of strings
            # to search for.  Spaces have to be quoted or escaped.
            estrings = atts['error_strings'].split(',')
            for es in estrings:
                self.error_strings.append(es.strip())
        else:
            self.error_strings = None

        if 'tool_config_prefix' in atts:
            self.config_prefix = atts['tool_config_prefix']
        else:
            self.config_prefix = None

        if 'threads' in atts:
            self.threads = atts['threads']
        else:
            self.threads = '1'

        if 'walltime' in atts:
            self.walltime = atts['walltime']
        else:
            self.walltime = '01:00:00'


        # We can't process any non-file tags until all our files
        # are processed and fixed up.  Rather than force an order
        # in the user's file, we simply stash the other tags in
        # a "pending tags" list.
        pending = []

        # Now process our child tags
        for child in tool:
            t = child.tag
            assert t in Tool.validTags, 'unknown child tag in tool tag: ' + t


            if t == 'file' or t == 'dir':
                # Register the file in the tool's file dictionary
                self.file(child)
            else:
                pending.append(child)

        # Now we can fix up our files.
        PipelineFile.fix_up_files(self.tool_files)

        # Now, finally, we can process the rest of the tags.
        for child in pending:
            t = child.tag
            if t == 'description':
                # This one is so simple we process it inline here, instead of 
                # having a different class to process it.
                self.description = child.text
            elif t == 'option':
                Option(child, self.options, self.tool_files)
            elif t == 'command':
                Command(child, self.commands, self.options, self.tool_files, self.xml_file)
            elif t == 'module':
                self.modules.append(child.text)
            elif t == 'validate':
                a = child.attrib
                if 'id' in a:
                    name = self.tool_files[a['id']].path
                else:
                    name = child.text
                self.verify_files.append(name)
            else:
                print >> sys.stderr, 'Unprocessed tag:', t
        #
        # And fix up any replacement file id tokens in the validate tags.
        #
        self.fixup_validate_files()

    def file(self, e):
        atts = e.attrib

        id = atts['id']
        # Ensure that the id is unique.
        assert id not in self.options, ('file id duplicates an option'
                                        'name: ' + self.id)
        assert id not in self.tool_files, ('file id is a duplicate: ' +
                                          self.id)
        

        PipelineFile.parse_XML(e, self.tool_files)

        # Track all the tool temporary files, so that we can
        # delete them at the end of the tool's execution.
        if self.tool_files[id].is_temp:
            self.tempfile_ids.append(id)

    def collect_files_to_validate(self):
        v = self.verify_files
        for c in self.commands:
            v.append(c.program)
        return v

    def collect_version_commands(self):
        vcs = []
        for c in self.commands:
            if c.real_version_command:
                vc = c.real_version_command
                if vc not in vcs:
                    vcs.append(vc)
        return vcs

    def submit(self, name_prefix):
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
        # pipeline XML processing is done. (Tempfiles need an output dir,
        # and might have been specified before the output dir.)
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

        # actually run the tool; get the date/time at the start of every
        # command, and at the end of the run.
        name = '{0}_{1}'.format(name_prefix, self.name)
        multi_command_list = []
        for c in self.commands:
            multi_command_list.append('date; ' + c.real_command)
        multi_command_list.append('date')

        # Tack on a final command to delete our temp files.
        if self.tempfile_ids:
            # Convert from file ids to paths.
            for n in range(len(self.tempfile_ids)):
                self.tempfile_ids[n] = (
                    self.tool_files[self.tempfile_ids[n]].path)

            rm_cmd = 'rm ' + ' '.join(self.tempfile_ids)
            multi_command_list.append(rm_cmd)

        multi_command = '\n'.join(multi_command_list)

        # Determine what jobs we depend on based on our input files.
        depends_on = []
        for fid in self.ins:
            f = self.pipeline_files[fid]
            if f.creator_job:
                j = f.creator_job
                if j not in depends_on:
                    depends_on.append(j)

        # Do the actual batch job sumbission
        if self.skip_validation:
            verify_file_list = None
        else:
            verify_file_list = self.verify_files  
        batch_job = BatchJob(
            multi_command, workdir=PipelineFile.get_output_dir(), 
            files_to_check=verify_file_list, 
            ppn=self.threads, walltime = self.walltime, modules=self.modules,
            depends_on=depends_on, name=name, error_strings=self.error_strings, 
            version_cmds=self.collect_version_commands())
    
        job_id = PL.job_runner.queue_job(batch_job)

        # Any files that we created and that will be passed to other jobs
        # need to be marked with our job id.  It is OK if we overwrite
        # a previous job.  I think. (FIXME?)
        for fid in self.outs:
            f = self.pipeline_files[fid]
            f.set_creator_job(job_id)

        # Mark the files we depend on so that they're not cleaned up too 
        # early.  Really only needs to be done for temp files, but for
        # simplicity, we mark them all.
        for fid in self.ins:
            f = self.pipeline_files[fid]
            f.add_consumer_job(job_id)

        print job_id + ':', self.name
        return job_id

    def check_files_exist(self):
        missing = []
        for fid in self.tool_files:
            f = self.tool_files[fid]
            if f.is_input:
                if not os.path.exists(f.path):
                    missing.append(f.path)
        return missing

    def fixup_validate_files(self):
        """
        This function is a simpler version of Command.fixupOptionsFiles(),
        Rather than complexify the code flow to handle both cases, we have
        two similar functions.
        """
        # The token replacement function is an inner function, so that
        # it has access to the self attributes.
        def token_replace(m):
            tok = m.group(1)
            if tok in self.tool_files:
                f = self.tool_files[tok]
                if f.is_list:
                    # Emit the code to invoke a file filter.
                    return "$(process_filelist.py f.in_dir.path, f.pattern)"
                return f.path

            # We didn't match a file id. Put out an error.
            print >> sys.stderr, ("\n\nUNKNOWN FILE ID: " + tok + 
                                  ' in file ' + self.xml_file)
            print >> sys.stderr, 'Tool files:', self.tool_files
            return 'UNKNOWN FILE ID: ' + tok 

        # pre-compile the pattern
        if not self.verify_replacement_pattern:
            self.verify_replacement_pattern = re.compile('{(.*?)}')

        # Fix up all the files in the verify_files list.
        for n in range(len(self.verify_files)):
            self.verify_files[n] = self.verify_replacement_pattern.sub(
                token_replace, self.verify_files[n])

class Option():
    def __init__(self, e, options, tool_files):
        self.command_text = ''
        self.value = ''
        try:
            name = e.attrib['name'].strip()
            self.name = name
            command_text = e.attrib['command_text'].strip()
            if 'value' in e.attrib:
                value = e.attrib['value'].strip()
            elif 'from_file' in e.attrib:
                fid = e.attrib['from_file']
                fn = tool_files[fid].path
                value = '$(cat ' + fn + ') '
        except:
            print >> sys.stderr, 'unexpected problem with {0}'.format(self)
            print >> sys.stderr, sys.exc_info()[0]
            raise

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
    def __init__(self, e, commands, options, tool_files, xml_file):
        # Stash the options and tool_files dictionaries.  We'll need
        # them to fix up the command lines.
        self.options = options
        self.tool_files = tool_files
        self.xml_file = xml_file
        self.version_command = None
        self.real_version_command = None
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
        delim_1 = self.delims[0]
        delim_2 = self.delims[1]
        if delim_1 in '|':
            delim_1 = '\\' + delim_1
        if delim_2 in '|':
            delim_2 = '\\' + delim_2
        self.replacePattern = re.compile(delim_1 + '(.*?)' + delim_2)

        # Capture desired output redirection
        if 'stdout_id' in atts:
            self.stdout_id = atts['stdout_id']
        else:
            self.stdout_id = None
        if 'stderr_id' in atts:
            self.stderr_id = atts['stderr_id']
        else:
            self.stderr_id = None

        # The command text can be either in the command element's text,
        # or as the "tail" of the child <version_command> tag. Sigh.
        # Therefore we'll process it in parts.
        if e.text:
            command_text = e.text
        else:
            command_text = ""

        # Only allow one child in a Command tag
        child_found = False
        for child in e:
            assert not child_found, ('only one subtag allowed in '
                                           'command tag')
            child_found = True
            t = child.tag
            assert t == 'version_command', ('unknown child tag in a command'
                                            ' tag: ' + t)
            assert not self.version_command, ('a command must have at most one'
                                              'version command: ' + t)
            self.version_command = re.sub('\s+', ' ', child.text).strip()

            # Get any command text that the parser considers part of this
            # child.
            if child.tail:
                command_text += child.tail

        # Strip out excess white space in the command
        if command_text:
            self.command_template = re.sub('\s+', ' ', command_text).strip()
        else:
            self.command_template = ''

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
                f = self.tool_files[tok]
                if f.is_list:
                    # Emit the code to invoke a file filter.
                    return "$(process_filelist.py f.in_dir.path, f.pattern)"
                return f.path

            # We didn't match a known option, or a file id. Put out an error.
            print >> sys.stderr, "\n\nUNKNOWN OPTION OR FILE ID:", tok, 'in file', self.xml_file
            print >> sys.stderr, 'Tool files:', self.tool_files
            print >> sys.stderr, 'Options:', self.options, '\n\n'
            return 'UNKNOWN OPTION OR FILE ID: ' + tok 
            
        # Fix up a command by replacing all the delimited option names and
        # file ids with the real option text and file paths.
        self.real_command = (self.program + ' ' +
                             self.replacePattern.sub(tokenReplace,
                                                     self.command_template))

        # Similarly, fix up a version_command by replacing all the delimited 
        # option names and file ids with the real option text and file paths.
        if self.version_command:
            self.real_version_command = (self.replacePattern.sub(tokenReplace,
                                         self.version_command))

        # Set up to capture output and error redirection, if requested.
        if self.stdout_id:
            self.real_command += ' > ' + self.tool_files[self.stdout_id].path
        if self.stderr_id:
            self.real_command += ' 2> ' + self.tool_files[self.stderr_id].path
