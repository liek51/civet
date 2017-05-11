# Copyright 2016 The Jackson Laboratory
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import print_function

import sys
import os
import tempfile
import re
import datetime
import xml.etree.ElementTree as ET

import utilities
import civet_exceptions

import pipeline_parse as PL

class PipelineFile(object):
    """
    Initialize ourselves from an XML tag that represents some kind
    of file.  Arguments: XML element representing the file, and a
    hash of processed files
    """
    validFileTags = [
        'file',
        'filelist',
        'dir',
        'string'
        ]

    # these attributes are shared between file, dir, and string tags (but not filelist)
    valid_common_attributes = [
        'id',
        'append',
        'datestamp_append',
        'datestamp_prepend',
        'parameter',
        'based_on',
        'pattern',
        'replace',
    ]

    # attributes that only make sense for a file tag
    valid_file_attributes = [
        'create',
        'input',
        'in_dir',
        'temp',
        'filespec',
        ]

    # attributes that only make sense for a dir tag
    valid_dir_attributes = [
        'create',
        'input',
        'in_dir',
        'temp',
        'filespec',
        'default_output',
        'from_file',
    ]

    # attributes that only make sense for a string
    valid_string_attributes = [
        'value'
    ]

    # valid attributes for a file list
    valid_list_attributes = [
        'id',
        'foreach_id',
        'in_dir',
        'pattern',
        'parameter',
        'input'
    ]

    # Track the master output directory.
    output_dir = None

    # And the parameters to the pipeline
    params = None

    def __init__(self, id, path, files, is_file=False, is_temp=False,
                 is_input=False, is_dir=False, is_string=False, based_on=None,
                 pattern=None, replace=None, append=None,
                 datestamp_prepend=None, datestamp_append=None, in_dir=None,
                 is_parameter=False, is_list=False, from_file=None, create=True,
                 default_output=False, foreach_dep=None):
        self.id = id
        self.path = path
        self._is_file = is_file
        self.is_temp = is_temp
        self.is_input = is_input
        self._is_dir = is_dir
        self.is_string = is_string
        self.based_on = based_on
        self.pattern = pattern
        self.replace = replace
        self.append = append
        self.datestamp_prepend = datestamp_prepend
        self.datestamp_append = datestamp_append
        self.in_dir = in_dir
        self.is_parameter = is_parameter
        self.is_list = is_list
        self.create = create
        self._is_fixed_up = False
        self.creator_job = None
        self.consumer_jobs = []
        self.foreach_dep = foreach_dep
        self.from_file = from_file

        # need a separate variable for this because is_parameter gets reset to
        # False once the param number -> value conversion happens
        self.list_from_param = True if is_list and is_parameter else False

        if self.id in files:
            # We've already seen this file ID.
            raise civet_exceptions.ParseError("File with ID '{}' was already defined".format(self.id))
        else:
            # Register this file in the files/options namespace
            files[self.id] = self

        # Mark this as the default output directory if necessary.
        if default_output:
            self.set_output_dir()

    # Track creator jobs to support file-based dependency scheduling.
    def set_creator_job(self, j):
        self.creator_job = j

    # Track the jobs that use this file as an input.  This is needed
    # to properly know when we can remove our temp files at the end
    # of a run.
    def add_consumer_job(self, j):
        self.consumer_jobs.append(j)

    @staticmethod
    def add_simple_dir(id, path, files):
        PipelineFile(id, path, files, False, False, True, True, False,
                     None, None, None, None, None, None, None, False, False,
                     None)

    @staticmethod        
    def parse_XML(e, files):

        import pipeline_parse as PL

        t = e.tag
        att = e.attrib
        # Make sure that we have the right kind of tag.
        if t not in PipelineFile.validFileTags:
            msg = "{}: Invalid tag '{}:'\n\n{}".format(os.path.basename(PL.xmlfile), t, ET.tostring(e))
            raise civet_exceptions.ParseError(msg)

        # id attribute is required, make sure this id is not already
        # in use, or, if it is, that it has the same attributes.
        id = att['id']

        # What kind of file...
        is_file = t == 'file'
        is_dir = t == 'dir'
        is_list = t == 'filelist'
        is_string = t == 'string'

        # Init some variables.
        path = None
        based_on = None
        pattern = None
        replace = None
        append = None
        datestamp_prepend = None
        datestamp_append = None
        is_parameter = False
        default_output = False
        foreach_dep = None
        from_file = None


        # make sure that the attributes make sense with the type of tag we are
        if is_file:
            for a in att:
                if a not in PipelineFile.valid_common_attributes + PipelineFile.valid_file_attributes:
                    msg = "Illegal pipeline file attribute: '{}'\n\n{}".format(a, ET.tostring(e))
                    raise civet_exceptions.ParseError(msg)

        elif is_dir:
            for a in att:
                if a not in PipelineFile.valid_common_attributes + PipelineFile.valid_dir_attributes:
                    msg = "Illegal pipeline dir attribute: '{}'\n\n{}".format(a, ET.tostring(e))
                    raise civet_exceptions.ParseError(msg)

            if 'default_output' in att:
                default_output = att['default_output'].upper() == 'TRUE'
                if 'in_dir' in att:
                    msg = ("Must not combine default_output and "
                           "in_dir attributes.\n\n{}").format(ET.tostring(e))
                    raise civet_exceptions.ParseError(msg)
            if 'from_file' in att:
                from_file = att['from_file']
                if 'filespec' in att:
                    msg = ("Must not combine 'from_file' and "
                           "'filespec'\n\n{}").format(ET.tostring(e))
                    raise civet_exceptions.ParseError(msg)

            valid_source = ['filespec', 'based_on', 'parameter', 'from_file']
            if True not in [x in att for x in valid_source]:
                msg = ("dir tag must contain one of:  {}"
                       "\n\n{}").format(", ".join(valid_source), ET.tostring(e))
                raise civet_exceptions.ParseError(msg)

        elif is_string:
            for a in att:
                if a not in PipelineFile.valid_common_attributes + PipelineFile.valid_string_attributes:
                    msg = "Illegal pipeline string attribute '{}'\n\n{}".format(a, ET.tostring(e))
                    raise civet_exceptions.ParseError(msg)
        elif is_list:
            for a in att:
                if a not in PipelineFile.valid_list_attributes:
                    msg = "Illegal pipeline filelist attribute '{}'\n\n{}".format(a, ET.tostring(e))
                    raise civet_exceptions.ParseError(msg)


        # What kind of file?
        is_temp = False
        if 'temp' in att:
            is_temp = att['temp'].upper() == 'TRUE'

        # Input?
        is_input = False
        if 'input' in att:
            is_input = att['input'].upper() == 'TRUE'

        # Create directory?
        if is_dir:
            if 'create' in att:
                create = att['create'].upper() == 'TRUE'
            else:
                create = True
        else:
            create = False

        in_dir = None
        if 'in_dir' in att:
            in_dir = att['in_dir']

        if 'filespec' in att:
            path = att['filespec']

        if 'value' in att:
            path = att['value']

        if 'parameter' in att:
            if path:
                msg = ("Must not have both 'filespec' and 'parameter' "
                       "attributes:\n\n{}").format(ET.tostring(e))
                raise civet_exceptions.ParseError(msg)
            if in_dir:
                msg = ("Must not have both 'in_dir' and 'parameter' "
                       "attributes:\n\n{}").format(ET.tostring(e))
                raise civet_exceptions.ParseError(msg)
            path = int(att['parameter'])
            is_parameter = True

        if is_list and 'pattern' in att:
            pattern = att['pattern']
            if 'foreach_id' in att:
                foreach_dep = att['foreach_id']

        if 'based_on' in att:
            if path or from_file:
                msg = ("Must not combined 'based_on' with 'filespec', "
                       "'parameter', or 'from_file' "
                       "attributes:\n\n{}").format(ET.tostring(e))
                raise civet_exceptions.ParseError(msg)

            based_on = att['based_on']

            if 'pattern' in att:
                pattern = att['pattern']
                if not 'replace' in att:
                    msg = ("'pattern' attribute specified without 'replace' "
                           "attribute:\n\n{}").format(ET.tostring(e))
                    raise civet_exceptions.ParseError(msg)
                replace = att['replace']


            if 'datestamp_append' in att:
                datestamp_append = att['datestamp_append']
            if 'datestamp_prepend' in att:
                datestamp_prepend = att['datestamp_prepend']

            if 'append' in att:
                if datestamp_append:
                    msg = ("'append' attribute is incompatible with "
                           "'datestamp_append' attribute:"
                           "\n\n{}").format(ET.tostring(e))
                    raise civet_exceptions.ParseError(msg)
                append = att['append']

        if is_list and not ((pattern and in_dir) or is_parameter):
            msg = ("'filelist' requires 'in_dir' and 'pattern' or it must be "
                   "passed as a parameter\n\n{}".format(ET.tostring(e)))
            raise civet_exceptions.ParseError(msg)
        if is_list and pattern and is_input:
            msg = ("pattern based filelist may not be specified as "
                   "input:\n\n{}").format(ET.tostring(e))
            raise civet_exceptions.ParseError(msg)

        PipelineFile(
            id, path, files, is_file, is_temp, is_input, is_dir,
            is_string, based_on, pattern, replace, append,
            datestamp_prepend, datestamp_append, in_dir,
            is_parameter, is_list, from_file, create, default_output,
            foreach_dep)

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return 'File:{} p:{} iI:{} it:{} iD:{} BO:{} Rep:{} Pat:{} Ap:{} DS:{} DSA:{} inD:{}'.format(
            self.id, self.path, self.is_input,
            self.is_temp, self._is_dir, self.based_on, self.replace,
            self.pattern, self.append, self.datestamp_prepend, self.datestamp_append,
            self.in_dir)

    def set_output_dir(self):
        # Register at most one output directory
        if PipelineFile.output_dir:
            raise civet_exceptions.ParseError("ERROR: only one directory can "
                                               "be marked as the default output directory")
        PipelineFile.output_dir = self

    @staticmethod
    def get_output_dir():
        assert PipelineFile.output_dir._is_fixed_up, "Using output_dir before fixed up..."
        return PipelineFile.output_dir.path

    @staticmethod
    def register_params(params):
        PipelineFile.params = params

    @staticmethod
    def fix_up_files(files):

        circularity = []

        # First off, fix up the output dir if we have one; others
        # may depend on it.
        if not PipelineFile.output_dir:
            # there is no output_dir
            # create a default one in our current working directory
            PipelineFile.output_dir = PipelineFile('default_output', ".", files,
                                                   False, False, False, True,
                                                   False, None, None,
                                                   None, None, None, None, None,
                                                   False, False, None,
                                                   create=True,
                                                   default_output=True,
                                                   foreach_dep=None)

        PipelineFile.output_dir.fix_up_file(files, circularity)

        if PipelineFile.output_dir.is_input or not PipelineFile.output_dir.create:
            if not os.path.exists(PipelineFile.output_dir.path):
                raise civet_exceptions.MissingFile("default output directory flagged as input must exist at pipeline submission time")
        else:
            utilities.make_sure_path_exists(PipelineFile.output_dir.path)

        for fid in files:
            files[fid].fix_up_file(files, circularity)

    def fix_up_file(self, files, circularity):
        """
        Take care of all the inter-file dependencies such as
        in_dir and based_on, as well as files passed in as
        parameters.
        """

        if self._is_fixed_up:
            return

        # detect dependency cycles
        if self in circularity:
            msg = "File dependency cycle detected processing '{}' ".format(self.id)
            for f in circularity:
                msg = msg + "\n" + str(f)
            msg = msg + "\n\n" + str(self)
            raise civet_exceptions.ParseError(msg)

        circularity.append(self)

        self.parameter_to_path()
        self.apply_from_file(files, circularity)
        self.apply_based_on(files, circularity)
        if not self.is_string:
            # might raise civet_exception.ParseError
            # to be handled at a higher level
            self.apply_in_dir_and_create_temp(files, circularity)

        # Turn all the paths into an absolute path, so changes in
        # working directory throughout the pipeline lifetime don't
        # foul us up. First check if the file doesn't have a path at all
        # i.e., just a filename.  If so, and it is not an input file,
        # place it in the output directory.
        if self.is_list:
            if self.in_dir:
                # filelist is comprised of a directory and pattern,
                # convert the directory to an absolute path
                self.in_dir = os.path.abspath(files[self.in_dir].path)
            elif self.list_from_param:
                # file list is passed as a parameter represented as a comma
                # delimited list.
                # convert paths in list to absolute path
                file_list = []
                for f in self.path.split(','):
                    file_list.append(os.path.abspath(f))
                self.path = ','.join(file_list)
        elif not self.is_string:
            path = self.path
            if (os.path.split(path)[0] == '' and
                (not self.is_input) and
                self != PipelineFile.output_dir and
                (PipelineFile.output_dir is None or
                     PipelineFile.output_dir._is_fixed_up)):
                path = os.path.join(PipelineFile.get_output_dir(), path)
            self.path = os.path.abspath(path)

        self._is_fixed_up = True

        # Make sure a directory exists, unless explicitly requested
        # to not do so.
        if self._is_dir and self.create:
            utilities.make_sure_path_exists(self.path)

        check = circularity.pop()

        if check != self:
            print("circularity.pop() failed!\ncheck:{}".format(check),
                  file=sys.stderr)
            print(" self:{}".format(self), file=sys.stderr)
            sys.exit(1)

    def parameter_to_path(self):
        if self.is_parameter:
            idx = self.path - 1
            params = PipelineFile.params
            if idx >= len(params):
                #print("Too few parameters...\n"
                #      "    len(params): {}\n"
                #      "    File: {}\n"
                #      "    referenced parameter: {}"
                #      "".format(len(params), self.id, self.path),
                #                  file=sys.stderr)
                msg = "Parameter out of range, File: {} referenced parameter: {} (pipeline was passed {} parameters)".format(self.id, self.path, len(params))
                raise civet_exceptions.ParseError(msg)
                #sys.exit(1)
            self.path = params[idx]
            self.is_parameter = False

    def apply_from_file(self, files, circularity):
        if not self.from_file:
            return
            
        if self.from_file not in files:
            sys.exit("ERROR: 'from_file' specifies unknown file id: {0}".format(self.from_file))
        ff = files[self.from_file]
        ff.fix_up_file(files, circularity)
        
        # get the directory from ff, strip any trailing slashes so
        # os.path.dirname does what we want
        self.path = os.path.dirname(ff.path.rstrip(os.path.sep))

    def apply_based_on(self, files, circularity):
        bo = self.based_on
        if not bo:
            return
        # the based_on attribute is the fid of another file
        # whose path we're going to mangle to create ours.
        if bo not in files:
            sys.exit('ERROR: {} is based on unknown file: {}'.format(self.id, bo))
        bof = files[bo]
        bof.fix_up_file(files, circularity)

        if bof.list_from_param:
            # based on a filelist passed as a parameter,  use first file in the
            # filelist
            path = bof.path.split(',')[0]
        else:
            path = bof.path

        # strip out any path - based_on only operates on filenames
        temp_path = os.path.basename(path)
        now = datetime.datetime.now()

        # do the replace first,  so there is no chance other based_on
        # actions could affect the pattern matching
        if self.replace:
            temp_path = re.sub(self.pattern, self.replace, temp_path)

        if self.append:
            temp_path = temp_path + self.append
        if self.datestamp_append:
            temp_path += now.strftime(self.datestamp_append)
        if self.datestamp_prepend:
            temp_path = now.strftime(self.datestamp_prepend) + temp_path

        self.path = temp_path

    def apply_in_dir_and_create_temp(self, files, circularity):
        ind = self.in_dir
        if (not ind) and (not self.is_temp):
            return

        if ind:
            if ind not in files:
                msg = ("ERROR: while processing file with id: '{}', "
                       "in_dir is unknown file: '{}'".format(self.id, ind))
                raise civet_exceptions.ParseError(msg)
            indf = files[ind]
            indf.fix_up_file(files, circularity)
            my_dir = indf.path
        else:
            my_dir = PipelineFile.get_output_dir()

        if self.is_list:
            return
        elif self.is_temp and not self.path:
            # If it is an anonymous temp, we'll create it in
            # the proper directory
            if self._is_dir:
                self.path = tempfile.mkdtemp(dir=my_dir)
            else:
                t = tempfile.NamedTemporaryFile(dir=my_dir, delete=False)
                name = t.name
                t.close()
                self.path = name
            if ind:
                self.in_dir = None
        elif ind:
            if os.path.isabs(self.path):
                raise civet_exceptions.ParseError("Can't combine 'in_dir' attribute with absolute path")

            # Apply the containing directory to the path...
            self.path = os.path.join(my_dir, self.path)

            # in_dir has been applied, clear it.
            self.in_dir = None
