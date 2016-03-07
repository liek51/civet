#
# Copyright (C) 2016  The Jackson Laboratory
#
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

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
        'pipeline_root'
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

    def __init__(self, id, path, is_file, is_temp, is_input, is_dir, 
                 files, is_path, is_string, based_on, pattern, replace, append,
                 datestamp_prepend, datestamp_append, in_dir,
                 is_parameter, is_list, from_file, create=True, default_output=False,
                 foreach_dep=None):
        self.id = id
        self.path = path
        self._is_file = is_file
        self.is_temp = is_temp
        self.is_input = is_input
        self._is_dir = is_dir
        self.is_path = is_path
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
        if is_list and is_parameter:
            self.list_from_param = True
        else:
            self.list_from_param = False

        if self.id in files:
            # We've already seen this file ID.
            # Make sure they're compatible
            # this will raise a civet_exception.ParseError if they are not
            # compatible
            self.compatible(files[self.id])
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
        path_is_path = False
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

            if 'pipeline_root' in att and 'filespec' in att:
                msg = ("Must not combine 'pipeline_root' and "
                       "'filespec'\n\n{}").format(ET.tostring(e))
                raise civet_exceptions.ParseError(msg)

            valid_source = ['filespec', 'based_on', 'parameter', 'from_file',
                            'pipeline_root']
            if True not in [x in att for x in valid_source]:
                msg = ("dir tag must contain one of:  {}"
                       "\n\n{}").format(", ".join(valid_source), ET.tostring(e))
                raise civet_exceptions(msg)

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
            path_is_path = True

        if 'pipeline_root' in att and att['pipeline_root'].upper() == "TRUE":
            path = PL.master_XML_dir
            path_is_path = True

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

            if 'datestamp_append' in att or 'datestamp_prepend' in att:
                if pattern or replace:
                    msg = ("'datestamp' attribute is incompatible with "
                            "'replace' attribute:\n\n{}").format(ET.tostring(e))
                    raise civet_exceptions.ParseError(msg)
                if 'datestamp_append' in att:
                    datestamp_append = att['datestamp_append']
                if 'datestamp_prepend' in att:
                    datestamp_prepend = att['datestamp_prepend']

            if 'append' in att:
                if pattern or replace or datestamp_append:
                    msg = ("'append' attribute is incompatible with "
                           "'datestamp', 'replace', and 'pattern' attributes:"
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

        #if is_temp and not path:
        #    print >> sys.stderr, "temp", id, "has no path"

        PipelineFile(
            id, path, is_file, is_temp, is_input, is_dir, files,
            path_is_path, is_string, based_on, pattern, replace, append,
            datestamp_prepend, datestamp_append, in_dir,
            is_parameter, is_list, from_file, create, default_output, foreach_dep)

    def compatible(self, o):
        # We have a file whose ID we've already seen. 
        # Make sure they're compatible.
        
        # Second instance must not have a path, be a tempfile, or a
        # directory.
        if self.path or self.is_temp or self._is_dir:
            raise civet_exceptions.ParseError("Incompatible redeclaration of "
                                              "{}".format(self.id))

            
    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return 'File:{0} p:{1} iP:{2} iI:{3} it:{4} iD:{5} BO:{6} Rep:{7} Pat:{8} Ap:{9} DS:{10} DSA:{11} inD:{12}'.format(
            self.id, self.path, self.is_path, self.is_input,
            self.is_temp, self._is_dir, self.based_on, self.replace,
            self.pattern, self.append, self.datestamp_prepend, self.datestamp_append,
            self.in_dir)

    def set_output_dir(self):
        # Register at most one output directory
        if PipelineFile.output_dir:
            sys.stderr.write('ERROR: only one directory can be marked as the default output directory\n')
            sys.exit(1)
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
        #print >> sys.stderr, '\n\nFixing up files. params: ', PipelineFile.params

        circularity = []

        # First off, fix up the output dir if we have one; others
        # may depend on it.
        if not PipelineFile.output_dir:
            # there is no output_dir
            # create a default one in our current working directory
            PipelineFile.output_dir = PipelineFile('default_output', ".", False,
                                                   False, False, True, files,
                                                   True, False, None, None,
                                                   None, None, None, None, None,
                                                   False, False, None,
                                                   create=True,
                                                   default_output=True,
                                                   foreach_dep=None)

        PipelineFile.output_dir.fix_up_file(files, circularity)

        for fid in files:
            files[fid].fix_up_file(files, circularity)

    def fix_up_file(self, files, circularity):
        """
        Take care of all the inter-file dependencies such as
        in_dir and based_on, as well as files passed in as
        parameters.
        """
        import pipeline_parse as PL

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

        if self is PipelineFile.output_dir:
            if PL.directory_version == 2:
                stamp_dir = "{0}-{1}".format(datetime.datetime.now().strftime('%Y%m%d_%H%M%S'), os.getpid())
                self.path = os.path.join(self.path, stamp_dir)
            utilities.make_sure_path_exists(self.path)
        else:
            # might raise civet_exception.ParseError, to be handled at a higher level
            self.apply_in_dir_and_create_temp(files, circularity)

        # Turn all the paths into an absolute path, so changes in
        # working directory throughout the pipeline lifetime don't
        # foul us up. First check if the file doesn't have a path at all
        # i.e., just a filename.  If so, and it is not an input file,
        # place it in the output directory.
        if self.is_list:
            if self.in_dir:
                #filelist is comprised of a directory and pattern,
                #convert the directory to an absolute path
                self.in_dir = os.path.abspath(files[self.in_dir].path)
            elif self.list_from_param:
                # file list is passed as a parameter,  might be comma delimited
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
                (PipelineFile.output_dir is None or PipelineFile.output_dir._is_fixed_up)):
                path = os.path.join(PipelineFile.get_output_dir(), path)
            self.path = os.path.abspath(path)

        self._is_fixed_up = True

        # Make sure a directory exists, unless explicitly requested
        # to not do so.
        if self._is_dir and self.create:
            utilities.make_sure_path_exists(self.path)

        check = circularity.pop()

        if check != self:
            print("circularity.pop() failed!\ncheck:{}".format(check), file=sys.stderr)
            print(" self:{}".format(self), file=sys.stderr)
            sys.exit(1)


    def parameter_to_path(self):
        if self.is_parameter:
            idx = self.path - 1
            params = PipelineFile.params
            if idx >= len(params):
                print("Too few parameters...\n"
                      "    len(params): {}\n"
                      "    File: {}\n"
                      "    referenced parameter: {}"
                      "".format(len(params), self.id, self.path),
                                  file=sys.stderr)
                sys.exit(1)
            self.path = params[idx]
            self.is_path = True
            self.is_parameter = False

    def apply_from_file(self, files, circularity):
        if not self.from_file:
            return
            
        if self.from_file not in files:
            sys.exit("ERROR: 'from_file' specifies unknown file id: {0}".format(ff))
        ff = files[self.from_file]
        ff.fix_up_file(files, circularity)
        
        # get the directory from ff, strip any trailing slashes so os.path.dirname does what we want
        self.path = os.path.dirname(ff.path.rstrip(os.path.sep))

    def apply_based_on(self, files, circularity):
        bo = self.based_on
        if not bo:
            return
        # the based_on attribute is the fid of another file
        # whose path we're going to mangle to create ours.
        #print >> sys.stderr, 'processing based_on\n', self
        if bo not in files:
            sys.exit('ERROR: based on unknown file: {0}'.format(bo))
        bof = files[bo]
        bof.fix_up_file(files, circularity)

        if bof.list_from_param:
            sys.exit("ERROR: file can not be based on a list: {0} is based on {1}".format(self.id, bo))

        # strip out any path - based_on only operates on filenames
        temp_path = os.path.basename(bof.path)
        now = datetime.datetime.now()

        if self.append:
            temp_path = temp_path + self.append
        if self.datestamp_append:
            temp_path = temp_path + now.strftime(self.datestamp_append)
        if self.datestamp_prepend:
            temp_path = now.strftime(self.datestamp_prepend) + temp_path
        if self.replace:
            temp_path = re.sub(self.pattern, self.replace, temp_path)
        self.path = temp_path


    def apply_in_dir_and_create_temp(self, files, circularity):
        ind = self.in_dir
        if (not ind) and (not self.is_temp):
            return
        dir = PipelineFile.get_output_dir()

        utilities.make_sure_path_exists(dir)
        if ind:
            if ind not in files:
                msg = ("ERROR: while processing file with id: '{}', "
                       "in_dir is unknown file: '{}'".format(self.id, ind))
                raise civet_exceptions.ParseError(msg)
            indf = files[ind]
            indf.fix_up_file(files, circularity)
            dir = indf.path

        if self.is_list:
            return
        elif self.is_temp and not self.path:
            # If it is an anonymous temp, we'll create it in
            # the proper directory
            if self._is_dir:
                self.path = tempfile.mkdtemp(dir=dir)
            else:
                t = tempfile.NamedTemporaryFile(dir=dir, delete=False)
                name = t.name
                t.close()
                self.path = name
                self.is_path = True
            if ind:
                self.in_dir = None
        elif ind:
            if os.path.isabs(self.path):
                raise civet_exceptions.ParseError("Can't combine 'in_dir' attribute with absolute path")


            # Apply the containing directory to the path...
            fn = os.path.split(self.path)[1]
            self.path = os.path.join(dir, fn)
            self.in_dir = None
