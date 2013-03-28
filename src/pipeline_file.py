import sys
import os
import tempfile
import re

import utilities

class PipelineFile():
    """
    Initialize ourselves from an XML tag that represents some king
    of file.  Arguments: XML element representing the file, and a
    hash of processed files
    """
    validFileTags = [
        'file',
        'dir',
        ]

    valid_file_attributes = [
        'create',
        'id',
        'input',
        'in_dir',
        'temp',
        'filespec',
        'parameter',
        'based_on',
        'pattern',
        'replace',
        ]

    # Track the master output directory.
    output_dir = None

    # And the parameters to the pipeline
    params = None

    def __init__(self, id, path, is_file, is_temp, is_input, is_dir, 
                 files, is_path, based_on, pattern, replace, in_dir,
                 is_parameter, create=True):
        self.id = id
        self.path = path
        self._is_file = is_file
        self.is_temp = is_temp
        self._is_input = is_input
        self._is_dir = is_dir
        self.is_path = is_path
        self.based_on = based_on
        self.pattern = pattern
        self.replace = replace
        self.in_dir = in_dir
        self.is_parameter = is_parameter
        self.create = create
        self._is_fixed_up = False

        if self.id in files:
            # We've already seen this file ID.
            # Make sure they're compatible
            self.compatible(files[self.id])
        else:
            # Register this file in the files/options namespace
            files[self.id] = self

    @staticmethod        
    def parse_XML(e, files):
        t = e.tag
        att = e.attrib
        # Make sure that we have the right kind of tag.
        assert t in PipelineFile.validFileTags, ('Illegal pipeline file tag: "'
                                                 + t + '"')

        for a in att:
            assert a in PipelineFile.valid_file_attributes, (
                'Illegal pipeline file attribute: "' + a + '"')

        # id attribute is required, make sure this id is not already
        # in use, or, if it is, that it has the same attributes.
        id = att['id']
        # We are a file...
        is_file = t == 'file'
        is_dir = t == 'dir'

        # Init some variables.
        path_is_path = False
        path = None
        based_on = None
        pattern = None
        replace = None
        is_parameter = False

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

        if 'parameter' in att:
            assert not path, ('Must not have both filespec'
                              'and parameter attributes.')
            path = int(att['parameter'])
            is_parameter = True

        if 'based_on' in att:
            assert (not path), (
                'Must not have based_on and path or parameter attributes.')
            based_on = att['based_on']
            pattern = att['pattern']
            replace = att['replace']

        #if is_temp and not path:
        #    print >> sys.stderr, "temp", id, "has no path"

        PipelineFile(
            id, path, is_file, is_temp, is_input, is_dir, files, 
            path_is_path, based_on, pattern, replace, in_dir,
            is_parameter, create)

        # This routine latches the first output directory we see.
        # We call it arbitrarily for all files.
        PipelineFile.set_output_dir(files[id])

    @property
    def is_output_dir(self):
        return self._is_dir and not self._is_input

    @staticmethod
    def from_filename(id, name, is_input, files):
        print >> sys.stderr, '****************from_filename() used:', name
        PipelineFile(id, name, 
                     True,     # is_file
                     False,    # is_temp
                     is_input,
                     False,    # is_dir
                     files,
                     True,     # path_is_path
                     None,     # based_on
                     None,     # pattern
                     None,     # replace
                     None,     # in_dir
                     False,    # is_parameter
                     False)    # create

    def compatible(self, o):
        # We have a file whose ID we've already seen. 
        # Make sure they're compatible.
        
        # Second instance must not have a path, be a tempfile, or a
        # directory.
        assert not self.path
        assert not self.is_temp
        assert not self._is_dir
            
    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return 'File:{0} p:{1} iP:{2} iI:{3} it:{4} iD:{5} BO:{6} Rep:{7} Pat:{8} inD:{9}'.format(
            self.id, self.path, self.is_path, self._is_input,
            self.is_temp, self._is_dir, self.based_on, self.replace,
            self.pattern, self.in_dir)

    @staticmethod
    def set_output_dir(f):
        # Only register the first output dir that is not cwd
        if PipelineFile.output_dir:
            return
        if not f._is_dir:
            return
        if f.is_output_dir:
            PipelineFile.output_dir = f

    @staticmethod
    def get_output_dir():
        if not PipelineFile.output_dir:
            return '.'
        if not PipelineFile.output_dir._is_fixed_up:
            raise Exception( "Using output_dir before fixed up...")
            sys.exit(1)
        return PipelineFile.output_dir.path

    @staticmethod
    def register_params(params):
        PipelineFile.params = params

    @staticmethod
    def fix_up_files(files):
        #print >> sys.stderr, '\n\nFixing up files. params: ', PipelineFile.params
        # First off, fix up the output dir if we have one; others
        # may depend on it.
        circularity = []
        if PipelineFile.output_dir:
            PipelineFile.output_dir.fix_up_file(files, circularity)

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
            print >> sys.stderr, '\nFile dependency cycle detected.'
            for f in circularity:
                print >> sys.stderr, f
            print >> sys.stderr, self
            sys.exit(1)

        #print >> sys.stderr, ('  ' * len(circularity)) + 'Fixing up' + str(self)
        circularity.append(self)

        self.parameter_to_path()
        self.apply_based_on(files, circularity)
        self.apply_in_dir_and_create_temp(files, circularity)

        # Make sure a directory exists, unless explicitly requested
        # to not do so.
        if self._is_dir and self.create:
            utilities.make_sure_path_exists(self.path)

        # Turn all the paths into an absolute path, so changes in
        # working directory throughout the pipeline lifetime don't
        # foul us up. First check if the file doesn't have a path at all
        # i.e., just a filename.  If so, and it is not an input file,
        # place it in the output directory.
        path = self.path
        if os.path.split(path)[0] == '' and self != PipelineFile.output_dir:
            path = os.path.join(PipelineFile.get_output_dir(), path)
        self.path = os.path.abspath(path)

        self._is_fixed_up = True

        check = circularity.pop()

        if check != self:
            print >> sys.stderr, 'circularity.pop() failed!\ncheck:', check
            print >> sys.stderr, ' self:', self
            sys.exit(1)

        #print >> sys.stderr, ('  ' * len(circularity)) + 'Done with' + str(self)


    def parameter_to_path(self):
        if self.is_parameter:
            idx = self.path - 1
            params = PipelineFile.params
            if idx >= len(params):
                print >> sys.stderr, ('Too few parameters... len(params): ' +
                    str(len(params)) + 'File: ' + self.id + 
                    ' referenced parameter ' + self.path)
            self.path = params[idx]
            self.is_path = True
            self.is_parameter = False

    def apply_based_on(self, files, circularity):
        bo = self.based_on
        if not bo:
            return
        # the based_on attribute is the fid of another file
        # whose path we're going to mangle to create ours.
        #print >> sys.stderr, 'processing based_on\n', self
        if not bo in files:
            print >> sys.stderr, 'ERROR: based on unknown file:', bo
            sys.exit(1)
        bof =files[bo]
        bof.fix_up_file(files, circularity)

        # strip out any path before using the re, in case the replacement
        # uses the leading strin twice.
        original_path = os.path.basename(bof.path)

        self.path = re.sub(self.pattern, self.replace, original_path)

    def apply_in_dir_and_create_temp(self, files, circularity):
        ind = self.in_dir
        if (not ind) and (not self.is_temp):
            return
        dir = PipelineFile.get_output_dir()

        utilities.make_sure_path_exists(dir)
        if ind:
            if not ind in files:
                print >> sys.stderr, ('ERROR: while processing ' 
                                      'file with id: ' + self.id +
                                      ', in_dir is unknown file: ' + ind)
                sys.exit(1)
            indf = files[ind]
            indf.fix_up_file(files, circularity)
            dir = indf.path

        if self.is_temp and not self.path:
            # If it is an anonymous temp, we'll create it in
            # the proper directory
            t = tempfile.NamedTemporaryFile(dir=dir, delete=False)
            name = t.name
            t.close()
            self.path = name
            self.is_path = True
        elif ind:
            # Apply the containing directory to the path...
            fn = os.path.split(self.path)[1]
            self.path = os.path.join(dir, fn)
            self.in_dir = None
