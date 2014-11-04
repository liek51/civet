import sys
import os
import tempfile
import re
import datetime

import utilities

import pipeline_parse as PL

class PipelineFile():
    """
    Initialize ourselves from an XML tag that represents some kind
    of file.  Arguments: XML element representing the file, and a
    hash of processed files
    """
    validFileTags = [
        'file',
        'filelist',
        'dir'
        ]

    valid_file_attributes = [
        'append',
        'create',
        'datestamp_append',
        'datestamp_prepend',
        'default_output',
        'id',
        'input',
        'in_dir',
        'temp',
        'filespec',
        'parameter',
        'based_on',
        'pattern',
        'replace',
        'foreach_id'
        ]

    # Track the master output directory.
    output_dir = None

    # And the parameters to the pipeline
    params = None

    def __init__(self, id, path, is_file, is_temp, is_input, is_dir, 
                 files, is_path, based_on, pattern, replace, append,
                 datestamp, datestamp_append, in_dir,
                 is_parameter, is_list, create=True, default_output=False,
                 foreach_dep=None):
        self.id = id
        self.path = path
        self._is_file = is_file
        self.is_temp = is_temp
        self.is_input = is_input
        self._is_dir = is_dir
        self.is_path = is_path
        self.based_on = based_on
        self.pattern = pattern
        self.replace = replace
        self.append = append
        self.datestamp = datestamp
        self.datestamp_append = datestamp_append
        self.in_dir = in_dir
        self.is_parameter = is_parameter
        self.is_list = is_list
        self.create = create
        self._is_fixed_up = False
        self.creator_job = None
        self.consumer_jobs = []
        self.foreach_dep = foreach_dep

        if self.id in files:
            # We've already seen this file ID.
            # Make sure they're compatible
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
    # to properly known when we can remove our temp files at the end 
    # of a run.
    def add_consumer_job(self, j):
        self.consumer_jobs.append(j)

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

        # What kind of file...
        is_file = t == 'file'
        is_dir = t == 'dir'
        is_list = t == 'filelist'

        # Init some variables.
        path_is_path = False
        path = None
        based_on = None
        pattern = None
        replace = None
        append = None
        datestamp = None
        datestamp_append = False
        is_parameter = False
        default_output = False
        foreach_dep = None


        # What kind of file?
        is_temp = False
        if 'temp' in att:
            is_temp = att['temp'].upper() == 'TRUE'

        # Input?
        is_input = False
        if 'input' in att:
            is_input = att['input'].upper() == 'TRUE'

        # Default output?
        if 'default_output' in att:
            default_output = att['default_output'].upper() == 'TRUE'
            assert 'in_dir' not in att, ('Must not combine default_output and '
                                         'in_dir attributes.')


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

        if is_list and 'pattern' in att:
            pattern = att['pattern']
            if 'foreach_id' in att:
                foreach_dep = att['foreach_id']

        if 'foreach_id' in att:
            assert is_list, ('foreach_id attribute can only be used with '
                             'a filelist tag.')

        if 'based_on' in att:
            assert (not path), (
                'Must not have based_on and path or parameter attributes.')
            based_on = att['based_on']

            if 'pattern' in att:
                pattern = att['pattern']
                if not 'replace' in att:
                    print >> sys.stderrr, 'pattern specified without replace.'
                    print >> sys.stderr, att
                    sys.exit(1)
                replace = att['replace']

            if 'datestamp_append' in att or 'datestamp_prepend' in att:
                if pattern or replace:
                    print >> sys.stderr, ('datestamp is incompatible with '
                                          'pattern and replace.')
                    print >> sys.stderr, att
                    sys.exit(1)
                if 'datestamp_append' in att:
                    datestamp = att['datestamp_append']
                    datestamp_append = True
                else:
                    datestamp = att['datestamp_prepend']

            if 'append' in att:
                if pattern or replace or datestamp:
                    print >> sys.stderr, ('append is incompatible with '
                                          'datestamp, pattern and replace.')
                    print >> sys.stderr, att
                    sys.exit(1)
                append = att['append']


        if is_list and not (pattern and in_dir):
            print >> sys.stderr, 'filelist requires in_dir and pattern.'
            print >> sys.stderr, att
            sys.exit(1)

        #if is_temp and not path:
        #    print >> sys.stderr, "temp", id, "has no path"

        PipelineFile(
            id, path, is_file, is_temp, is_input, is_dir, files, 
            path_is_path, based_on, pattern, replace, append, 
            datestamp, datestamp_append, in_dir,
            is_parameter, is_list, create, default_output, foreach_dep)

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
        return 'File:{0} p:{1} iP:{2} iI:{3} it:{4} iD:{5} BO:{6} Rep:{7} Pat:{8} Ap:{9} DS:{10} DSA:{11} inD:{12}'.format(
            self.id, self.path, self.is_path, self.is_input,
            self.is_temp, self._is_dir, self.based_on, self.replace,
            self.pattern, self.append, self.datestamp, self.datestamp_append,
            self.in_dir)

    def set_output_dir(self):
        # Register at most one output directory
        if PipelineFile.output_dir:
            sys.stderr.write('ERROR: only one directory can be marked as the default output directory\n')
            sys.exit(1)
        PipelineFile.output_dir = self

    @staticmethod
    def get_output_dir():
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

        circularity = []

        # First off, fix up the output dir if we have one; others
        # may depend on it.
        if not PipelineFile.output_dir:
            # there is no output_dir
            # create a default one in our current working directory
            PipelineFile.output_dir = PipelineFile('default_output', ".", False,
                                                   True, False, True, files,
                                                   True, None, None, None, None,
                                                   None, None, None, False,
                                                   False, create=True,
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
            print >> sys.stderr, '\nFile dependency cycle detected.'
            for f in circularity:
                print >> sys.stderr, f
            print >> sys.stderr, self
            sys.exit(1)

        #print >> sys.stderr, ('  ' * len(circularity)) + 'Fixing up' + str(self)
        circularity.append(self)

        self.parameter_to_path()

        if self is PipelineFile.output_dir:
            if PL.directory_version == 2:
                stamp_dir = "{0}-{1}".format(datetime.datetime.now().strftime('%Y%m%d_%H%M%S'), os.getpid())
                self.path = os.path.join(self.path, stamp_dir)
            utilities.make_sure_path_exists(self.path)
        else:
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
        if self.is_list:
            self.in_dir = os.path.abspath(files[self.in_dir].path)
        else:
            path = self.path
            if (os.path.split(path)[0] == '' and
                (not self.is_input) and
                self != PipelineFile.output_dir and
                (PipelineFile.output_dir is None or PipelineFile.output_dir._is_fixed_up)):
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
                print >> sys.stderr, ('Too few parameters...\n'
                    '    len(params): ' +
                    str(len(params)) + '\n    File: ' + self.id + 
                    '\n    referenced parameter: ' + str(self.path))
                sys.exit(1)
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
        bof = files[bo]
        bof.fix_up_file(files, circularity)

        # strip out any path before using the re, in case the replacement
        # uses the leading string twice.
        original_path = os.path.basename(bof.path)

        if self.append:
            self.path = original_path + self.append
        elif self.datestamp:
            ds = datetime.datetime.now().strftime(self.datestamp)
            if self.datestamp_append:
                self.path = original_path + ds
            else:
                self.path = ds + original_path
        else: # replace
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
            # Apply the containing directory to the path...
            fn = os.path.split(self.path)[1]
            self.path = os.path.join(dir, fn)
            self.in_dir = None
