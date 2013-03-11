class PipelineFile():
    """
    Initialize ourselves from an XML tag that represents some king
    of file.  Arguments: XML element representing the file, and a
    hash of processed files
    """
    me = None
    
    validFileTags = [
        'file',
        'dir',
        'tempfile' ]
        
    def __init__(self, id, path, type, is_file, is_temp, is_input, is_dir, files, is_path):
        self.id = id
        self.path = path
        self._filetype = type
        self._is_file = is_file
        self.is_temp = is_temp
        self._is_input = is_input
        self._is_dir = is_dir
        self.is_path = is_path
        
        if self.id in files:
            # We've already seen this file ID.
            # Make sure they're compatible
            self.compatible(files[self.id])
        else:
            # Register this file in the files/options namespace
            files[self.id] = self

        

    @staticmethod
    def get_instance():
        return me

    @staticmethod        
    def parse_XML(e, files):
        t = e.tag
        att = e.attrib
        # Make sure that we have the right kind of tag.
        assert t in PipelineFile.validFileTags, 'Illegal pipeline file tag: "' + t + '"'

        # id attribute is required, make sure we're not already in, or,
        # if we are, that we have the same attributes.
        id = att['id']

        # We are a file...
        is_file = t == 'file' or t == 'tempfile'
        is_dir = t == 'dir'

        # Init some variables.
        path_is_path = False
        path = None
        fileType = None

        # What kind of file?
        is_temp = e.tag == 'tempfile'

        # Input?
        is_input = False
        if input in att:
            is_input = att['input'].upper() == 'TRUE'

        # All except directories require a type 
        if not is_dir:
            fileType = e.attrib['type']

        # All except temp files need either a filespec or parameter
        if not is_temp:
            if 'filespec' in att:
                path = att['filespec']
                path_is_path = True
            if 'parameter' in att:
                assert not path, 'Must not have both filespec and parameter attributes.'
                path = int(att['parameter'])

        PipelineFile(id, path, fileType, is_file, is_temp, is_input, is_dir, files, path_is_path)

    def is_output_dir(self):
        return self._is_dir and not self._is_input

    @staticmethod
    def from_filename(id, name, is_input, files):
        PipelineFile(id, name, None, True, False, is_input, False, files, True)

    def compatible(self, o):
        # We have a file whose ID we've already seen. 
        # Make sure they're compatible.
        
        # Second instance must not have a path, be a tempfile, or a
        # directory.
        assert not self.path
        assert not self.is_temp
        assert not self._is_dir
            
        # Same type of file 
        assert self._fileType == o.fileType

    def __repr__(self):
        return self.__str__()
    def __str__(self):
        return 'File: %s\tp: %s\tt: %s\tiP: %r\tiI: %r\tit: %r\tiD: %r' % (self.id, self.path, self._fileType, self.path_is_path, self._is_input, self.is_temp, self._is_dir)
