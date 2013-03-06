class PipelineFile():
    """
    Initialize ourselves from an XML tag that represents some king
    of file.  Arguments: XML element representing the file, and a
    hash of processed files
    """
    me = None
    
    validFileTags = [
        'input',
        'inputdir',
        'output',
        'outputdir',
        'tempfile' ]
        
    def __init__(self, id, path, type, isFile, isTemp, isInput, isDir, files, isPath):
        self._id = id
        self._path = path
        self._filetype = type
        self._isFile = isFile
        self._isTemp = isTemp
        self._isInput = isInput
        self._isDir = isDir
        self._isPath = isPath
        self._logDir = None
        
        if self._id in files:
            # We've already seen this file ID.
            # Make sure they're compatible
            self.compatible(files[self._id])
        else:
            # Register this file in the files/options namespace
            files[self._id] = self
            
    @staticmethod
    def get_instance():
        return me

    @staticmethod        
    def parse_XML(e, files):
        print 'In PFile.parseXML:', e.tag, e.attrib
        t = e.tag
        att = e.attrib
        # Make sure that we have the right kind of tag.
        assert t in PipelineFile.validFileTags, 'Illegal pipeline file tag: "' + t + '"'

        # id attribute is required, make sure we're not already in, or,
        # if we are, that we have the same attributes.
        id = att['id']

        # We are a file...
        isFile = True

        # Init some variables.
        pathIsPath = False
        path = None
        fileType = None
        
        # What kind of file?
        isTemp = e.tag == 'tempfile'
        isInput = e.tag == 'input' or e.tag == 'inputdir'
        isDir = e.tag == 'outputdir' or e.tag == 'inputdir'
        
        # All except directories require a type 
        if not isDir:
            fileType = e.attrib['type']

        # All except temp files need either a filespec or parameter
        if not isTemp:
            if 'filespec' in att:
                path = att['filespec']
                pathIsPath = True
            if 'parameter' in att:
                assert not path, 'Must not have both filespec and parameter attributes.'
                path = int(att['parameter'])

        PipelineFile(id, path, fileType, isFile, isTemp, isInput, isDir, files, pathIsPath)

    def is_output_dir(self):
        return self._isDir and not self._isInput

    @property
    def path(self):
        return self._path

    @staticmethod
    def from_filename(id, name, isInput, files):
        PipelineFile(id, name, None, True, False, isInput, False, files, True)

    def compatible(self, o):
        # We have a file whose ID we've already seen. 
        # Make sure they're compatible.
        
        # Second instance must not have a path, be a tempfile, or a
        # directory.
        assert not self._path
        assert not self._isTemp
        assert not self._isDir
            
        # Same type of file 
        assert self._fileType == o.fileType

    def __repr__(self):
        return self.__str__()
    def __str__(self):
        return 'File: %s\tp: %s\tt: %s\tiP: %r\tiI: %r\tit: %r\tiD: %r' % (self._id, self._path, self._fileType, self._pathIsPath, self._isInput, self._isTemp, self._isDir)
