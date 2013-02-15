class PipelineFile():
    # Initialize ourselves from an XML tag that represents some king
    # of file.  Arguments: XML element representing the file, and a
    # hash of processed files
    validFileTags = [
        'input',
        'inputdir',
        'output',
        'outputdir',
        'tempfile' ]
        
    def __init__(self, id, path, type, isFile, isTemp, isInput, isDir, files, isPath):
        self.id = id
        self.path = path
        self.filetype = type
        self.isFile = isFile
        self.isTemp = isTemp
        self.isInput = isInput
        self.isDir = isDir
        self.isPath = isPath

        if self.id in files:
            # We've already seen this file ID.
            # Make sure they're compatible
            self.compatible(files[self.id])
        else:
            # Register this file in the files/options namespace
            files[self.id] = self


    @staticmethod        
    def parseXML(e, files):
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


    @staticmethod
    def fromFilename(id, name, isInput, files):
        PipelineFile(id, name, None, True, False, isInput, False, files, True)

    def compatible(self, o):
        # We have a file whose ID we've already seen. 
        # Make sure they're compatible.
        
        # Second instance must not have a path, be a tempfile, or a
        # directory.
        assert not self.path
        assert not self.isTemp
        assert not self.isDir
            
        # Same type of file 
        assert self.fileType == o.fileType

    def __repr__(self):
        return self.__str__()
    def __str__(self):
        return 'File: %s\tp: %s\tt: %s\tiP: %r\tiI: %r\tit: %r\tiD: %r' % (self.id, self.path, self.fileType, self.pathIsPath, self.isInput, self.isTemp, self.isDir)
        
