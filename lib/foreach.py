# Standard imports
import re

# Pipeline components
from step import *
from pipeline_file import *

class ForEach():
    validTags = [
        'file',
        'related',
        'step' ]
    def __init__(self, e, pipelineFiles, skip_validation=False):
        self.file = None
        self.relatedFiles = {}
        self.steps = []
        self.pipelineFiles = pipelineFiles
        att = e.attrib
        assert 'dir' in att, 'the foreach tag must specify a dir attribute'
        self.dir = att['dir']
        for child in e:
            t = child.tag
            assert t in ForEach.validTags, 'invalid tag in ForEach:' + t
            if t == 'step':
                self.steps.append(Step(child, pipelineFiles, skip_validation))
            elif t == 'file':
                assert not self.file, 'the foreach tag must contain exactly one file tag.'
                self.file = ForEachFile(child, self.pipelineFiles)
            else:
                ForEachRelated(child, self.relatedFiles, pipelineFiles)
        assert self.file, 'the foreach tag must contain a file tag'
        assert self.file.id not in self.relatedFiles, "a foreach file's id must not be the same as a related file's id" + self.id
        pass
        
    def execute(self):
        # Get file list
        # for file in list
            # build related filenames
            # register file and related in pipeline files list
            # execute step(s)
            # clean up files from pipeline files list
        allFiles = os.listdir(self.pipelineFiles[self.dir].path)
        for fn in allFiles:
            if self.file.pattern.match(fn):
                print "Matched:", self.file.id, fn
                cleanups = []
                PipelineFile.fromFilename(self.file.id, os.path.join(self.dir, fn), True, self.pipelineFiles)
                cleanups.append(self.file.id)
                for relid in self.relatedFiles:
                    rel = self.relatedFiles[relid]
                    rfn = rel.pattern.sub(rel.replace, fn)
                    PipelineFile.fromFilename(rel.id, rfn, rel.inout, self.pipelineFiles)
                    cleanups.append(rel.id)
                    print 'Related:', rel.id, rfn
                print self.steps
                for step in self.steps:
                    print 'foreach executing step:', step.name
                    step.execute()
                for id in cleanups:
                    del self.pipelineFiles[id]
        pass
        
class ForEachFile():
    requiredAtts = [
        'id',
        'pattern' ]

    def __init__(self, e, pipelineFiles):
        atts = e.attrib
        for a in atts:
            assert a in ForEachFile.requiredAtts, 'foreach file tag has unrecognized attribute: ' + a
        for a in ForEachFile.requiredAtts:
            assert a in atts, 'foreach file tag is missing required attribute: ' + a
        self.id = atts['id']
        assert self.id not in pipelineFiles, "a foreach file's id must not be the same as a pipeline file id: " + self.id
        self.pattern = re.compile(atts['pattern'])
        


class ForEachRelated():
    requiredAtts = [
        'id',
        'inout',
        'pattern',
        'replace' ]
    def __init__(self, e, relatedFiles, pipelineFiles):
        atts = e.attrib
        for a in atts:
            assert a in ForEachRelated.requiredAtts, 'foreach related tag has unrecognized attribute: ' + a
        for a in ForEachRelated.requiredAtts:
            assert a in atts, 'foreach related tag is missing required attribute: ' + a
        self.id = atts['id']
        assert self.id not in relatedFiles, 'foreach tag contains duplicate related file id: ' + self.id
        assert self.id not in pipelineFiles, "a foreach file's id must not be the same as a pipeline file id: " + self.id
        self.inout = atts['inout']
        self.pattern = re.compile(atts['pattern'])
        self.replace = atts['replace']
        relatedFiles[self.id] = self
