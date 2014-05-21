# Standard imports

# Pipeline components
from step import *
from pipeline_file import *


class ForEach():
    validTags = [
        'file',
        'related',
        'step' ]

    def __init__(self, e, pipeline_files, skip_validation=False):
        self.file = None
        self.relatedFiles = {}
        self.steps = []
        self.pipelineFiles = pipeline_files
        self.skip_validation = skip_validation
        att = e.attrib
        assert 'dir' in att, 'the foreach tag must specify a dir attribute'
        self.dir = att['dir']
        for child in e:
            t = child.tag
            assert t in ForEach.validTags, 'invalid tag in ForEach:' + t
            if t == 'step':
                #self.steps.append(Step(child, pipelineFiles, skip_validation))
                self.steps.append(child)
            elif t == 'file':
                assert not self.file, 'the foreach tag must contain exactly one file tag.'
                self.file = ForEachFile(child, self.pipelineFiles)
            else:
                ForEachRelated(child, self.relatedFiles, pipeline_files)
        assert self.file, 'the foreach tag must contain a file tag'
        assert self.file.id not in self.relatedFiles, "a foreach file's id must not be the same as a related file's id" + self.file.id

    def submit(self, name_prefix, foreach_iteration=None):
        # NOTE: foreach_iteration is a dummy parameter so that ForEach.submit()
        # has the same signature as Step.submit()

        # Get file list
        # for file in list
            # build related filenames
            # register file and related in pipeline files list
            # submit step(s)
            # clean up files from pipeline files list
        job_ids = []
        iteration = 0
        all_files = os.listdir(self.pipelineFiles[self.dir].path)
        for fn in all_files:
            if self.file.pattern.match(fn):
                print "Matched:", self.file.id, fn
                iteration += 1
                cleanups = []
                PipelineFile(self.file.id, fn, True, False, True, False,
                             self.pipelineFiles, True, None, None, None, None,
                             None, None, self.dir, False, False)
                cleanups.append(self.file.id)
                for relid in self.relatedFiles:
                    rel = self.relatedFiles[relid]
                    rfn = rel.pattern.sub(rel.replace, fn)
                    PipelineFile(rel.id, rfn, True, False, rel.inout, False,
                                 self.pipelineFiles, True, None, None, None,
                                 None, None, None, self.dir, False, False)
                    cleanups.append(rel.id)
                    print 'Related:', rel.id, rfn
                PipelineFile.fix_up_files(self.pipelineFiles)

                for s in self.steps:
                    step = Step(s, self.pipelineFiles, self.skip_validation)
                    print 'foreach submitting step:', step.name
                    for jid in step.submit(name_prefix, iteration):
                        job_ids.append(jid)
                for jid in cleanups:
                    del self.pipelineFiles[jid]
        return job_ids

    def check_files_exist(self):
        missing = []
        if not os.path.exists(self.pipelineFiles[self.dir].path):
            missing.append(self.pipelineFiles[self.dir].path)
        return missing

        
class ForEachFile():
    requiredAtts = [
        'id',
        'pattern' ]

    def __init__(self, e, pipeline_files):
        atts = e.attrib
        for a in atts:
            assert a in ForEachFile.requiredAtts, 'foreach file tag has unrecognized attribute: ' + a
        for a in ForEachFile.requiredAtts:
            assert a in atts, 'foreach file tag is missing required attribute: ' + a
        self.id = atts['id']
        assert self.id not in pipeline_files, "a foreach file's id must not be the same as a pipeline file id: " + self.id
        self.pattern = re.compile(atts['pattern'])
        

class ForEachRelated():
    requiredAtts = [
        'id',
        'input',
        'pattern',
        'replace' ]
    def __init__(self, e, related_files, pipeline_files):
        atts = e.attrib
        for a in atts:
            assert a in ForEachRelated.requiredAtts, 'foreach related tag has unrecognized attribute: ' + a
        for a in ForEachRelated.requiredAtts:
            assert a in atts, 'foreach related tag is missing required attribute: ' + a
        self.id = atts['id']
        assert self.id not in related_files, 'foreach tag contains duplicate related file id: ' + self.id
        assert self.id not in pipeline_files, "a foreach file's id must not be the same as a pipeline file id: " + self.id
        self.inout = atts['input']
        self.pattern = re.compile(atts['pattern'])
        self.replace = atts['replace']
        related_files[self.id] = self
