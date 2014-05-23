# Standard imports

# Pipeline components
from step import *
from pipeline_file import *


class ForEach():
    validTags = [
        'file',
        'related',
        'step',
        'id' ]

    # to prevent a user from flooding the system we impose a limit on the
    # maximum number of jobs that can be created by one foreach instance
    # perhaps someday we will have a more sophisticated Civet run time and
    # possibly have the ability to throttle job submission or bundle jobs
    # together
    MAX_JOBS = 256

    def __init__(self, e, pipeline_files):
        self.file = None
        self.relatedFiles = {}
        self.steps = []
        self.pipelineFiles = pipeline_files
        self.code = "FE"
        att = e.attrib
        assert 'id' in att, 'the foreach tag must specify an id'
        self.id = att['id']
        assert 'dir' in att, 'the foreach tag must specify a dir attribute'
        self.dir = att['dir']
        for child in e:
            t = child.tag
            assert t in ForEach.validTags, 'invalid tag in ForEach:' + t
            if t == 'step':
                #self.steps.append(Step(child, pipelineFiles))
                self.steps.append(child)
            elif t == 'file':
                assert not self.file, 'the foreach tag must contain exactly one file tag.'
                self.file = ForEachFile(child, self.pipelineFiles)
            else:
                ForEachRelated(child, self.relatedFiles, pipeline_files)
        assert self.file, 'the foreach tag must contain a file tag'
        assert self.file.id not in self.relatedFiles, "a foreach file's id must not be the same as a related file's id" + self.file.id

    def submit(self, name_prefix):

        import pipeline_parse as PL

        # Get file list
        # for file in list
            # build related filenames
            # register file and related in pipeline files list
            # submit step(s)
            # clean up files from pipeline files list
        matched_files = []
        job_ids = []
        iteration = 0
        all_files = os.listdir(self.pipelineFiles[self.dir].path)
        for fn in all_files:
            if self.file.pattern.match(fn):
                matched_files.append(fn)

        # figure out if this foreach loop will exceed the limit
        total_jobs = 0
        for s in self.steps:
            for child in s:
                if child.tag == 'tool':
                    total_jobs += 1
        total_jobs *= len(matched_files)

        assert total_jobs <= ForEach.MAX_JOBS, 'foreach {0} jobs exceed limit (max = {1})'.format(total_jobs, ForEach.MAX_JOBS)

        for fn in matched_files:
            iteration += 1
            cleanups = []
            PipelineFile(self.file.id, fn, True, False, True, False,
                         self.pipelineFiles, True, None, None, None, None,
                         None, None, self.dir, False, False)
            cleanups.append(self.file.id)

            for relid in self.relatedFiles:
                rel = self.relatedFiles[relid]
                rfn = rel.pattern.sub(rel.replace, fn)
                if rel.is_input:
                    #we assume related input files are in the same dir
                    #as the foreach file it is related to
                    directory = self.dir
                elif rel.indir:
                    directory = rel.indir
                else:
                    #related file is an output file and indir not specified
                    #write it to the default output directory
                    directory = None
                PipelineFile(rel.id, rfn, True, False, rel.is_input, False,
                             self.pipelineFiles, True, None, None, None,
                             None, None, None, directory, False, False)
                cleanups.append(rel.id)
            PipelineFile.fix_up_files(self.pipelineFiles)

            step_iteration = 0
            for s in self.steps:
                step_iteration += 1
                step = Step(s, self.pipelineFiles)
                prefix = "{0}-{1}_S{2}".format(name_prefix, iteration, step_iteration)
                for jid in step.submit(prefix):
                    job_ids.append(jid)

            for jid in cleanups:
                del self.pipelineFiles[jid]

        #enqueue "barrier" job here
        barrier_job = BatchJob('echo "placeholder job used for synchronizing foreach jobs"',
                               workdir=PipelineFile.get_output_dir(),
                               depends_on=job_ids,
                               name="{0}_barrier".format(name_prefix),
                               walltime="00:02:00")
        try:
            job_id = PL.job_runner.queue_job(barrier_job)
        except Exception as e:
            sys.stderr.write(str(e) + '\n')
            sys.exit(PL.BATCH_ERROR)
        PL.all_batch_jobs.append(job_id)
        PL.foreach_barriers[self.id] = job_id
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
        'replace']
    optionalAtts = [
        'indir' ]

    def __init__(self, e, related_files, pipeline_files):
        atts = e.attrib
        for a in atts:
            assert a in ForEachRelated.requiredAtts or a in ForEachRelated.optionalAtts, 'foreach related tag has unrecognized attribute: ' + a
        for a in ForEachRelated.requiredAtts:
            assert a in atts, 'foreach related tag is missing required attribute: ' + a
        self.id = atts['id']
        assert self.id not in related_files, 'foreach tag contains duplicate related file id: ' + self.id
        assert self.id not in pipeline_files, "a foreach file's id must not be the same as a pipeline file id: " + self.id
        self.is_input = atts['input'].upper() == 'TRUE'
        self.pattern = re.compile(atts['pattern'])
        self.replace = atts['replace']

        if 'indir' in atts:
            self.indir = atts['indir']
        else:
            self.indir = None
        assert not (self.is_input and self.indir), 'foreach related input file must be in same directory as foreach file: ' + self.id
        related_files[self.id] = self
