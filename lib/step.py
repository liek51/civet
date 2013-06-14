from pipeline_tool import *

class Step():
    validTags = [
        'tool' ]
    def __init__(self, e, files):
        # Every step requires a name.
        assert len(e.attrib) == 1, "Step must have (only) a name attribute"
        self.name = e.attrib['name']
        self.tools = []
        for child in e:
            t = child.tag
            # print 'Step child:', t, child.attrib
            assert t in Step.validTags, 'Illegal tag in step: ' + t
            self.tools.append(PipelineTool(child, files))

    def submit(self, name_prefix):
        invocation = 0
        job_ids = []
        for tool in self.tools:
            invocation += 1
            name = '{0}_{1}_T{2}'.format(name_prefix, self.name,
                                             invocation)
            job_id = tool.submit(name)
            job_ids.append(job_id)
        return job_ids
            
    def collect_files_to_validate(self):
        fns = []
        for tool in self.tools:
            tfns = tool.collect_files_to_validate()
            for fn in tfns:
                if fn not in fns:
                    fns.append(fn)
        return fns

    def collect_version_commands(self):
        vcs = []
        for tool in self.tools:
            tvcs = tool.collect_version_commands()
            for vc in tvcs:
                if vc not in vcs:
                    vcs.append(vc)
        return vcs

        def check_files_exist(self):
        missing = []
        for tool in self.tools:
            tmissing = tool.check_files_exist()
            for fn in tmissing:
                if fn not in missing:
                    missing.append(fn)
        return missing
