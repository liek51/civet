
import xml.etree.ElementTree as ET

from pipeline_tool import *
import civet_exceptions


class Step(object):
    validTags = [
        'tool']

    def __init__(self, e, files):
        # Every step requires a name.
        if 'name' not in e.attrib or len(e.attrib) != 1:
            msg = ("Step must have (only) a name attribute. Tag had these "
                   "attributes: '{}'".format(", ".join(e.attrib.keys())))
            raise civet_exceptions.ParseError(msg)

        self.name = e.attrib['name'].replace(' ', '_')
        self.tools = []
        self.code = "S"
        for child in e:
            t = child.tag
            if t not in Step.validTags:
                msg = ("Illegal tag in step '{}': \n"
                       "{}".format(self.name, ET.tostring(child)))
                raise civet_exceptions.ParseError(msg)
            self.tools.append(PipelineTool(child, files))

    def submit(self, name_prefix):
        invocation = 0
        job_ids = []
        for tool in self.tools:
            invocation += 1
            name = '{0}_{1}_T{2}'.format(name_prefix, self.name, invocation)
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

