# Copyright 2016 The Jackson Laboratory
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import xml.etree.ElementTree as ET

from pipeline_tool import *
from civet_exceptions import *

class Step(object):
    validTags = [
        'tool']

    def __init__(self, e, files):
        # Every step requires a name.
        if 'name' not in e.attrib or len(e.attrib) != 1:
            msg = ("Step must have (only) a name attribute. Tag had these "
                   "attributes: '{}'\n{}".format(", ".join(e.attrib.keys()), ET.tostring(e)))
            raise ParseError(msg)

        self.name = e.attrib['name'].replace(' ', '_')
        self.tools = []
        self.code = "S"
        for child in e:
            t = child.tag
            if t not in Step.validTags:
                msg = ("Illegal tag in step '{}': \n\n"
                       "{}\n\nValid Tags: '{}'".format(self.name,
                                                   ET.tostring(child).rstrip(),
                                                   ", ".join(Step.validTags)))
                raise ParseError(msg)
            self.tools.append(PipelineTool(child, files))

    def submit(self, name_prefix):
        invocation = 0
        job_ids = []
        for tool in self.tools:
            invocation += 1
            name = self.generate_name(name_prefix, invocation, tool.tool.name_from_pipeline)
            job_id = tool.submit(name)
            job_ids.append(job_id)
        return job_ids

    def create_tasks(self, name_prefix, execution_mode):
        invocation = 0
        tasks = []
        for tool in self.tools:
            invocation += 1
            name = self.generate_name(name_prefix, invocation, tool.tool.name_from_pipeline)
            tasks.append(tool.create_task(name, execution_mode))
        return tasks

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

    def generate_name(self, name_prefix, invocation, tool_name):
        return '{}_{}_T{}_{}'.format(name_prefix, self.name, invocation, tool_name)

