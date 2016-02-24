#
# Copyright (C) 2016  The Jackson Laboratory
#
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

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

