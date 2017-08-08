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

from tool import *


class PipelineTool(object):
    def __init__(self, e, files):
        att = e.attrib
        input = []
        output = []
        walltime = None
        tool_config_prefix = None

        # Every tool requires a name and a description, which is
        # the path to the tool's XML file.
        self.name = att['name'].replace(' ', '_')
        self.description = att['description']

        if 'input' in att:
            input = att['input'].split(',')
            for n in range(len(input)):
                input[n] = input[n].strip()

        if 'output' in att:
            output = att['output'].split(',')
            for n in range(len(output)):
                output[n] = output[n].strip()

        if 'walltime' in att:
            walltime = att['walltime']

        if 'tool_config_prefix' in att:
            tool_config_prefix = att['tool_config_prefix']

        self.tool = Tool(self.description, input, output, files, self.name, walltime, tool_config_prefix)
        
    def submit(self, name_prefix):
        return self.tool.submit(name_prefix)

    def create_task(self, task_id, managed_batch=False):
        return self.tool.create_task(task_id, managed_batch=managed_batch)

    def collect_version_commands(self):
        return self.tool.collect_version_commands()

    def collect_files_to_validate(self):
        return self.tool.collect_files_to_validate()

    def check_files_exist(self):
        return self.tool.check_files_exist()
