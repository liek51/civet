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

    def collect_version_commands(self):
        return self.tool.collect_version_commands()

    def collect_files_to_validate(self):
        return self.tool.collect_files_to_validate()

    def check_files_exist(self):
        return self.tool.check_files_exist()
