from tool import *

class PipelineTool():
    def __init__(self, e, files):
        att = e.attrib
        input = []
        output = []
        # Every tool requires a name and a description, which is 
        # the path to the tool's XML file.
        self.name = att['name']
        self.description = att['description']
        if 'input' in att:
            input = att['input'].split(',')
            for n in range(len(input)):
                input[n] = input[n].strip()
        if 'output' in att:
            output = att['output'].split(',')
            for n in range(len(output)):
                output[n] = output[n].strip()

        self.tool = Tool(self.description, input, output, files)
        
    def submit(self, depends_on, name_prefix):
        return self.tool.submit(depends_on, name_prefix)
