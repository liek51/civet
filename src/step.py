from pipeline_tool import *

class Step():
    validTags = [
        'tool' ]
    def __init__(self, e, files):
        print 'In step:', e.tag, e.attrib
        # Every step requires a name.
        assert len(e.attrib) == 1, "Step must have (only) a name attribute"
        self.name = e.attrib['name']
        self.tools = []
        for child in e:
            t = child.tag
            # print 'Step child:', t, child.attrib
            assert t in Step.validTags, 'Illegal tag in step: ' + t
            self.tools.append(PipelineTool(child, files))

    def execute(self):
        print '    Executing step', self.name
        for tool in self.tools:
            tool.execute()

