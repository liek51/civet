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

    def submit(self, depends_on, name_prefix):
        print '    Executing step', self.name
        invocation = 0
        for tool in self.tools:
            invocation += 1
            name = '{0}_{1}_Tool_{2}'.format(name_prefix, self.name,
                                             invocation)
            depends_on = tool.submit(depends_on, name)

