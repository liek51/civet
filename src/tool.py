class Tool():
    def __init__(self, xmlfile):
        tool = ET.parse(xmlfile).getroot()

    def logVersion(self):
        pass
    def validate(self):
        # Determine whether this tool is unchanged from those that were certified for CLIA.
        pass
    def execute(self):
        # actually run the tool
        pass
    def getCommand(self):
        # return the command as a string
        pass
    def logCommand(self):
        # write the command to be executed to a log file.
        # Not sure this is the place to have this, or whether some place else
        # simply calls getCommand and writes it.
        pass
        
class ToolOption():
    def __init__(self, element):
        try:
            name = e.attrib['name']
            command_text = e.attrib['command_text']
            value = e.attrib['value']
        except:
            dumpElement(e, 0)
            return
        self.name = name
        self.isFile = False
        self.command_text = command_text
        self.value = value
        
        # We don't allow the same option name in a tool twice
        assert name not in FileOrOption.names
        FileOrOption.names[name] = self

    def __str__(self):
        return '\t'.join(['Option: ' + self.name, self.command_text, self.value])

