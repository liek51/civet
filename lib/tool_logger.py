class ToolLogger():
    me = None
    def __init__(self, logfile, instance):
        self.logfile = logfile
        self.instance = instance
        ToolLogger.me = self

    @staticmethod
    def getLogger():
        return me
        
    def log(msg):
        print >> self.logfile, self.instance + ':', msg

    def logList(msgList):
        print >> self.logfile, self.instance + ':', '\n' + self.instance + ': '.join(msgList)
