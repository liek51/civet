import os
import string

_DEFAULT_WALLTIME = "01:00:00"

class BatchJob(object):
    """
    a container for a batch job
    
    cmd     : a string containing the command (with arguments) to execute
    workdir : the job's working directory (default is current working directory)
    nodes   : number of nodes to request from resource manager
    ppn     : processors per node to request from resource manager
    walltime : walltime to request
    modules  : modules to load, pass a list to load multiple module files
    depends_on  : list of job IDs that this job has a dependency on
    name        : name for batch job
    stdout_path : path to final location for job's stdout spool (default = resource manager default)
    stderr_path : path to final location for job's stderr spool (default = resource manager default)
    files_to_check : files to validate before running command
    epilogue : optional post-job checks
    version_cmd : command to report version of the tool being used 
    """
    
    def __init__(self, cmd, workdir=None, nodes=1, ppn=1, 
                 walltime=_DEFAULT_WALLTIME, modules=[], depends_on=[], 
                 name=None, stdout_path=None, stderr_path="/dev/null", files_to_check=None, 
                 epilogue=None, version_cmd=None, error_strings=None):
        self.cmd = cmd
        self.ppn = ppn
        self.nodes = nodes
        self.modules = modules
        self.depends_on = depends_on
        self.stdout_path = stdout_path
        self.stderr_path = stderr_path
        self.workdir = workdir
        self.walltime = walltime
        self.name = name
        self.files_to_check = files_to_check
        self.epilogue = epilogue
        self.version_cmd = version_cmd
        self.error_strings = error_strings
        
    
    # setter for workdir, sets to the current working directory if a directory is 
    # not passed   
    def set_workdir(self, dir):
        if dir:
            self._workdir = os.path.abspath(dir)
        else:
            self._workdir = os.getcwd()
    
    def get_workdir(self):
        return self._workdir
    
    workdir = property(get_workdir, set_workdir)
    
    # setter for the job name, throw an exception if the name passed has invalid
    # characters
    def set_name(self, name):
        if name:
            chars = []
            for c in name:
                chars.append(c)
                if c not in string.digits + string.letters + "_-.":
                    raise ValueError("Invalid job name: '{0}'. "
                                     "Illegal character {1} {2}".format(name, c, chars))
        self._name = name
    
    def get_name(self):
        return self._name

    name = property(get_name, set_name)
