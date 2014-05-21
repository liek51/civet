import os
import string
import re

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
    version_cmds : list of command lines to report version of the executables being used
    error_strings: A list of strings Civet will search for in the job's stderr.
                   If the string is found, the job is considered to have failed.
    mail_option  : parameter to pass to resource manager's mail options for the job
    emai         :  email address to send resource manager notification emails
    files_to_test: list of file paths to check for before running command. if 
                   the file test returns true, the job will exit with success 
    file_test_logic : logic used to join file tests geneerated.  Can be "AND" or "OR"
    mem     : batch job mem attribute (in GB)
    """

    def __init__(self, cmd, workdir=None, nodes=1, ppn=1,
                 walltime=_DEFAULT_WALLTIME, modules=[], depends_on=[],
                 name=None, stdout_path=None, stderr_path="/dev/null",
                 files_to_check=None,
                 epilogue=None, version_cmds=None, error_strings=None,
                 mail_option="n", email=None, files_to_test=[],
                 file_test_logic="AND", mem=None):
        self.cmd = cmd
        self.ppn = ppn
        self.nodes = nodes
        self.modules = modules
        self.depends_on = depends_on
        self.stdout_path = stdout_path
        self.stderr_path = stderr_path
        self._mail_option = mail_option
        self.email = email
        self.workdir = workdir
        self.walltime = walltime
        self.name = name
        self.files_to_check = files_to_check
        self.epilogue = epilogue
        self.version_cmds = version_cmds
        self.error_strings = error_strings
        self.files_to_test = files_to_test
        self.file_test_logic = file_test_logic
        self.mem = mem

    # setter for workdir, sets to the current working directory if a directory
    # is not passed
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
                                     "Illegal character {1} {2}".format(name, c,
                                                                        chars))
        self._name = name

    def get_name(self):
        return self._name

    name = property(get_name, set_name)

    @property
    def mail_option(self):
        return self._mail_option

    @mail_option.setter
    def mail_option(self, val):
        if val and re.match(r'[^abe]', val) and val != 'n':
            raise ValueError("Invalid mail_option. Must be n|{abe}|None")
        self._mail_option = val

    @property
    def file_test_logic(self):
        return self._file_test_logic

    @file_test_logic.setter
    def file_test_logic(self, val):
        if not val or val.upper() not in ["AND", "OR"]:
            raise ValueError(
                'Invalid exit_test_bool option ({0}). Must be "AND" or "OR"'.format(
                    val))
        self._file_test_logic = val.upper()

    @property
    def mem(self):
        return self._mem

    @mem.setter
    def mem(self, m):
        if m is not None:
            if not m.isdigit():
                raise ValueError(
                    'Invalid mem request.  Must be a positive integer.')

            # Civet requires tools request memory in gigabytes.  We add the 
            # correct Torque size suffix
            self._mem = m + 'gb'
        else:
            self._mem = None