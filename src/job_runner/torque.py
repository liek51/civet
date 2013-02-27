#!/usr/bin/env python

"""
job_runner/torque.py 

provide functionality for queueing and querying jobs on a TORQUE cluster

"""


import textwrap
import os
import socket
from tempfile import mkstemp
import string

import pbs
import PBSQuery



#TODO: make dependency type settable per job
DEFAULT_DEPEND_TYPE = "afterany"
DEFAULT_WALLTIME = "01:00:00"



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
    prologue    : path to optional job prologue script. Will run before cmd, job 
               will abort if prologue does not return 0. Note that this parameter 
               allows the flexiblity of calling an external prologue scrip or 
               multiple lines of bash code can be passed in for the prologue code
               to be self-contained within the batch script. If passing in bash
               code directly, the caller must make sure the last command executed
               returns 0 on success of the entire prologue block
    
"""
class BatchJob(object):


    def __init__(self, cmd, workdir=None, nodes=1, ppn=1, 
                 walltime=DEFAULT_WALLTIME, modules=[], depends_on=[], 
                 name=None, stdout_path=None, stderr_path=None, prologue=None):
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
        self.prologue = prologue
        
    
    # setter for workdir, sets to the current working directory a directory is 
    # not passed   
    def set_workdir(self, dir):
        if dir:
            self._workdir = workdir
        else:
            self._workdir = os.getcwd()
    
    def get_workdir(self):
        return self._workdir
    
    workdir = property(get_workdir, set_workdir)
    
    # setter for the job name, throw an exception if the name passed has invalid
    # characters
    def set_name(self, name):
        if name:
            for c in name:
                if c not in string.digits + string.letters + "_-.":
                    raise Exception("Invalid job name: {0}".format(name))
        self._name = name
    
    def get_name(self):
        return self._name

    name = property(get_name, set_name)


"""
    JobStatus - a class that holds the dictionary-like object returned by 
    PBSQuery.getjob, and knows how to parse that output to get the information 
    we are interested in.  This is what PBSJobRunner.query_job() returns.
    status : TORQUE job status as returned by PBSQueryJob
"""
class JobStatus(object):

    def __init__(self, status):
        self.status = status

        
    def get_state(self):
        return self.status.get_value('job_state')[0]
    
    state = property(get_state)
    
    def get_error_path(self):
        return self.status['Error_Path'][0]
        
    error_path = property(get_error_path)
    
    def get_stdout_path(self):
        return self.status['Output_Path'][0]
        
    stdout_path = property(get_stdout_path)
    
    # retrun the exit_status attribute if it exists, if it does not exist
    # return None.  Should only exist if the job is in the "C" state.
    def get_exit_status(Self):
        if 'exit_status' in self.status:
            return self.status['exit_status'][0]
        else:
            return None
            
    exit_status = property(get_exit_status)

    #TODO: implement more getters (like for resources_used.walltime)


"""
   TorqueJobRunner is a class that encapsulates the functionality of submitting
   jobs to a TORQUE cluster
"""        
class TorqueJobRunner(object):
    
    #the template script, which will be customized for each job
    script_template = textwrap.dedent("""\
        #!/bin/bash
        
        # sleep to overcome any issues with NFS file attribute cacheing
        sleep 60
        
        $MODULE_LOAD_CMDS
        
        cd $$PBS_O_WORKDIR
        
        
        #run any user supplied checks
        $PROLOGUE
        #end job prologue
        #save return code for later use
        PROLOGUE_RETURN=$$?
        
        if [ $$PROLOGUE_RETURN -eq 0 ]; then 
            $CMD
            if [ $$? -ne 0 ]; then
                #TODO call cleanup script
                echo "command returned non-zero value.  abort pipeline"
            fi
        else
            #TODO change this to write to log file
            echo "command not run, prologue returned non-zero value"
            
            #TODO call cleanup script
        fi
    
    """)
  
    
    def __init__(self, log_dir="log", submit_with_hold=True):
        self.held_jobs = []
        self.submit_with_hold = submit_with_hold
  
        
    """
      queue_job - queue a BatchJob.
      batch_job : description of the job to queue
      queue     : optional destination queue, uses server default if none is passed
      server    : optional destination server, used default server if none passed
    """
    def queue_job(self, batch_job, queue=None, server=None):
        job_attributes = {}
        job_resources = {}
        
        job_resources['nodes'] = "{0}:ppn={1}".format(batch_job.nodes, batch_job.ppn)
        job_resources['walltime'] = batch_job.walltime
        
        job_attributes[pbs.ATTR_v] = self._generate_env(batch_job)
        
        if batch_job.name:
            job_attributes[pbs.ATTR_N] = batch_job.name
        
        if batch_job.stdout_path:
            job_attributes[pbs.ATTR_o] = batch_job.stdout_path
            
        if batch_job.stderr_path:
            job_attributes[pbs.ATTR_e] = batch_job.stderr_path
            
        if batch_job.depends_on:
            job_attributes[pbs.ATTR_depend] = self._dependency_string(batch_job)
       
        pbs_attrs = pbs.new_attropl(len(job_attributes) + len(job_resources))
        
        # populate pbs_attrs
        attr_idx = 0
        for resource,val in job_resources.iteritems():
            pbs_attrs[attr_idx].name = pbs.ATTR_l
            pbs_attrs[attr_idx].resource = resource
            pbs_attrs[attr_idx].value = val
            attr_idx += 1
            
        for attribute,val in job_attributes.iteritems():
            pbs_attrs[attr_idx].name = attribute
            pbs_attrs[attr_idx].value = val
            attr_idx += 1
            
        # we've initialized pbs_attrs with all the attributes we need to set
        # now we can connect to the server and submit the job
        connection = self._connect_to_server(server)

        #connected to pbs_server
        
        #write batch script to temp file, will remove after pbs_submit
        fd, tmp_filename = mkstemp(suffix=".sh")
        os.write(fd, self.generate_script(batch_job))
        os.close(fd)
            
        #submit job
        id = pbs.pbs_submit(connection, pbs_attrs, tmp_filename, queue, None)
        
        #a copy of the script was sent to the pbs_server, we can delete it  
        os.remove(tmp_filename)
       
        #check to see if the job was submitted sucessfully. 
        if not id:
            e, e_msg = pbs.error()
            pbs.pbs_disconnect(connection)
            # the batch system returned an error, throw exception 
            raise Exception("Error submitting job.  {0}: {1}".format(e, e_msg))
       
        pbs.pbs_disconnect(connection)
                
        return id

        
    """
        query_job -- query server for status of job specified by id.  If no
        server is specified, the default will be used.  query_job will return
        None if the job does not exist on the server, otherwise it will return
        a JobStatus object.
    """    
    def query_job(self, id, server=None):
        pbsq = PBSQuery.PBSQuery(server=server)
        job_status =  pbsq.getjob(id)
        
        # check to see if the job existed.  this is kind of lame, but we can't
        # just do "if job_status:" because PBSQuery.getjob returns an empty 
        # dictionary if the job is not found, but it returns some other object
        # that acts like a dictionary but does not have a __nonzero__ attribute
        if 'Job_Name' in job_status:
            return JobStatus(job_status)
        else:
            return None

    """
        call pbs_deljob on a job id, return pbs_deljob return value (0 on success)
    """
    def delete_job(self, id, server=None):
        connection = self._connect_to_server(server)
        rval = pbs.pbs_deljob(connection, id, '' )
        pbs.pbs_disconnect(connection)
        
        return rval
 
    
    """
        generate a batch script based on our template and return as a string
        
        mainly intended to be used internally in PBSJobRunner, but it could be
        useful externally for debugging/logging the contents of a job script
        generated for a batch_job
    """    
    def generate_script(self, batch_job):
        tokens = {}
        
        tokens['CMD'] = batch_job.cmd
        
        tokens['MODULE_LOAD_CMDS'] = ""
        
        # I want this to work if batch_job.modules is a string containing the name
        # of a single modulefile or a list of modulefiles    
        if batch_job.modules:
            if isinstance(batch_job.modules, basestring):  #basestring = str in Python3
                tokens['MODULE_LOAD_CMDS'] = "module load " + batch_job.modules
            else:
                for module in batch_job.modules:
                    tokens['MODULE_LOAD_CMDS'] = "{0}module load {1}\n".format(tokens['MODULE_LOAD_CMDS'], module)
            
        
        if batch_job.prologue:
            tokens['PROLOGUE'] = batch_job.prologue
        else:
            #force "empty" prologue to return 0
            tokens['PROLOGUE'] = "true"
        
        return string.Template(self.script_template).substitute(tokens)


    """
    strerror - look up the string associated with a given pbs error code
    NOTE: Until the pbs_python developers update their source, most of these
    strings are out of sync with the integer error codes
    """
    def strerror(self, e):
        return pbs.errors_txt[e]


    """
        open a connection to a pbs_server.  if no server specified, connect
        to the default server.  Will raise an exception if a connection can 
        not be established.
    """    
    def _connect_to_server(self, server=None):        
        if not server:
            server = pbs.pbs_default()
        
        connection = pbs.pbs_connect(server)
        
        if connection <= 0:
            e, e_msg = pbs.error()
            # the batch system returned an error, throw exception 
            raise Exception("Error connecting to pbs_server.  {0}: {1}".format(e, e_msg))
            
        return connection
        
    
    """
        generate_env - generate a basic environment string to send along with the 
        job. This can define any environment variables we want defined in the job's
        environment when it executes. We define some of the typical PBS_O_* variables
    """
    def _generate_env(self, batch_job):
    
        # most of our scripts start with "cd $PBS_O_WORKDIR", so make sure we set it
        env = "PBS_O_WORKDIR={0}".format(batch_job.workdir)
        
        # define some of the other typical PBS_O_* environment variables
        # PBS_O_HOST is used to set default stdout/stderr paths, the rest probably
        # aren't necessary
        
        env = "".join([env, ",PBS_O_HOST=", socket.getfqdn()])
        if os.environ['PATH']:
            env = "".join([env, ",PBS_O_PATH=", os.environ['PATH']])
        if os.environ['HOME']:
            env = "".join([env, ",PBS_O_HOME=", os.environ['HOME']])
        if os.environ['LOGNAME']:
            env = "".join([env, ",PBS_O_LOGNAME=", os.environ['LOGNAME']])
        
        return env

        
    """
        generate a TORQUE style dependency string for a batch job, to be passed
        to the ATTR_depend job attribute, will return empty string if 
        batch_job.depends_on is empty
    """
    def _dependency_string(self, batch_job):
    
        # we want this to work if batch_job.depends_on is a string containing 
        # the ID of a single job or a list of job ID strings
        if not batch_job.depends_on:
            return ""
        elif isinstance(batch_job.depends_on, basestring):  #basestring = str in Python3
            #handle string case
            return "{0}:{1}".format(DEFAULT_DEPEND_TYPE, batch_job.depends_on)
        else:
            #not a string, assume list of job ids to join
            return "{0}:{1}".format(DEFAULT_DEPEND_TYPE, ':'.join(batch_job.depends_on))


"""
   simple main function that tests some functionality if we run this script
   directly rather than import it
"""
def main():
    job_runner = TorqueJobRunner()

    job = BatchJob("sleep 600", walltime="00:11:00", name="test_job", 
                   modules=["python"])

    
    print "submitting job with the following script:"
    print "---------------------------------------------------"
    print job_runner.generate_script(job)
    print "---------------------------------------------------"
    id = job_runner.queue_job(job)
    
    print id
    
    status = job_runner.query_job(id)
    if status:
        print "Status of job is " + status.state
    
    
    
if __name__ == '__main__': 
    main() 
    
