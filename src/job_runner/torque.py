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
import errno

import pbs
import PBSQuery



#TODO: make dependency type settable per job
DEFAULT_DEPEND_TYPE = "afterany"
DEFAULT_WALLTIME = "01:00:00"

def _make_sure_path_exists(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

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
                  will abort if prologue does not return 0. 
    
"""
class BatchJob(object):


    def __init__(self, cmd, workdir=None, nodes=1, ppn=1, 
                 walltime=DEFAULT_WALLTIME, modules=[], depends_on=[], 
                 name=None, stdout_path=None, stderr_path=None, prologue=None, 
                 epilogue=None):
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
        self.epilogue = epilogue
        
    
    # setter for workdir, sets to the current working directory if a directory is 
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
                    raise ValueError("Invalid job name: {0}".format(name))
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

    @property    
    def state(self):
        return self.status.get_value('job_state')[0]
      
    @property
    def error_path(self):
        return self.status['Error_Path'][0]
        
    @property
    def stdout_path(self):
        return self.status['Output_Path'][0]
            
    # retrun the exit_status attribute if it exists, if it does not exist
    # return None.  Should only exist if the job is in the "C" state.
    @property
    def exit_status(Self):
        if 'exit_status' in self.status:
            return self.status['exit_status'][0]
        else:
            return None
            

    #TODO: implement more properties (like for resources_used.walltime)


"""
   TorqueJobRunner is a class that encapsulates the functionality of submitting
   jobs to a TORQUE cluster.
   
   attributes
    held_jobs  : a list of job_id,server pairs that were submitted with a user hold
    submit_with_hold : if True any root job (job with no dependency) will be submitted with a user hold
    log_dir : directory to store log files, will be created if it doesn't exist
"""        
class TorqueJobRunner(object):
    
    # the template script, which will be customized for each job
    # $VAR will be subsituted before job submission, $$VAR will become $VAR after subsitution
    script_template = textwrap.dedent("""\
        #!/bin/bash
        
        #define some useful functions
        function abort_pipeline {
            # just iterate over all of the job ids in this pipeline and try to 
            # qdel them.  We don't care what the state is, or even if they still exit
            
            echo "Aborting pipeline" > $LOG_DIR/abort.log
            while read ID; do
                if [ "$$ID" != "$$PBS_JOBID" ]; then
                    echo "calling qdel on $$PBS_JOBID" >> $LOG_DIR/abort.log
                    qdel $$ID >> $LOG_DIR/abort.log 2>&1
                fi
            done < $LOG_DIR/id_list.txt
            echo "$$1" > $LOG_DIR/$${PBS_JOBID}-status.txt
            exit $$1
        }
        
        # sleep to overcome any issues with NFS file attribute cacheing
        sleep 60
        
        $MODULE_LOAD_CMDS
        
        cd $$PBS_O_WORKDIR
        
        
        #run any supplied pre-job checks
        $PROLOGUE

        #save return code for later use
        PROLOGUE_RETURN=$$?
        
        if [ $$PROLOGUE_RETURN -eq 0 ]; then 
            $CMD
            CMD_EXIT_STATUS=$$?
            if [ $$CMD_EXIT_STATUS -ne 0 ]; then
                echo "Command returned non-zero value.  abort pipeline" 1>&2
                abort_pipeline $$CMD_EXIT_STATUS
            fi
        else
            echo "Command not run, prologue returned non-zero value. Aborting pipeline!"  1>&2
            abort_pipeline $$PROLOGUE_RETURN            
        fi
        
        #run supplied post-job checks
        $EPILOGUE
        
        #save return code for later use
        EPILOGUE_RETURN=$$?
        
        if [ $$EPILOGUE_RETURN -ne 0 ]; then
            echo "Post job sanity check failed. Aborting pipeline!" 1>&2
            abort_pipeline $$EPILOGUE_RETURN
        else
            # no errors (prologue, command, and epilogue returned 0).  Write sucess status to file.
            echo "0" > $LOG_DIR/$${PBS_JOBID}-status.txt
    
        fi
    
    """)
  
    
    def __init__(self, log_dir="log", submit_with_hold=True, pbs_server=None):
        self.held_jobs = []
        self.submit_with_hold = submit_with_hold
        self._log_dir = log_dir      
        
        _make_sure_path_exists(log_dir)
          
        self._id_log = open(os.path.join(log_dir, "pipeline_batch_id_list.txt"), 'w')
        
        if pbs_server:
            self._server = pbs_server
        else:
            self._server = pbs.pbs_default()
  
    @property
    def log_dir(self):
        return self._log_dir
            
    """
      queue_job - queue a BatchJob.
      batch_job : description of the job to queue
      queue     : optional destination queue, uses server default if none is passed
    """
    def queue_job(self, batch_job, queue=None):
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
        elif self.submit_with_hold:
            job_attributes[pbs.ATTR_h] = 'u'
       
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
        connection = self._connect_to_server()

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
        
        if self.submit_with_hold and not batch_job.depends_on:
            self.held_jobs.append(id)
            
        self._id_log.write(id + "\n")
        self._id_log.flush()
        return id

        
    """
        query_job -- query server for status of job specified by id.  If no
        server is specified, the default will be used.  query_job will return
        None if the job does not exist on the server, otherwise it will return
        a JobStatus object.
    """    
    def query_job(self, id):
        pbsq = PBSQuery.PBSQuery(server=self._server)
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
    def delete_job(self, id):
        connection = self._connect_to_server()
        rval = pbs.pbs_deljob(connection, id, '' )        
        pbs.pbs_disconnect(connection)
        
        return rval
 
    
    """
        release_job - release a user hold from a held batch job
        id : job id to release (short form not allowed)
        server : optional hostname for pbs_server
        conn   : optinal connection to a pbs_server, if not passed release_job
                 will establish a new connection 
    """
    def release_job(self, id, connection=None):
        if connection:
            c = connection
        else:
            c = self._connect_to_server()
        
        rval = pbs.pbs_rlsjob(c, id, 'u', '')
        
        if not connection:
            pbs.pbs_disconnect(c)
        
        if rval == 0:
            self.held_jobs.remove(id)
        return rval
    
    
    """
        release_all - Release all jobs in self.held_jobs list reusing connections.  
    """
    def release_all(self):
        jobs = list(self.held_jobs)  #copy the list of held jobs to iterate over because release_job mutates self.held_jobs
        connection = self._connect_to_server()
        for id in jobs:
            self.release_job(id, connection)
        pbs.pbs_disconnect(connection)

    
    """
        generate a batch script based on our template and return as a string
        
        mainly intended to be used internally in PBSJobRunner, but it could be
        useful externally for debugging/logging the contents of a job script
        generated for a batch_job
    """    
    def generate_script(self, batch_job):
        tokens = {}
        
        tokens['CMD'] = batch_job.cmd
        
        #expand log_dir to absolute path because a job can have a different
        #working directory
        tokens['LOG_DIR'] = os.path.abspath(self.log_dir) 
        
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
            
        if batch_job.epilogue:
            tokens['EPILOGUE'] = batch_job.epilogue
        else:
            #force empty epilogue to return 0
            tokens['EPILOGUE'] = "true"
        
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
    def _connect_to_server(self):        
        connection = pbs.pbs_connect(self._server)
        
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

    job = BatchJob("hostname", walltime="00:02:00", name="test_job", 
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
        
    
    print "calling job_runner.release_all()"
    job_runner.release_all()
    status = job_runner.query_job(id)
    if status:
        print "Status of job is " + status.state
    
if __name__ == '__main__': 
    main() 
    