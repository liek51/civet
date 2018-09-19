#!/usr/bin/env python

# Copyright 2016 The Jackson Laboratory
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
job_runner/torque.py 

provide functionality for queueing and querying jobs on a TORQUE cluster

"""

from __future__ import print_function

import errno
import sys
import textwrap
import socket
import stat
import time
import tempfile

import pbs
import PBSQuery

#make sure we look in the parent directory for modules when running as a script
#so that we can find the utilities module
if __name__ == "__main__":
    sys.path.insert(0, "..")
    
from batch_job import *
import common
import civet_exceptions
import utilities
import version
import config

#TODO: make dependency type settable per job
_DEFAULT_DEPEND_TYPE = "afterok"

_SHELL_SCRIPT_DIR = "submitted_shell_scripts"

_error_strings = None

_MAX_RETRY = 4


def _connect_to_server(server=None):
    """
        open a connection to a pbs_server at hostname server, if server is None 
        then connect to the default server.
        
        This function is shared between JobManager and TorqueJobRunner
    """
    server_name = server if server else pbs.pbs_default()

    retry = 0
    connection = pbs.pbs_connect(server_name)

    while connection <= 0 and retry < _MAX_RETRY:
        retry += 1
        time.sleep(retry ** 2)
        connection = pbs.pbs_connect(server_name)
        
    if connection <= 0:
        e, e_msg = pbs.error()
        # the batch system returned an error, throw exception 
        raise Exception("Error connecting to pbs_server.  "
            "Torque error {0}: '{1}'".format(e, torque_strerror(e)))
        
    return connection


def torque_strerror(err):
    global _error_strings
    if not _error_strings:
        _error_strings = dict(line.strip().split('\t') for line in open(os.path.join(common.CIVET_HOME, "lib/job_runner/torque_errors.txt")))
            
    return _error_strings[str(err)]

        
class JobManager(object):
    """
        This class encapsulates the functionality for monitoring and controlling
        a Torque job.
    """


    #constants with the return codes for unknown job id and invalid state (complete)
    # these are valid for TORQUE 4.x,  pbs_python has outdated error 
    # codes -> error string mappings
    E_UNKNOWN = 15001
    E_STATE = 15018

    def __init__(self, pbs_server=None):
        self.pbsq = None
        retry = 0
        cached_exception = None
        while not self.pbsq and retry < _MAX_RETRY:
            try:
                self.pbsq = PBSQuery.PBSQuery(server=pbs_server)
            except PBSQuery.PBSError as e:
                cached_exception = e
                retry += 1
                time.sleep(retry ** 2)

        if not self.pbsq:
            if cached_exception:
                raise civet_exceptions.CivetException(cached_exception.message)
            else:
                raise civet_exceptions.CivetException("Unable to instantiate PBSQuery instance, unknown error.")
        self.pbs_server = pbs_server

    def query_job(self, job_id):
        """
            Query server for status of job
        
            query_job will return None if the job does not exist on the server, 
            otherwise it will return a JobStatus object.

            :param job_id: job id of job to query
        """

        # with some versions of Torque (Torque 4),  it is fairly common for
        # Torque to fail to establish a connection when making lots of
        # successive queries. If this happens,  wait and retry again
        retry = 0
        job_status = None

        while job_status is None:
            try:
                job_status = self.pbsq.getjob(job_id)
            except PBSQuery.PBSError as e:
                if retry < _MAX_RETRY:
                    retry += 1
                    print("Retrying connection...", file=sys.stderr)
                    time.sleep(retry ** 2)
                    continue
                else:
                    # TODO use custom exception rather than PBSQuery.PBSError
                    raise e

        # check to see if the job existed.  this is kind of lame, but we can't
        # just do "if job_status:" because PBSQuery.getjob returns an empty 
        # dictionary if the job is not found, but it returns some other object
        # that acts like a dictionary but does not have a __nonzero__ attribute
        # This should be fixed in a future version of PBSQuery.
        if 'Job_Name' in job_status:
            return JobStatus(job_status)
        else:
            return None

    def delete_job(self, job_id):
        """
           Sends job delete request to pbs_server for job

           :param job_id: job id to delete
           :return:  pbs_deljob return value (0 on success)
        """
        connection = _connect_to_server(self.pbs_server)
        rval = pbs.pbs_deljob(connection, job_id, '')
        pbs.pbs_disconnect(connection)
        return rval

    def delete_all_jobs(self, ids):
        """

        delete all jobs in a list of jobs

        :param ids: list of all jobs
        :return: zero on success, otherwise return value of failed pbs_deljob
        """
        for job_id in ids:
            connection = _connect_to_server(self.pbs_server)
            rval = pbs.pbs_deljob(connection, job_id, '')
            pbs.pbs_disconnect(connection)
            if rval and (rval != self.E_UNKNOWN or rval != self.E_STATE):
                return rval
        return 0
        
    def release_job(self, job_id):
        """
        Release a user hold on a job
        :param job_id: job to release
        """
        connection = _connect_to_server(self.pbs_server)
        rval = pbs.pbs_rlsjob(self.connection, job_id, 'u', '')
        pbs.pbs_disconnect(connection)
        return rval


class JobStatus(object):
    """
        JobStatus - a class that holds the dictionary-like object returned by 
        PBSQuery.getjob, and knows how to parse that output to get the  
        information we are interested in.
        
        This is what PBSJobRunner.query_job() returns.
        status : TORQUE job status as returned by PBSQueryJob
    """
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
            
    # return the exit_status attribute if it exists, if it does not exist
    # return None.  Should only exist if the job is in the "C" state.
    @property
    def exit_status(self):
        if 'exit_status' in self.status:
            return self.status['exit_status'][0]
        else:
            return None
    
    @property
    def walltime(self):
        if 'resources_used' in self.status:
            return self.status['resources_used']['walltime'][0]
        else:
            return "00:00:00"       

    @property
    def requested_walltime(self):
        if 'walltime' in self.status['Resource_List']:
            return self.status['Resource_List']['walltime'][0]
        else:
            #no walltime in Resource_List means no limit
            return "unlimited"


class TorqueJobRunner(object):
    """
        TorqueJobRunner is a class that encapsulates the functionality of 
        submitting jobs to a TORQUE cluster.
       
        attributes
        held_jobs: a list of job_id,server pairs that were submitted with a
            user hold
        submit_with_hold: if True any root job (job with no dependency) will be
            submitted with a user hold
        log_dir: directory to store log files, will be created if it doesn't
            exist
        execution_log_dir: log directory on execution host if different
            than log_dir
        pipeline_bin: allow a pipeline bin directory to be added to PATH.
            if set, this string will be prepended to the user's PATH
            at job run time
        validate: if true, run validate command
        queue: optional Torque queue, if None Torque default will be used
        submit:  If False job scripts will be generated but jobs will not be
            submitted.  Useful for debugging pipelines.
        send_failure_email: if true, send user email if a pipeline tool exits
            with non zero value and aborts the pipeline
    """

    # the template script, which will be customized for each job
    # $VAR will be substituted before job submission $$VAR will become $VAR
    # after substitution
    script_template = textwrap.dedent("""\
        #!/bin/bash
        
        # This script was generated by Civet (Version $CIVET_VERSION)
        $INFO
        
        $PBS_DIRECTIVES
        
        # define civet shell functions
        source $FUNCTIONS

        CIVET_LOGDIR=$LOG_DIR
        
        DATE=$$(date)
        
        exec 2> $LOG_DIR/$${PBS_JOBNAME}-err.log
        
        echo "Run time log for $$PBS_JOBNAME ($$PBS_JOBID)" > $LOG_DIR/$${PBS_JOBNAME}-run.log
        echo "Using Civet $CIVET_VERSION" >> $LOG_DIR/$${PBS_JOBNAME}-run.log
        
        echo "stderr log for $$PBS_JOBNAME ($$PBS_JOBID)" >&2

        echo "Run began on $$DATE" >> $LOG_DIR/$${PBS_JOBNAME}-run.log
        
        echo "EXECUTION HOST DETAILS:" >> $LOG_DIR/$${PBS_JOBNAME}-run.log
        uname -a >> $LOG_DIR/$${PBS_JOBNAME}-run.log

        $MODULE_PURGE
        
        # then load modulefiles, if any, specified by the tool xml
        $MODULE_LOAD_CMDS
        
        # add any tool/pipeline paths (if specified) to PATH
        # add the Civet bin directory to our PATH
        PATH=${TOOL_PATH}${PIPELINE_PATH}$CIVET_BIN:$$PATH

        # log the PATH in the job's -run.log to assist with debugging
        echo "PATH=$$PATH" >> $LOG_DIR/$${PBS_JOBNAME}-run.log
        
        cd $$PBS_O_WORKDIR

        # run validate command, if configured to do so
        RUN_VALIDATION=$RUN_VALIDATION
        if [ $$RUN_VALIDATION -ne 0 ]; then
            $CIVET_PYTHON $VALIDATE -m $MASTER_FILE $FILES_TO_VALIDATE >> $LOG_DIR/$${PBS_JOBNAME}-run.log
            VALIDATION_STATUS=$$?

            if [ $$VALIDATION_STATUS -ne 0 ]; then
                MESSAGE="Command not run, pre-run validation returned non-zero value."
                echo "$$MESSAGE  Aborting pipeline!" >&2
                send_failure_email $EMAIL_LIST "$$MESSAGE"
                check_epilogue $LOG_DIR/submitted_shell_scripts/epilogue.sh
                exit $$VALIDATION_STATUS
            fi
        fi
        
        $FILE_TEST

        # all pre-job checks passed, run any supplied version commands and
        # execute command(s)

        echo "Working directory: $$(pwd)" >> $LOG_DIR/$${PBS_JOBNAME}-run.log

        # optional version command, $VERSION_CMDS might be replaced by a noop,
        # so initialize VERSION_STATUS to 0
        VERSION_STATUS=0
        $VERSION_CMDS > $LOG_DIR/$${PBS_JOBNAME}-version.log 2>&1; VERSION_STATUS=$$?

        if [ $$VERSION_STATUS -ne 0 ]; then
            MESSAGE="Command not run, version command returned non-zero value."
            echo "$$MESSAGE  Aborting pipeline!" >&2
            send_failure_email $EMAIL_LIST "$$MESSAGE"
            check_epilogue $LOG_DIR/submitted_shell_scripts/epilogue.sh
            exit $$VERSION_STATUS
        fi

        # command(s) passed into BatchJob:
        $CMD
        
        CMD_EXIT_STATUS=$$?


        echo "EXIT STATUS: $$CMD_EXIT_STATUS" >> $LOG_DIR/$${PBS_JOBNAME}-run.log
        if [ $$CMD_EXIT_STATUS -ne 0 ]; then
            MESSAGE="Command returned non-zero value ($$CMD_EXIT_STATUS)."
            echo "$$MESSAGE  Aborting pipeline!" >&2
            if $SEND_FAILURE_EMAIL; then
                send_failure_email $EMAIL_LIST "$$MESSAGE"
            fi
            check_epilogue $LOG_DIR/submitted_shell_scripts/epilogue.sh
            exit $$CMD_EXIT_STATUS
        fi
        
        # check error log for list of keywords
        for str in $ERROR_STRINGS; do
            if grep -q "$$str" $LOG_DIR/$${PBS_JOBNAME}-err.log; then
                MESSAGE="Found error string in stderr log."
                echo "$$MESSAGE  Aborting pipeline!" >&2
                if $SEND_FAILURE_EMAIL; then
                    send_failure_email $EMAIL_LIST "$$MESSAGE"
                fi
                check_epilogue $LOG_DIR/submitted_shell_scripts/epilogue.sh
                exit 1
            fi
        done

        check_epilogue $LOG_DIR/submitted_shell_scripts/epilogue.sh
    
    """)

    # the template job epilogue, which will be shared between all jobs
    # submitted by this TorqueJobRunner
    # $VAR will be substituted before job submission $$VAR will become $VAR
    # after substitution
    epilogue_template = textwrap.dedent("""\
        #!/bin/bash

        # This script was generated by Civet (Version $CIVET_VERSION)

        #define civet shell functions
        source $FUNCTIONS

        PBS_JOBID=$$1
        PBS_JOBNAME=$$4
        EXIT_STATUS=$${10}
        WALLTIME=$$(get_walltime $$7)
        WALLTIME_REQUESTED=$$(get_walltime $$6)
        CIVET_LOGDIR=$LOG_DIR


        # the TORQUE epilogue doesn't seem to have USER defined, which is the default
        # TorqueJobRunner uses for sending error emails unless one is supplied.  Fix that.
        if [ -z $${USER+x} ]; then
            USER=$$(whoami)
        fi

        if [ $$EXIT_STATUS -lt 0 ]; then
            # Negative job exit status indicates Torque error, such as wall time limit
            if [ $$EXIT_STATUS -eq "-11" ]; then
                MESSAGE="Job hit walltime limit ($${WALLTIME_REQUESTED})"
            else
                MESSAGE="TORQUE Error ($${EXIT_STATUS})"
            fi
            send_failure_email $EMAIL_LIST "$$MESSAGE"
            if $ABORT; then
                abort_pipeline $LOG_DIR $$EXIT_STATUS $$WALLTIME $$WALLTIME_REQUESTED
            fi
        elif [ $$EXIT_STATUS -gt 0 ]; then
            # Job exited with non-zero, Job script should have already sent email
            if $ABORT; then
                abort_pipeline $LOG_DIR $$EXIT_STATUS $$WALLTIME $$WALLTIME_REQUESTED
            fi
        else
            # normal exit

            # in some specific cases, the job script may have created the -status.txt file
            # if so,  don't recreate it
            if [ ! -f $LOG_DIR/$${PBS_JOBNAME}-status.txt ]; then
                echo "exit_status=0" > $LOG_DIR/$${PBS_JOBNAME}-status.txt
                echo "walltime=$$WALLTIME" >> $LOG_DIR/$${PBS_JOBNAME}-status.txt
                echo "requested_walltime=$$WALLTIME_REQUESTED" >> $LOG_DIR/$${PBS_JOBNAME}-status.txt
            fi

            $SLEEP

            echo "Run finished on $$(date)" >> $LOG_DIR/$${PBS_JOBNAME}-run.log
        fi

    """)

    def __init__(self, log_dir="log", submit_with_hold=True, pbs_server=None, 
                 pipeline_bin=None, validate=False,
                 execution_log_dir=None, queue=None, submit=True,
                 epilogue_email=None, pipeline_path=None, validation_file=None,
                 send_failure_email=True):
        self.held_jobs = []
        self.submit_with_hold = submit_with_hold
        self.validate = validate
        self.log_dir = os.path.abspath(log_dir)
        self._job_names = []
        self._server = pbs_server
        self.pipeline_bin = pipeline_bin
        self.execution_log_dir = execution_log_dir
        self.queue = queue
        self.submit = submit
        self._id_seq = 0  # used to fake Torque job IDs when self.submit is False
        self.need_to_write_epilogue = True
        self.epilogue_email=epilogue_email
        self.pipeline_path = pipeline_path
        self.validation_file = validation_file
        self.send_failure_email = send_failure_email

        if self.execution_log_dir:
            self.execution_log_dir = os.path.abspath(self.execution_log_dir)

        self.epilogue_filename = os.path.join(self.log_dir, _SHELL_SCRIPT_DIR, "epilogue.sh")

        utilities.make_sure_path_exists(self.log_dir)
          
        self._id_log = open(os.path.join(log_dir, common.BATCH_ID_LOG), 'w')

        if not self.submit:
            # we aren't actually submitting jobs,  create a file in the log 
            # directory that civet_status can use to detect this case
            open(os.path.join(log_dir, common.NO_SUB_FLAG), 'w').close()

        # do some sanity checking on groups of files created in the log dir
        g_test_file = os.path.join(log_dir, ".group_test")
        open(g_test_file, 'w').close()
        gid = os.stat(g_test_file).st_gid
        os.remove(g_test_file)
        if gid not in os.getgroups():
            print(textwrap.fill("ERROR: The log directory {} has a group ID "
                                "that you are not a member of. This will "
                                "prevent Torque from operating correctly.".format(self.log_dir),
                                width=80, break_long_words=False),
                  file=sys.stderr)
            print("\nPIPELINE NOT SUBMITTED\n", file=sys.stderr)
            sys.exit(1)

    @staticmethod
    def __make_pbs_attrs(resources, attributes):
        pbs_attrs = pbs.new_attropl(len(attributes) + len(resources))

        # populate pbs_attrs
        attr_idx = 0
        for resource, val in resources.iteritems():
            pbs_attrs[attr_idx].name = pbs.ATTR_l
            pbs_attrs[attr_idx].resource = resource
            pbs_attrs[attr_idx].value = str(val)
            attr_idx += 1

        for attribute, val in attributes.iteritems():
            pbs_attrs[attr_idx].name = attribute
            pbs_attrs[attr_idx].value = str(val)
            attr_idx += 1

        return pbs_attrs

    def queue_job(self, batch_job):
        """
          queue a BatchJob.
          
          :param batch_job: description of the job to queue
        """
        
        # batch job names should be unique for civet pipelines because the 
        # job name is used to name log files and other output
        # Civet generates unique names for each step, so this is just checking
        # for a programming error
        assert batch_job.name not in self._job_names
        
        if self.execution_log_dir:
            log_dir = self.execution_log_dir
        else:
            log_dir = self.log_dir

        # set batch_job.stderr_path and batch_job.stdout_path if they aren't already set
        if not batch_job.stdout_path:
            batch_job.stdout_path =  os.path.join(log_dir, batch_job.name + ".o")
        if not batch_job.stderr_path:
            batch_job.stderr_path = os.path.join(log_dir, batch_job.name + ".e")

        # write batch script
        filename = self.write_script(batch_job)
        
        if self.submit:
            # build up our torque job attributes and resources
            job_attributes = {}
            job_resources = {
                'nodes': "{0}:ppn={1}".format(batch_job.nodes, batch_job.ppn),
                'walltime':  batch_job.walltime,
                'epilogue': self.epilogue_filename
            }

            if batch_job.mem:
                job_resources['mem'] = batch_job.mem
        
            job_attributes[pbs.ATTR_v] = self.generate_env(batch_job.workdir)
        
            if batch_job.name:
                job_attributes[pbs.ATTR_N] = batch_job.name
        
            job_attributes[pbs.ATTR_o] = batch_job.stdout_path

            #XXX workaround for a TORQUE bug where local copies of stderr &
            # stdout files to /dev/null don't work correctly but remote
            # copies (to submit host) do
            if job_attributes[pbs.ATTR_o] == "/dev/null":
                job_attributes[pbs.ATTR_o] = socket.gethostname() + ":/dev/null"
            
            job_attributes[pbs.ATTR_e] = batch_job.stderr_path

            #XXX workaround for a TORQUE bug where local copies of stderr &
            # stdout files to /dev/null don't work correctly but remote
            # copies (to submit host) do
            if job_attributes[pbs.ATTR_e] == "/dev/null":
                job_attributes[pbs.ATTR_e] = socket.gethostname() + ":/dev/null"

            if batch_job.depends_on:
                job_attributes[pbs.ATTR_depend] = self._dependency_string(batch_job)
            elif self.submit_with_hold:
                job_attributes[pbs.ATTR_h] = 'u'
            
            if batch_job.mail_option:
                job_attributes[pbs.ATTR_m] = batch_job.mail_option
                
            if batch_job.email_list:
                job_attributes[pbs.ATTR_M] = batch_job.email_list

            if batch_job.date_time:
                job_attributes[pbs.ATTR_a] = str(int(time.mktime(batch_job.date_time.timetuple())))

            pbs_attrs = pbs.new_attropl(len(job_attributes) + len(job_resources))
        
            # populate pbs_attrs
            attr_idx = 0
            for resource, val in job_resources.iteritems():
                pbs_attrs[attr_idx].name = pbs.ATTR_l
                pbs_attrs[attr_idx].resource = resource
                pbs_attrs[attr_idx].value = val
                attr_idx += 1
            
            for attribute, val in job_attributes.iteritems():
                pbs_attrs[attr_idx].name = attribute
                pbs_attrs[attr_idx].value = val
                attr_idx += 1
            
            # we've initialized pbs_attrs with all the attributes we need to set
            # now we can connect to the server and submit the job
            connection = _connect_to_server(self._server)

            # connected to pbs_server

            # submit job
            retry = 0
            job_id = pbs.pbs_submit(connection, pbs_attrs, filename,
                                    self.queue, None)

            # if pbs.pbs_submit failed, try again
            while not job_id and retry < _MAX_RETRY:
                retry += 1
                print("Retrying connection...", file=sys.stderr)
                time.sleep(retry ** 2)
                job_id = pbs.pbs_submit(connection, pbs_attrs, filename,
                                        self.queue, None)

            pbs.pbs_disconnect(connection)

            # check to see if the job was submitted successfully.
            if not job_id:
                e, e_msg = pbs.error()
                # the batch system returned an error, throw exception 
                raise Exception("Error submitting job.  "
                                "Torque error {0}: '{1}'".format(e, torque_strerror(e)))

            if self.submit_with_hold and not batch_job.depends_on:
                self.held_jobs.append(job_id)
        
        else:
            #self.submit is False, fake a job ID
            job_id = "{0}.civet".format(self._id_seq)
            self._id_seq += 1
            
        self._job_names.append(batch_job.name)

        self._id_log.write(job_id + '\t' + batch_job.name + '\t' + str(
            self._printable_dependencies(batch_job.depends_on)) + '\n')
        self._id_log.flush()
        return job_id

    @staticmethod
    def submit_managed_job(task, pbs_server=None):
        """
        submit a managed job from the civet_managed_batch_master program.  Not
        passed as a BatchJob object, but simply as dictionary describing the
        task
        :param task: dictionary describing the task.
        :return: batch job ID
        """
         # build up our torque job attributes and resources
        job_attributes = {}
        job_resources = {}

        job_resources['nodes'] = "1:ppn={}".format(task['threads'])
        job_resources['walltime'] = task['walltime']
        job_resources['epilogue'] = task['epilogue_path']
        if task['mem']:
            job_resources['mem'] = task['mem']

        job_attributes[pbs.ATTR_v] = task['batch_env']
        job_attributes[pbs.ATTR_N] = task['name']
        job_attributes[pbs.ATTR_o] = task['stdout_path']
        job_attributes[pbs.ATTR_e] = task['stderr_path']
        if task['mail_options']:
            job_attributes[pbs.ATTR_m] = task['mail_options']
        if task['email_list']:
            job_attributes[pbs.ATTR_M] = task['email_list']

        pbs_attrs = TorqueJobRunner.__make_pbs_attrs(job_resources,
                                                     job_attributes)

        queue = str(task['queue']) if task['queue'] else None
        return TorqueJobRunner.submit_with_retry(pbs_attrs, str(task['script_path']),
                                                 queue)

    @staticmethod
    def submit_simple_job(task, pbs_server=None):
        """
        Submit a basic batch job, without all the extra stuff used in a normal
        civet job.  This is used to submit the civet_managed_batch_master
        command.
        :param task:
        :param pbs_server:
        :return:
        """

        # build up our torque job attributes and resources
        job_attributes = {}
        job_resources = {}

        # we will require a number of threads, walltime, workdir, and name
        # if workdir is not defined, use the current working directory
        # if threads is not defined, use 1
        workdir = task.get('workdir', os.getcwd())
        job_resources['nodes'] = "1:ppn={}".format(task.get('threads', '1'))
        job_resources['walltime'] = task['walltime']
        job_attributes[pbs.ATTR_v] = TorqueJobRunner.generate_env(workdir)
        job_attributes[pbs.ATTR_N] = task['name']

        if 'mem' in task:
            job_resources['mem'] = task['mem']
        if 'stdout_path' in task:
            job_attributes[pbs.ATTR_o] = task['stdout_path']
        if 'stderr_path' in task:
            job_attributes[pbs.ATTR_e] = task['stderr_path']
        if 'mail_options' in task:
            job_attributes[pbs.ATTR_m] = task['mail_options']
        if 'email_list' in task:
            job_attributes[pbs.ATTR_M] = task['email_list']

        pbs_attrs = TorqueJobRunner.__make_pbs_attrs(job_resources,
                                                     job_attributes)

        queue = str(task['queue']) if task['queue'] else None

        # generate a simple batch submission script to run our command
        with tempfile.NamedTemporaryFile(delete=False) as tf:
            tf.write("#!/bin/bash\n")
            tf.write("cd $PBS_O_WORKDIR\n")
            tf.write(task['cmd'])
            script_path = tf.name

        job_id = TorqueJobRunner.submit_with_retry(pbs_attrs, script_path,
                                                   queue)
        os.unlink(script_path)

        return job_id

    @staticmethod
    def submit_with_retry(pbs_attrs, script_path, queue, pbs_server=None):
        # connect to pbs server
        connection = _connect_to_server(pbs_server)

        # submit job
        retry = 0
        job_id = pbs.pbs_submit(connection, pbs_attrs, script_path,
                                queue, None)

        # if pbs.pbs_submit failed, try again
        while not job_id and retry < _MAX_RETRY:
            retry += 1
            print("Retrying connection...", file=sys.stderr)
            time.sleep(retry ** 2)
            job_id = pbs.pbs_submit(connection, pbs_attrs, script_path,
                                    queue, None)

        pbs.pbs_disconnect(connection)

        #check to see if the job was submitted successfully.
        if not job_id:
            e, e_msg = pbs.error()
            # the batch system returned an error, throw exception
            raise Exception("Error submitting job.  "
                            "Torque error {0}: '{1}'".format(e, torque_strerror(e)))

        return job_id

    def release_job(self, job_id, connection=None):
        """
            Release a user hold from a held batch job.
            
            :param job_id: job id to release (short form not allowed)
            :param id: job id to release (short form not allowed)
            :param connection: optional connection to a pbs_server, if not
                  passed release_job will establish a new connection
        """
        c = connection if connection else _connect_to_server(self._server)
        
        rval = pbs.pbs_rlsjob(c, job_id, 'u', '')
        
        if not connection:
            pbs.pbs_disconnect(c)
        
        if rval == 0:
            self.held_jobs.remove(job_id)
        return rval

    def release_all(self):
        """
            Release all jobs in self.held_jobs list reusing connections.  
        """

        # if we are 'faking' pipeline submission with civet_run -n, then there
        # is nothing to do
        if not self.submit:
            return

        # copy the list of held jobs to iterate over because release_job mutates
        # self.held_jobs
        jobs = list(self.held_jobs)  
        connection = _connect_to_server(self._server)
        for id in jobs:
            self.release_job(id, connection)
        pbs.pbs_disconnect(connection)

    def write_script(self, batch_job):

        #create script directory if necessary
        self._setup_script_dir(batch_job.email_list)

        #write batch script
        filename = os.path.join(self.log_dir, _SHELL_SCRIPT_DIR, "{0}.sh".format(batch_job.name))
        with open(filename, "w") as script_file:
            script_file.write(self.generate_script(batch_job))

        return filename

    def generate_script(self, batch_job):
        """
            Generate a Torque batch script based on our template and return as
            a string.
            
            mainly intended to be used internally in PBSJobRunner, but it could 
            be useful externally for debugging/logging the contents of a job 
            script generated for a batch_job

            :param batch_job: BatchJob for which to generate script
            :return: batch script as string
        """  
        tokens = {}
        
        tokens['PBS_DIRECTIVES'] = self._generate_directives(batch_job, self.epilogue_filename)
        tokens['CMD'] = batch_job.cmd
        
        if self.execution_log_dir:
            tokens['LOG_DIR'] = self.execution_log_dir
        else:
            tokens['LOG_DIR'] = self.log_dir
            
        tokens['ID_FILE'] = common.BATCH_ID_LOG
        
        tokens['MODULE_LOAD_CMDS'] = ""  
        if batch_job.modules:
            for module in batch_job.modules:
                tokens['MODULE_LOAD_CMDS'] = "{0}module load {1}\n".format(tokens['MODULE_LOAD_CMDS'], module)   
        
        tokens['CIVET_PYTHON'] = config.civet_python
        tokens['VALIDATE'] = os.path.join(common.CIVET_HOME, "bin/validate")
        if self.validate and batch_job.files_to_validate:
            tokens['RUN_VALIDATION'] = 1
            tokens['FILES_TO_VALIDATE'] = ' '.join(batch_job.files_to_validate)
            tokens['MASTER_FILE'] = self.validation_file
        else:
            tokens['RUN_VALIDATION'] = 0
            tokens['FILES_TO_VALIDATE'] = ""
            tokens['MASTER_FILE'] = ""
            
        if batch_job.version_cmds:
            tokens['VERSION_CMDS'] = "({0})".format('; '.join(batch_job.version_cmds))
        else:
            tokens['VERSION_CMDS'] = "#[none given]"

        if batch_job.error_strings:
            tokens['ERROR_STRINGS'] = ' '.join(batch_job.error_strings)
        else:
            tokens['ERROR_STRINGS'] = ''
            
        if batch_job.walltime:
            tokens['WALLTIME_REQUESTED'] = batch_job.walltime
        else:
            tokens['WALLTIME_REQUESTED'] = "unlimited"
            
        tokens['FUNCTIONS'] = os.path.join(common.CIVET_HOME, "lib/job_runner/functions.sh")
        
        if self.pipeline_bin:
            tokens['CIVET_BIN'] = "{0}:{1}".format(self.pipeline_bin, os.path.join(common.CIVET_HOME, "bin"))
        else:
            tokens['CIVET_BIN'] = os.path.join(common.CIVET_HOME, "bin")
            
        tokens['CIVET_VERSION'] = version.version_from_git()

        if batch_job.info:
            tokens['INFO'] = "# " + batch_job.info.replace('\n', "\n# ")
        else:
            tokens['INFO'] = ""
        
        tokens['FILE_TEST'] = self._build_file_test(batch_job)

        if batch_job.email_list:
            tokens['EMAIL_LIST'] = batch_job.email_list
        else:
            tokens['EMAIL_LIST'] = "${USER}"

        tokens['SEND_FAILURE_EMAIL'] = 'true' if self.send_failure_email else 'false'

        if self.pipeline_path:
            tokens['PIPELINE_PATH'] = self.pipeline_path + ":"
        else:
            tokens['PIPELINE_PATH'] = ''

        if batch_job.tool_path:
            tokens['TOOL_PATH'] = batch_job.tool_path + ":"
        else:
            tokens['TOOL_PATH'] = ''

        if config.purge_user_modulefiles:
            tokens['MODULE_PURGE'] = (
                "# first unload any loaded modulefiles, these may be loaded automatically\n"
                "# in a user's startup scripts, but they could conflict with modulefiles\n"
                "# specified by the Civet tool\n"
                "module purge")
        else:
            tokens['MODULE_PURGE'] = ""
        
        return string.Template(self.script_template).substitute(tokens)

    def generate_epilogue(self, email_list, abort_on_failure=True):

        tokens = {}
        tokens['CIVET_VERSION'] = version.version_from_git()
        tokens['FUNCTIONS'] = os.path.join(common.CIVET_HOME, "lib/job_runner/functions.sh")
        tokens['EMAIL_LIST'] = email_list if email_list else "${USER}"
        tokens['ABORT'] = 'true' if abort_on_failure else 'false'

        if self.execution_log_dir:
            tokens['LOG_DIR'] = self.execution_log_dir
        else:
            tokens['LOG_DIR'] = self.log_dir

        if config.io_sync_sleep:
            tokens['SLEEP'] = (
                            "# sleep to overcome any lag with NFS file attribute cacheing\n"
                            "# This ensures that downstream jobs will see all output files written by this job\n"
                            "sleep {}".format(config.io_sync_sleep)
            )
        else:
            tokens['SLEEP'] = ""

        return string.Template(self.epilogue_template).substitute(tokens)

    @staticmethod
    def generate_env(workdir):
        """
            Generate a basic environment string to send along with the job. 
            
            This can define any environment variables we want defined in the 
            job's environment when it executes. We define some of the typical 
            PBS_O_* variables

            :param workdir: working directory of the job
        """
    
        # our script start with "cd $PBS_O_WORKDIR", make sure we set it
        env = "PBS_O_WORKDIR={0}".format(workdir)
        
        # define some of the other typical PBS_O_* environment variables
        # PBS_O_HOST is used to set default stdout/stderr paths, the rest probably
        # aren't necessary
        
        env = "".join([env, ",PBS_O_HOST=", socket.getfqdn()])
        env = "".join([env, ",PBS_O_PATH=", os.environ['PATH']])
        if 'HOME' in os.environ:
            env = "".join([env, ",PBS_O_HOME=", os.environ['HOME']])
        if 'LOGNAME' in os.environ:
            env = "".join([env, ",PBS_O_LOGNAME=", os.environ['LOGNAME']])
        
        return env

    @staticmethod
    def _generate_directives(batch_job, epilogue_filename=None):
        """
            Generate #PBS directives to insert into batch script to facilitate
            rerunning individual scripts by hand (during development)
            as well as documenting the resources requested when the job was
            submitted

            :param batch_job: BatchJob for which to generate #PBS directives
        """
        directives = ["#PBS -l walltime=" + batch_job.walltime,
                      "#PBS -l nodes={0}:ppn={1}".format(batch_job.nodes, batch_job.ppn)]

        if epilogue_filename:
            directives.append("#PBS -l epilogue={}".format(epilogue_filename))
        
        if batch_job.mem:
            directives.append("#PBS -l mem=" + batch_job.mem)
            
        if batch_job.name:
            directives.append("#PBS -N " + batch_job.name)

        if batch_job.stderr_path:
            directives.append("#PBS -e " + batch_job.stderr_path)

        if batch_job.stdout_path:
            directives.append("#PBS -o " + batch_job.stdout_path)
        
        return '\n'.join(directives)

    @staticmethod
    def _dependency_string(batch_job):
        """
            Generate a TORQUE style dependency string for a batch job to be 
            passed to the ATTR_depend job attribute.
            
            This will return empty string if batch_job.depends_on is empty.

            :param batch_job: BatchJob for which to generate dependency string
            :return: dependency string for job
        """
    
        # we want this to work if batch_job.depends_on is a string containing 
        # the ID of a single job or a list of job ID strings
        if not batch_job.depends_on:
            return ""
        elif isinstance(batch_job.depends_on, utilities.string_types):
            #handle string case
            return "{0}:{1}".format(_DEFAULT_DEPEND_TYPE, batch_job.depends_on)
        else:
            #not a string, assume list of job ids to join
            return "{0}:{1}".format(_DEFAULT_DEPEND_TYPE, 
                                    ':'.join(batch_job.depends_on))
                                    
    def _printable_dependencies(self, dependency_list):
        """
            Return a list containing shortened (hostname removed) job
            dependencies

            :param dependency_list: Torque dependency string to reformat
            :return: formatted dependency string
        """
        shortened = []
        for id in dependency_list:
            shortened.append(id.split('.', 1)[0])
            
        return shortened
        
    def _build_file_test(self, batch_job):
        """
            Return a chunk of bash code that will perform the required file 
            test(s) for this job. This code will exit using the file_test_exit 
            function, which will do some logging in addition to exiting the job

            :param batch_job: BatchJob to generate file tests for insertion into
                              job script
            :return: bash code to perform file tests, to be inserted into script
        """
        header = "#pre job file test\n"
        
        # need the walltime because we will create a -status.txt file if we need
        # to bail out because of the file test
        if batch_job.walltime:
            walltime = batch_job.walltime
        else:
            walltime = "unlimited"
    
        if batch_job.file_test_logic == 'AND':
            test_type = '&&'
        elif batch_job.file_test_logic == 'OR':
            test_type = '||'
        else:
            test_type = '&&'  #else default to AND to be safe, but BatchJob should force it to be either AND or OR

        if not batch_job.files_to_test:
            bash_code = ""
        elif isinstance(batch_job.files_to_test, utilities.string_types):
            bash_code = header + 'if [ -e {0} ]; then file_test_exit {1} {2}; fi'.format(batch_job.files_to_test, self.log_dir, walltime)
        else:
            tests = []
            bash_code = header + "if [["
            for file in batch_job.files_to_test:
                tests.append(' -e "{0}" '.format(file))
            bash_code += test_type.join(tests)
            bash_code += ']]; then file_test_exit {0} {1}; fi'.format(self.log_dir, walltime)
            
        return bash_code

    def _setup_script_dir(self, email_list):
        """
        setup the directory in the log_dir to hold all of the shell scripts we
        will generate.  Also write the epilogue script to this directory

        :param email_list:  we need the list of email addresses (usually a
            single address) when generating the epilogue script from the
            template
        """

        script_dir = os.path.join(self.log_dir, _SHELL_SCRIPT_DIR)
        try:
            os.makedirs(script_dir)
            os.chmod(script_dir, stat.S_IRWXU)

            if self.need_to_write_epilogue:
                with open(self.epilogue_filename, "w") as epilogue_file:
                    epilogue_file.write(self.generate_epilogue(email_list))
                os.chmod(self.epilogue_filename, stat.S_IRWXU)
                self.need_to_write_epilogue = False

        except OSError as exception:
            if exception.errno != errno.EEXIST:
                print('Error while creating directory ' + script_dir, file=sys.stderr)
                raise


def main():
    """
    simple main function that tests some functionality if we run this script
    directly rather than import it
    """

    job_runner = TorqueJobRunner()
    jm = JobManager()
    modules = []

    job = BatchJob("hostname", walltime="00:02:00", name="test_job", 
                   modules=modules, mail_option="be")

    print("submitting job with the following script:")
    print("---------------------------------------------------")
    print(job_runner.generate_script(job))
    print("---------------------------------------------------")
    id = job_runner.queue_job(job)
    
    print(id)
    
    status = jm.query_job(id)
    if status:
        print("Status of job is " + status.state)
    
    print("calling job_runner.release_all()")
    job_runner.release_all()
    status = jm.query_job(id)
    if status:
        print("Status of job is " + status.state)

    
if __name__ == '__main__': 
    main()
