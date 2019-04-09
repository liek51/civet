#
# Copyright (C) 2016  The Jackson Laboratory
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

#
#   This file is sourced by our batch job template to provide some bash 
#   functions that we use for simple runtime tasks as part of a batch job
#   submitted by civet
#

#make sure the shell is configured to use modulefiles
function module {
    eval `/usr/bin/modulecmd bash $*`
}

function get_walltime {
    local saveIFS=$IFS
    IFS=','
    for RESOURCE in $1
    do
        NAME=$(echo $RESOURCE | cut -d "=" -sf 1)
        VALUE=$(echo $RESOURCE | cut -d "=" -sf 2)
        if [ $NAME = "walltime" ]; then
            echo $VALUE
            IFS=$saveIFS
            return 0
        fi
    done

    IFS=$saveIFS
}

function send_failure_email {

    # send an email notification regarding the pipeline failure
    echo "Civet Pipeline Failure:  Tool ${PBS_JOBNAME} (Batch job ${PBS_JOBID}) Civet Log Directory ${CIVET_LOGDIR} $2" |  mailx -s "Civet Pipeline Failure" $1
}

function abort_pipeline {
    
    local LOGDIR=$1
    local EXIT_VAL=$2
    local WALLTIME=$3
    local WALLTIME_REQ=$4

    echo "exit_status=${EXIT_VAL}" > ${LOGDIR}/${PBS_JOBNAME}-status.txt
    echo "walltime=${WALLTIME}" >> ${LOGDIR}/${PBS_JOBNAME}-status.txt
    echo "requested_walltime=${WALLTIME_REQ}" >> ${LOGDIR}/${PBS_JOBNAME}-status.txt

    # there is a race condition here..  its possible the pipeline could be
    # canceled, but we can't see the cancel.log file yet.  Not much
    # we can do about that.  The civet_status code can still figure it out if
    # the job was canceled while running.
    if [ -f ${LOGDIR}/cancel.log ]; then
        echo "canceled=TRUE" >> ${LOGDIR}/${PBS_JOBNAME}-status.txt
        echo "state_at_cancel=R" >> ${LOGDIR}/${PBS_JOBNAME}-status.txt
    fi

    echo "Aborting pipeline" > ${LOGDIR}/${PBS_JOBNAME}-abort.log

    # if this pipeline is "managed" then the pipeline manager will take care
    # of deleting the rest of the running pipeline jobs
    if [ -f ${LOGDIR}/MANAGED_BATCH ]; then
        return
    fi

    echo "calling qdel on all jobs (ignoring previous job state)" >> ${LOGDIR}/${PBS_JOBNAME}-abort.log
    
    # just iterate over all of the job ids in this pipeline and try to 
    # qdel them.  We don't care what the state is, or even if they still exit
    while read ID NAME DEP; do
        if [ "$ID" != "$PBS_JOBID" ]; then
            echo "calling qdel on $ID (${NAME})" >> ${LOGDIR}/${PBS_JOBNAME}-abort.log
            qdel ${ID} >> ${LOGDIR}/${PBS_JOBNAME}-abort.log 2>&1
        fi
    done < ${LOGDIR}/pipeline_batch_id_list.txt

}

function file_test_exit {
    local LOGDIR=$1
    local WALLTIME_REQ=$2

    echo "Exiting because of pre-job file test (exit_if_exists)" > ${LOGDIR}/${PBS_JOBNAME}-run.log
    
    echo "exit_status=0" > ${LOGDIR}/${PBS_JOBNAME}-status.txt
    echo "walltime=00:00:00" >> ${LOGDIR}/${PBS_JOBNAME}-status.txt
    echo "requested_walltime=${WALLTIME_REQ}" >> ${LOGDIR}/${PBS_JOBNAME}-status.txt
    echo "exit_if_exists=TRUE" >> ${LOGDIR}/${PBS_JOBNAME}-status.txt
    
    exit 0
}

function check_epilogue {

    # for now, all this function does is make sure the epilogue.sh script has
    # the correct permissions.  It is created with 700, but we have had some
    # users change them before the pipeline finishes.  If the permissions are
    # too loose, Torque may refuse to execute the epilogue
    chmod 700 $1

}