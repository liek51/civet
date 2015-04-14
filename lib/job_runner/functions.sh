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
    echo "Civet Pipeline Failure:  Tool ${PBS_JOBNAME} (Batch job ${PBSJOBID}) $2" |  mailx -s "Civet Pipeline Failure" $1
}

function abort_pipeline {
    
    local LOGDIR=$1
    local EXIT_VAL=$2
    local WALLTIME=$3
    local WALLTIME_REQ=$4


    echo "Aborting pipeline" > ${LOGDIR}/abort.log
    echo "calling qdel on all jobs (ignoring previous job state)" >> ${LOGDIR}/abort.log
    
    # just iterate over all of the job ids in this pipeline and try to 
    # qdel them.  We don't care what the state is, or even if they still exit
    while read ID NAME DEP; do
        if [ "$ID" != "$PBS_JOBID" ]; then
            echo "calling qdel on $ID (${NAME})" >> ${LOGDIR}/abort.log
            qdel ${ID} >> ${LOGDIR}/abort.log 2>&1
        fi
    done < ${LOGDIR}/pipeline_batch_id_list.txt
    
    
    echo "exit_status=${EXIT_VAL}" > ${LOGDIR}/${PBS_JOBNAME}-status.txt
    echo "walltime=${WALLTIME}" >> ${LOGDIR}/${PBS_JOBNAME}-status.txt
    echo "requested_walltime=${WALLTIME_REQ}" >> ${LOGDIR}/${PBS_JOBNAME}-status.txt

}

# Iterate over all loaded moulefiles and unload them.
#
# The purpose is to start with a clean slate and only load modules specified
# by the pipeline tool if the user has modulefiles loaded automatically in 
# their .bashrc
# If a user loaded some modulefiles automatically it is possible that they 
# could conflict with specific versions specified in the tool xml
function unload_all_modules {
    local saveIFS=$IFS
    IFS=:
    for MOD in $LOADEDMODULES
    do
        # module unload command gets confused unless we reset IFS
        IFS=$saveIFS module unload ${MOD}
    done

    IFS=$saveIFS

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
