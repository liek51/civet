# Civet User Guide

## Overview

This document provides a basic user guide for Civet commands. See the 
`civet_XML.md` document for information about developing Civet 
pipelines. 

## Pipeline Execution Modes

Civet provides multiple pipeline execution modes. 

### Standard Batch

This is the standard mode of execution. In this case, Civet leverages 
the job dependency features of the batch queuing system to manage 
pipeline execution. There is no persistent Civet management process
to monitor and manage the pipeline execution. This is also called 
"unmanaged mode".

The `civet_run` command is used to launch a Civet pipeline using the 
standard batch execution mode. The required arguments for `civet_run` 
are the path to the pipeline XML followed by the pipeline parameters.

### Managed Batch

In this case a management process controls execution of the pipeline. 
The management process tracks job dependencies and submits jobs to the 
batch queue once all of their dependencies have been satisfied. The 
management process can limit the number of jobs queued at a given time, 
this way it can execute pipelines that exceed any user job limits 
that are configured by HPC administrators.

To execute a pipeline in managed batch mode the user must prepare a 
task file using the `civet_prepare` command.  This command processes 
the pipeline XML similar to `civet_run` but instead of submitting 
batch jobs for each tool invocation, `civet_prepare` creates a "task 
list" of tasks that need to be executed to complete the pipeline. These 
tasks are stored in a "task file" for later execution.  Multiple 
`civet_prepare` invocations can write tasks to the same file, in other 
words one task file can contain multiple pipelines. This task file is
executed by a management process, which can be started as a batch job 
by the `civet_start_managed` command.

### Cloud

_under development_