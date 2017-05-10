#Civet XML Description
This document describes the XML files used to define a Civet pipeline.

##Pipeline XML Description
Each pipeline will be controlled by an XML description file. The outer 
tag is `<pipeline>`.

###pipeline

    <pipeline name="..." tool_search_path="..." path="...">
        <file />
        <dir />
        <foreach />
        <step />
    </pipeline>

A `<pipeline>` tag contains one or more each of `<file>`, `<dir>`, and 
`<step>` tags. The `tool_search_path` attribute is optional and it 
contains a colon-delimited list of directories to search in when 
looking for tool XML definition files. The search process is described 
in more detail with the `<tool>` tag documentation. The optional path 
attribute specifies path information to be prepended onto the user's
PATH at runtime for each job submitted as part of the pipeline.

***

###file

The `<file>` tag specifies a file that is to be used in the pipeline. 
The `filespec` (complete or partial file path) may be hardcoded in the 
tag, or refer to a positional command line parameter. A file's path can
also be based on another file's path; a number of different
transformation options are available.

    <file id="..." input="..." temp="..." in_dir="..." filespec="..." />
    <file id="..." input="..." temp="..." in_dir="..." parameter="..." />
    <file id="..." input="..." temp="..." in_dir="..." based_on="..."  
        pattern="..." replace="..." append="..."  
        datestamp_append="..." datestamp_prepend="..." />


The `input` attribute is optional, and can have the values True or 
False (case-blind). If omitted, the attribute defaults to False. Input 
files must exist at the start of the pipeline. A pipeline is free 
(even encouraged) to check that all input files exist at the start of 
the pipeline, to fail early rather than encounter a 'file not found'
error partially through the pipeline execution.

The `temp` attribute is optional, and can have the values True or False 
(case-blind). If omitted, it defaults to False. Temp files will be 
deleted at the end of the pipeline's execution. A `filespec` attribute
may be specified on a temp file. If so, the file will have the name 
provided, otherwise, it will have an arbitrary name generated, as if by 
Python's `tempfile.NamedTemporaryFile()`.

The `in_dir` attribute is optional. If present, it specifies the 
directory id of the directory in which the file or directory is located 
or to be created.

The `filespec` form could be used, for instance, for the path to a 
reference genome.

If the `parameter` form is used, the value of the attribute is the 
positional argument's number (1-based) from the command line invoking
the pipeline.

It is possible to create files where the filename is created by 
manipulating the basename of another file, by using the `based_on` 
attribute. If the `based_on` form is used, the `parameter` and 
`filespec` attributes cannot be used. The `based_on` attribute 
specifies a previously declared file id. This is used in conjunction 
with either the `append`, `datestamp_append`, or `datestamp_prepend` 
attributes, or the combination of `pattern` and `replace` attributes. 
If used, the `pattern` and `replace` attributes must both be specified. 
The `append` and `datestamp_append` attributes are mutually 
incompatible with each other. 

If the `append` attribute used, its value is simply appended to the 
basename of the file specified in the `based_on` attribute.

If the `datestamp_append` or `datestamp_prepend` attributes are used, 
the attribute values are interpreted as if they are format strings for 
Python's `datetime.strftime`. An example is

    datestamp_append="_%Y_%m_%d"

would append the date in the form _YYYY_MM_DD to the filename of the 
`based_on` file to create the new filename.

If the `pattern` and `replace` attributes are used, the filename will 
be generated as if by using the Python `re.subs()` operation applying 
the values of `pattern` and `replace` to the basename of the file 
specified in the `based_on` attribute. This capability is needed, for 
instance, using samtools, where some commands need the trailing ".bam" 
stripped off of a file name.

**Note:** when the `replace` attribute is combined with other attributes, 
such as `append` or `datestamp_prepend`, the `replace` will be applied 
first. 

**Note:** when using `based_on` only the basename of the source file is 
used; the pipeline developer must use the  `in_dir` attribute if they 
want the file to be placed anywhere other than the default output 
directory.

***

###dir

The `<dir>` tag specifies a directory. As with files, a `dir` can 
specify an input or output directory. The difference is that an output 
directory need not exist on startup.

    <dir id="..." input="..." filespec="..." in_dir="..." create="..."  
        based_on="..." pattern="..." replace="..." append="..."  
        datestamp_prepend="..." datestamp_append="..."
        default_output="..." from_file="..."/>

A directory that is specified as an output directory and exists prior 
to the start of the pipeline must be writable.

The `<dir>` tag with the `default_output="True"` attribute specifies 
the default output directory for output files specified with a relative 
path. It is also the location where various run logs are stored. If 
no `<dir default_output="True" ...>` tags are specified, the current 
working directory will be used. Since the default output directory is 
always processed first, the `default_output` attribute can not be
combined with `in_dir`. 

The `in_dir` attribute is optional. If present, it specifies the 
directory id of the directory in which the file or directory exists or 
is to be created.

As with `<file>`, the input attribute is optional, and can have the 
values True or False (case-blind). If omitted, the attribute defaults 
to False. Input directories must exist at the start of the pipeline, 
otherwise an error message will be reported. 

The `create` attribute is optional and defaults to True. It controls 
whether the directory is created by the pipeline prior to use. We have 
to be able to control this because some tools want to create output 
files in an existing directory, and others want to create the directory 
and will fail if the output directory already exists.

The `filespec` attribute is used to specify the directory path in the 
XML. The path can also be specified with the `from_file` attribute, 
which takes a file ID as input. In this case the directory is set to 
the parent directory of the file specified in the `from_file` 
attribute. Like a `file`, a `dir` can also be "based_on" another 
file or directory.

The `based_on`, `pattern`, `replace`, `append`, `datestamp_append` and 
`datestamp_prepend` attributes are as documented for the `<file>` tag.

***


### Implicitly Defined Files/Directories

Civet defines the following file IDs automatically:

PIPELINE_ROOT: This directory ID contains the path to the directory 
containing the pipeline XML file. Pipeline developers can use this to 
reference any supplemental files contained within the pipeline XML 
directory.

###filelist

Occasionally, some phase of the processing may require a set of files 
that can't be specified at pipeline design time, for instance a set of
bam files to be merged. The `<filelist>` tag allows specifying a 
regular expression pattern to be matched in a certain directory. At 
execution time, all the files matching this pattern will be provided in 
a space-separated list. The `foreach_id` attribute is used to signify 
that the `<filelist>` has a dependency on the execution of all the jobs 
in a specific `<foreach>` tag and is required for proper job scheduling 
when a `filelist` is the result of files generated in a `<foreach>` tag.

    <filelist id="..." in_dir="..." pattern="..." foreach_id="..."/>

The pattern will be processed as if by Python's `re.match()` function,
against all files in the specified directory. NOTE: this pattern is not 
a shell wildcard expression. For example, the pattern attribute to match
all .txt files would not be `"*.txt"`, but instead would be 
`".*\.txt$"`.

A `<filelist>` can also be passed as a parameter:

    <filelist id="..." parameter="..."/>

The parameter attribute is, in this case, the list of files is passed 
as a comma-delimited list on the command line. Paths are assumed to be 
relative to the current working directory at time of job submission and 
are converted into absolute paths.

The `<filelist>` tag can be used anywhere the `<file>` tag can be used.

***

###string

The `<string>` tag specifies a string that can be used in the pipeline. 
A `<string>` differs from a `<file>` in that `<file>` paths are 
expanded to absolute paths since pipelines can create and set their own 
working directory while strings are left unmodified. The string value 
may be hardcoded in the tag, or it may be based on another string or 
pipeline parameter.

    <string id="..." value="..." />
    <string id="..." parameter="..." />
    <string id="..." based_on="..." pattern="..." replace="..."  
        append="..." datestamp_append="..." datestamp_prepend="..." />

A string `parameter`, `based_on`, `pattern`, `replace`, `append`, 
`datestamp_append`, and `datestamp_prepend` are analogous to their 
counterparts for the `<file>` tag. See the documentation for `<file>` 
for more information. A string can be used anywhere a file can be used.

*** 
  
###step
A step is a logical phase of the pipeline. A `<step>` tag contains one 
or more `<tool>` tags.

    <step name="...">
        <tool ... />
    </step>

Steps currently have no effect on pipeline flow,  they are purely to 
help the pipeline developer group related tools. Steps do have an 
effect on naming of batch jobs.

***

###foreach

The `<foreach>` tag allows a step to be run on several files in a 
directory identified by the `dir` attribute.

**IMPORTANT NOTE:** The "iterations" of the `<foreach>` processing may 
happen in parallel. All processing of one set of files must be 
completely independent of the processing of another set of files.

The files to be used within this directory are identified by pattern 
matching the file names, so it is useful primarily when the files are 
systematically named, for instance, the output directory of an Illumina 
sequencing run. When there are groups of files that need to be 
processed together, for instance, paired fastq files, the tag allows 
constructing the name of the other input or output files based on the 
name of first one, again via regexp pattern matching / pattern 
replacement. This could be used, for instance, in performing paired 
end alignment. 

**IMPORTANT NOTE:** Since Civet submits all of the jobs of a pipeline 
up front, the files matched by a <foreach> tag must exist at pipeline 
submission time. These could be 'stub' files that get overwritten by
one or more steps of a pipeline, but the filenames need to at least 
exist at submission time so Civet knows how many jobs to submit for the 
`<foreach>`.

    <foreach id="..." dir="...">
        <file id="..." pattern="..." />
        <related id="..." input="..." pattern="..." replace="..." />
        <step />
    </foreach>

The `<foreach>` tag processes a set of files in the directory whose id 
is specified in the `dir` attribute. The order in which the files are 
processed is unspecified; in a cluster environment, they may be 
processed in parallel. The `id` attribute is used to specify a 
`<filelist>` dependency on this `<foreach>` tag.

The `<file>` tag's pattern attribute specifies the Python regex pattern
that will be used to select files for processing; it will be applied to 
each filename in the directory as if by using Python's `re.match()`
function. There must be exactly one `<file>` tag in a `<foreach>` 
construct. Any filenames that match will be processed. The tag's `id`
attribute specifies the id this file will be referenced by in foreach 
block. Since the files identified by this tag must exist to be pattern 
matched, they are inherently classed as input files.

The `<related>` tag specifies another file, either an existing input 
file or a file to be created. The `id` attribute specifies the id by 
which this file will be referenced in the foreach block. The `input` 
attribute's value shall be either True or False. Related files that
are specified as input files will have a default directory of the 
foreach directory, but this may be overridden by specifying the `in__dir` 
attribute of the `<related>` file. Output related files will default 
to the pipeline output directory unless overridden with the `in__dir` 
attribute. The `pattern` and `replace` attributes specify Python regex 
patterns which are used to modify the controlling filename into the 
desired filename as if by Python's `re.sub()` function.

The operations specified by the `<step>` tag(s) will be executed for 
each set of files identified.

####example

Assume that the directory /home/example contains the following files, 
output from an Illumina sequencing run, that we now need to align with 
our pipeline:

    A2_S1_L001_R1_001.fastq  
    A2_S1_L001_R1_002.fastq  
    A2_S1_L001_R2_001.fastq  
    A2_S1_L001_R2_002.fastq

 
This foreach construct would match these fastq pairs, and execute a BWA 
alignment for each pair:

    <dir id="indir" input="True" filespec="/home/example" />  
    <foreach dir="indir">  
        <file id="end1" pattern=".*_R1_.*fastq" />  
        <related id="end2" input="True" pattern="(.*)_R1_(.*fastq)" replace="\1_R2_\2" />  
        <related id="sam" input="False" pattern="(.*)_R1_(.*)fastq" replace="\1_\2sam" />  
        <step name="Alignment">  
            <tool name="bwa" description="run_bwa.xml" input="end1,end2" output="sam" />  
        </step>  
    </foreach>

The `<foreach>` tag would result in two invocations of the Alignment 
step, the first processing `A2_S1_L001_R1_001.fastq` and 
`A2_S1_L001_R2_001.fastq` and producing output file 
`A2_S1_L001_001.sam`. The second invocation would process 
`A2_S1_L001_R1_002.fastq` and `A2_S1_L001_R2_002.fastq`, producing 
`A2_S1_L001_002.sam`.

***

###tool

A tool takes one or more inputs, and creates one or more outputs. For 
instance, it could be an aligner, or a simple sam to bam converter.

    <tool name="..."  
        input="id [, id]..."  
        output="id [, id]..."  
        description="..."  
        walltime="..."/>

The description attribute specifies the XML file which describes the 
tool and its parameters; see Tool XML Description section, below. This 
can be a filename, in which case the default search location for the 
file is the directory containing the pipeline XML file. The 
description can also specify relative or absolute path information. If 
the description does not specify an absolute path, then the search path 
can be altered by the `CIVET_PATH` environment variable (if using 
`civet_run`; if using the Python API this can be passed using the 
`search_path` parameter to `parse_XML()`) or through the 
`default_search_path` pipeline attribute. `CIVET_PATH` and 
default_search_path may contain a colon-delimited list of directories 
to search. The search order is as follows 1) `CIVET_PATH`, 2) pipeline 
`default_search_path`, 3) pipeline XML directory. Many of our own 
production pipelines do not use `civet_run`, rather they have their own 
pipeline driver script that calls `parse_XML()` directly. This gives us 
tighter control and allows us to simplify the interface (for example, 
in that case we can place the driver script in the user's path and
unlike with `civet_run` the user does not need to know the path to the 
XML defining the pipeline). If a pipeline has its own driver script 
then it is up to the developer to determine if they want to implement 
`CIVET_PATH` for their script. Some developers like to share tool 
efinitions between multiple pipeline. One way to accomplish that is to 
place tool definitions in a shared repository and set CIVET_PATH.

The `input` and `output` attributes are used to map from the pipeline 
file id space to a tool file id space. In this way, a tool description 
file can be utilized in multiple pipelines or steps without having to 
have a global id assignment registry.

The ids listed in the input or output attributes are in the pipeline / 
step id space. The order in the list determines the id they will have 
in the tool's description file. The first id in the list in this tag
will receive the id "in_1" or "out_1", as appropriate, in the tool 
description XML file. Input and output lists use separate indexes, so 
the first file in each list will be _1, the second in each list will 
be _2, etc.

The `walltime` attribute is optional, and is used to override the 
walltime specified in the Tool's XML definition file.

##Tool XML Description

A tool XML description file is separate from a pipeline description 
file. This allows utilizing a tool description in multiple pipelines, 
and also conveniently tweaking a tool's configuration without editing
a whole pipeline file.

The outer tag is `<tool>`.

###tool

    <tool name="..." tool_config_prefix="..." threads="..."  
      walltime="..." error_strings="..." mem="..." exit_if_exists="."  
      exit_test_logic="..." path="...">  
        <description />  
        <option />  
        <command />  
        <file />  
        <validate />  
        <module />  
    </tool>

The `tool_config_prefix`, `path`, `threads`, `mem`, and `walltime` 
attributes are optional. The optional `path` attribute specifies path 
information to be prepended onto the user's PATH at runtime for the job
submitted to run this tool. This is prepended after any modulefiles are 
loaded. Any relative path is relative to the directory containing the 
tool's XML definition file. If not present, `threads` defaults to one
`walltime` defaults to one hour, and `mem` defaults to the batch system 
default (typically unlimited). The `mem` attribute specifies the amount 
of physical memory to request for the job; it is specified in gigabyes 
and must be an integer.

The `exit_if_exists` attribute allows a tool developer to have a tool 
exit immediately if a file exists. This attribute can be a 
comma-delimited list of `<file>` ids. There must be a `<file>` with a 
matching id defined within the `<tool>`. If the value is a comma 
delimited list of files, then the default behavior is to combine these 
file tests with a logical AND. This can be changed with the 
`exit_test_logic` attribute. This attribute can take the value "AND" 
or "OR" (default value AND).

See the `<option>` tag for a description of the `tool_config_prefix` 
attribute.

The `threads` attribute specifies the maximum number of threads any 
command in this tool will use. If not specified the value will default 
to 1. If executing in a cluster environment, it specifies the number 
of processors that will be allocated. If the commands in a tool run 
for a significant length of time and use widely different numbers of 
threads, consider splitting it into multiple tools within one step, so 
that each tool can specify the number of threads appropriate to its 
command(s).

**IMPORTANT NOTE:** Civet currently only supports 'single node' jobs.
With TORQUE, Civet will request `nodes=1:ppn=num_threads` for jobs 
submitted to execute this tool (where num_threads is the value of the 
`threads` attribute). Future version may allow multi-node requests (for 
example, to allow tools that make use of MPI to distribute computing 
across multiple cluster nodes).

The `error_strings` attribute allows specifying a comma-separated list 
of strings. If specified and the tool's stderr output contains any of
the strings, the tool is deemed to have failed, even if its last 
command returns a zero exit status.

There may be zero or more `<option>` tags. There must be one or more 
`<command>` tags. If multiple `<command>` tags are specified, they are 
executed serially, in lexical order. The `<description>` tag is 
optional.

***

###description

The `<description>` tag contains free form information about the tool.

    <description> ... </description>

***

###option
The `<option>` tag is designed to specify command line parameters and 
their values in a way that allows those values to be derived from a 
file, from a tool attribute (the `threads` attribute), or as a hard 
coded default value. Options can be overridden. Option overriding is 
described in a separate document.

The information in the `<option>` tag is used in the `<command>` tag.

    <option name="..." from_file="..." command_text="..." binary="..."  
        threads="..." value="..." />

The `<option>` tag must contain the `name` attribute and one of the 
`from_file`, `value`, or `threads` attributes. The `command_text` 
attribute is combined with the `value` or `threads` attribute. See the 
description of the `<command>` tag for how these are used. The `binary` 
attribute is combined with the `value` attribute and indicates that the 
value can be True or False. If the value is true, the `command_text` 
will be used to substitute for the option in the command line. If the 
value is false then an empty string will be substituted for the option 
in the command line.

Option names are in the same name space as the tool file ids, but 
separate from the name space of the invoking pipeline. All names in 
the tool's option name / file id name space must be unique.

Occasionally, the value associated with an option is complex and is 
derived from the processing being done in the pipeline. An example is 
specifying read group information during BWA alignment. In this case, 
the `from_file` attribute specifies the id of a file containing a 
single line which is used as the option's value instead of what would
have been specified in the `command_text` and value attributes.

The `threads` attribute, if set to "True", will use the tool's
`threads` attribute as the option value. This allows you to specify the 
number of threads in the command without having to enter the number of 
threads in multiple places (the tool's `threads` attribute and in the
`<commmand>`). It also allows the number of threads to be overridden. 

For each option specified, if the `tool_config_prefix` attribute is 
specified in the `<tool>` tag, option processing will search for an 
configuration file in the directory containing the pipeline XML file, 
or specified on the `civet_run` command line. If the configuration 
file exists and the option's name is listed with a matching prefix, the
value from the configuration file will be used instead of the value 
specified in this tag. A full description of configuration processing 
is in a separate document.

***

###command

The `<command>` tag specifies how to construct the command line that 
will be executed.

    <command program="..." delimiters="..." stdout_id="..." 
      stderr_id="..." if_exists="..." if_not_exists="..." 
      if_exists_logic="...">
        ...
    </command>

The command line to be executed will be constructed from the value of 
the program attribute followed by the text contained within the tag.

The text within this tag consists of literal text that will be inserted 
in the command line after the program filename, interspersed with 
option names or file ids enclosed within braces ({}). Option names in 
this context means the values of the name attributes in the `<option>` 
tags.

If a command to be executed uses braces in its syntax (e.g., the find 
command), it is necessary to specify an alternate set of delimiters or 
an escaping mechanism. This specification allows for alternate 
delimiters. If the optional `delimiters` attribute is specified, its 
value is a two character string. The first character is used in place 
of open brace ({) to indicate the start of an option name, and the 
second character is used in place of close brace (}). If a literal 
brace is required in the command line, use the delimiters attribute to 
specify an alternate pair of characters.

The optional `stdout_id` and `stderr_id` attributes allow IO 
redirection. Specifying a file id for one of these attributes will 
result in that output stream to be redirected to the file specified.

The `if_exists` and `if_not_exists` attributes add conditional 
execution to a command. Each of these optional attributes take a 
comma-delimited list of file ids. In the case of `if_exists`, the 
command will only execute if those file(s) exist at run time. In the 
case of `if_not_exists`, the command will only execute if the file(s) 
do not exist at run time. When multiple files are passed to one of 
these attributes in a comma delimited list the default logical operator 
used to join them is AND. This can be overridden with the 
`if_exists_logic` attribute. This attribute can take "AND" or "OR" as a 
value (case insensitive). 

When a brace-enclosed option name is encountered, the value of the 
`command_text` attribute will be inserted, if present. Then the value 
of the 'value' attribute if specified, and finally the filename
represented by the id attribute, if specified. But see the 
specification of tool configuration files for processing that overrides 
the values specified in the XML file.

When a brace-enclosed option name is encountered where the 
corresponding `<option>` tag uses the `command_text` / `value` form, 
Civet will insert it its place the value of the `command_text` 
attribute followed by a space, followed by the value of the `value` 
attribute into the command line. If the value of the `command_text` 
attribute ends with an equals ('=') or colon (':'), the space will be
omitted. If the corresponding `<option>` tag uses the `from_file` form, 
then the contents of the specified file will replace the brace-enclosed 
option name in the command line. When a brace-enclosed file id is 
encountered, the path of the corresponding `<file>` tag is inserted in 
it's place into the command line.

All of the text within the `<command> </command>` tags will be 
reconstructed as a single line, with any line breaks treated as spaces 
and all spaces collapsed. 

####Example 1

The following is a fragment from a tool description file, intended to 
illustrate the use of an option in a command line, not to be complete. 
The fragment
    
    <tool>  
        <option name="bowtie_max_multi" command_text="-m" value="40" />  
        <command program="bowtie">{bowtie_max_multi} ... </command>


would result in the following being emitted for the command:

    bowtie –m 40 ...

####Example 2
The following fragment demonstrates use of a file id being used in a 
command. This fragment assumes that the file "fred.sam" was passed as 
id "out_1" into the tool. The fragment:

    <command program="bowtie">–s ... {out_1}</command>

would result in the following being emitted:

    bowtie –s ... fred.sam

####Example 3

The following fragment demonstrates use of the delimiters attribute. It 
assumes that the pipeline's outputdir is named "myoutput" and its id
was passed as id out_3. The fragment:

    <command delimiters="%%" program="find">
        %out_3% -name "*.tmp" –exec rm {} \+
    </command>

would result in this command being executed:

    find myoutput –name="*.tmp" –exec rm {} \+

***

###validate

The `<validate>` tag allows us to specify a file for validation, that 
it has not been altered since the pipeline was initially validated. 
This does not need to be specified for the command program names; they 
are automatically added to the list. However, for java jar files, etc., 
that aren't the initial word in a command, we can check them this way.
The tag has two forms:
    
    <validate>filepath</validate>  
    <validate id="..." />

In the first form, the file is searched for by name on the PATH, or in 
the current working directory. In the second form, the file is checked 
for using the "in_X" and "out_X" file IDs passed into the tool.

***

###version_command

One of the requirements on this pipeline tool is repeatability. Part of 
that is the ability to log the versions of tools that are used. When 
specified within a `<command>` tag, the `<version_command>` tag allows 
us to specify a command that will cause the tool to emit its version 
string to either stdout or stderr.

    <version_command output="..."> ... </version_command>

The value of the `output` attribute shall be either "stdout" or 
"stderr". This identifies the IO channel on which the result of the 
command is printed.  If not specified, defaults to "stdout".

If this tag is specified, the version information for the tool will be 
recorded in a version log file, which is written to the pipeline's log
directory.

Example:

    <version_command output="stdout">java -version</version_command>

The command string may need to be a pipe, if the command puts out too 
much or not enough information to be useful in succinctly identifying 
the program version. Two more examples:

    <version_command>  
        java -jar {gatk} –help | grep "(GATK)"  
    </version_command>  

    <version_command>  
        echo -n "BWA "; bwa 2>&amp;1 | grep Version  
    </version_command>

Note the use of the XML string `&amp;` to represent an ampersand!

***

###file
A tool definition `<file>` tag is analogous to the pipeline definition 
`<tool>` tag. See the description above. When specified in the context 
of a tool definition file, the parameter form is not allowed. If a 
temporary file is declared in the context of a tool, that temporary 
file is deleted at the end of the tool's execution.
