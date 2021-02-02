.. _usage:

========
Usage
========

esub
====

To run esub use the following syntax::

    $ esub PYTHON-EXECUTABLE [further esub arguments] [arguments passed to EXECUTABLE]

Note that as long as you do not start jobs with the same job_name from the same working
directory esub will never confuse your jobs so you can run as many as you want.
If you still start multiple jobs with the same name from the same directory it will not
crash your jobs but the log files might just be messed up since all jobs write to the same 
log.

A short explanation of the arguments:

PYTHON-EXECUTABLE
-----------------

esub will search in this file for the main function. Per default the main function is named 'main', but
its name can be changed using the --main_name flag.
In addition esub will search for some special functions namely: resources, watchdog, merge, check_missing.
The file can also contain other functions but esub will ignore them.
For an example of how such a file should look please have a look at :ref:`exec_example`.

Further esub arguments
----------------------

esub has a variety of additional arguments that we describe here

- mode:

    default: run

    choices: run, jobarray, mpi, run-mpi, run-tasks

    The mode in which to operate. See below under :ref:`esub_modes` for a description.

- function:

    default: main

    choices: main, watchdog, merge, rerun_missing, all

    The functions that should be executed. See below under :ref:`esub_functions` for a description.

- main_name:

    default: main

    The name of the main function in the executable file that esub will search for.

- tasks:

    default: 0

    Task string from which the task indices are parsed.
    Either single index, list of indices (eg [0,1,2]),  range (eg 0 > 10) or a path to a file containing such a list or range.

- n_cores:

    default: 1

    The number of cores to request for the main jobs (watchdog and merge always run on only one core).

- job_name:

    default: job

    Individual name for this job (determines name of the log files and name of the job submitted to a queing system).

- source_file:

    default: source_esub.sh

    Optionally provide a shell file which gets sourced before running any job (used to load modules,
    declaring environemental variables and so on when submitting to a queing system).
    See :ref:`source_example` for an example of such a script.

- dependency:

    default: ''

    A dependency string that gets added to the dependencies (meant for chaining multiple jobs after each other, see :ref:`epipe`).

- system:

    default: bsub

    choices: bsub

    Type of the queing system (so far only IBM's bsub is supported).

Resource allocation (overwrites resources function in executable file):

- main_memory:

    default: 1000

    Memory allocated per core for main job in MB.

- main_time:

    default: 4

    Job run time limit in hours for main job.

- main_time_per_index:

    default: None

    Job run time limit in hours for main job per index, overwrites main_time if set.

- main_scratch:

    default: 2000

    Local scratch allocated for main job (only relevant if node scratch should be used).

- watchdog_memory:

    default: 1000

    Memory allocated for watchdog job in MB.

- watchdog_time:

    default: 4

    Job run time limit in hours for watchdog job.

- watchdog_scratch:

    default: 2000

    Local scratch allocated for watchdog job (only relevant if node scratch should be used).

- merge_memory:

    default: 1000

    Memory allocated for merge job in MB.

- merge_time:

    default: 4

    Job run time limit in hours for merge job.

- merge_scratch:

    default: 2000

    Local scratch allocated for merge job (only relevant if node scratch should be used).

Arguments passed to EXECUTABLE
------------------------------

All arguments unknown to esub are automatically passed on
to the functions in the PYTHON-EXECUTABLE file in form of an argparse object.

.. _esub_functions:

esub's functions
----------------

The python executable file must contain at least a main function. Additionaly esub will search for the following functions: resources, watchdog, merge.
The file can also contain other functions but esub will ignore them.
If --function=all is given esub will all the functions. Additonaly, the built in function rerun-missing will be ran as well.
Rerun-missing checks if some of the main functions have failed and will rerun them before the merge function is executed (this was introduced due to
some clusters having random memory leaks which can cause a few jobs to fail in an unpredictable way).
For an example of how such a file should look please have a look at :ref:`exec_example`.

- Main function:

    The main function. It receives a list of task indices as an argument and will run on each index one after another. If multiple cores are allocated esub will
    split the list equally to the different cores.

- Watchdog function:

    The watchdog function runs on a single core. Depending on the mode it will run alongside the main functions
    or after the main functions have finished (in this case
    it serves the same purpose as the merge function). It runs on the full list of all task indices.
    The watchdog function is meant for collecting output files that the main functions are writting on the fly (or other things you can come up with :).

- Merge function:

    The merge function runs on a single core. It will always only start when all the main functions and the watchdog function have already finished.
    It runs on the full list of all task indices. The merge function is meant for postprocessing of the data produced by the main and watchdog functions,
    such as calculating means and errors.

- Resources function:

    The resources function can be used to assign the computational resources for the jobs (see :ref:`exec_example`) for an example of the syntax).
    It does not have to be declared to be ran explicitly. If it is present in the executable file it will be ran.

- Setup function:

    Yet another special function. If esub finds a function called setup in your executable it will run it first before doing anything else. This is useful
    to create output directories for example.

.. _esub_modes:

esub's modes
------------


What makes esub very convenient to use is that you write your executable file only once and it can then be ran in all different modes of esub.
This means you do not have to worry about parallelization and so on.
Here we present the different modes of esub:

- run:

    In run mode everything is ran on a single core serially and locally (no job is submitted to the queing system).
    This mode is meant for debugging of your code or lightweight code that you just want to test on your own computer or so.
    Ignores the n_cores parameter.

.. image:: pictures/run_mode.png

- run-tasks:

    The run-tasks mode allows you to split the load of the main function onto multiple local cores.
    This mode is meant to run not so heavy code on your local machines or small servers using the power of multiprocessing.
    Note that the cores are independent -> There is no communication between the cores

.. image:: pictures/run-tasks_mode.png

- run-mpi:

    The run-mpi mode allows you to run a local MPI job. It will run just a single main job (ignoring the tasks parameter) with index 0 but on all the cores.
    This mode is meant to run lightweight MPI jobs on your local machine or on a small server such as MCMC chains for example.
    Note that in MPI mode the cores can communicate with each other.
    NOTE: you need a working MPI environement set up in order to use this mode.

.. image:: pictures/run-mpi_mode.png

- mpi:

    This mode assumes that there is a queing system present. It will submit a single MPI job running only on the index 0 (ignoring the tasks parameter) to the queing system.
    This mode is meant for heavy MPI jobs such as long MCMC sampling for example.

.. image:: pictures/mpi_mode.png

- jobarray:

    This mode assumes that there is a queing system present. It will split the load of the main jobs equally onto the n_cores. This mode allows you to easily parallelize
    your jobs. There is no communication between the cores though.
    Note that jobarrays will go through the queing system much faster than MPI jobs, so unless you absolutely need communication between the cores this mode should be prefered.

.. image:: pictures/jobarray_mode.png

esub's jobchainer mode for backwards compatibility
--------------------------------------------------

To run in this mode simply add -jc flag to the esub or epipe command. For epipe -jc, all jobs in the pipe will receive this flag.
The jobchainer mode preallocates jobs for reruns, the number of jobs is currently hardcoded to be 10% of main job. For example with n_reruns=2, the job stack will look like:
main[0-1000]
missing[1]
main[0-100]
missing[1]
main[0-100]
merge[1]
The scripts have to implement "main", "missing", and "merge" functions. The "missing" function has to be implemented by the user and returns a list of indices that have not been completed.
The merge function can be used as watchdog (starts after first job of main is started) or like regular merge (after last rerun is finished.)
This is controlled by the argument --merge_depenency_mode='along' (watchdog) or 'after' (regular merge).
Jobchainer uses a indices tracking system with yaml files in a folder esub_indices - do not delete or modify this folder during runs.

.. _epipe:

epipe
=====


epipe is a subtool of esub that allows you to chain multiple esub jobs after each other.
This is very useful if you want to run a long pipeline on a cluster.

The only thing needed to run epipe is a YAML file containing the information about the different jobs
that should be ran.

Each job instance consists of a job name, the esub command that should be submitted as well as some
optional dependencies (can either be names of other jobs or job IDs).
The job will only start running once the jobs listed in its dependencies have finished.

epipe also allows you to specify loops and the loop index can be passed to the commands.

One more feature is that you can specify global variables which can then be inserted into the commands.

The general syntax for running epipe is::

    $ epipe PIPELINE.yaml

Please refer to the :ref:`pipeline_example` file for an example of how such a file should look like.

Convenience scripts
===================

There are two scipts to facilitate job management, both located in esub.scripts: check_logs and send_cmd.

check_logs
----------

Can be executed via ::

    $ python -m esub.scripts.check_logs --dirpath_logs=[directory to check]

Checks all esub log files located in the given directory and prints the names of the log files which contain
unfinished jobs.

send_cmd
--------

Can be executed via ::

    $ python -m esub.scripts.send_cmd --dirpath_logs=[directory to check] --cmd=[cmd to send] --log_filter=[logs to include]

Sends a command to all jobs logged in all log files located in the given directory. The command could for example be
"bkill" to terminate these jobs. Optionally, the log_filter argument allows to only include jobs logged in files
whose names contain a given string.
