.. _examples:

========
Examples
========

Some basic example use cases
============================

All the example scripts used here are included in the sections below for illustration.

Running 10 tasks in the active shell serially. Also runs the watchdog and merge functions. something argument gets passed to the executable::

    $ esub exec_example.py --tasks='0 > 10' --function=all --mode=run --something=10

Submitting 10 tasks to a queing system using 5 cores (each core executes 2 tasks). Also runs the watchdog, merge and rerun-missing functions. something argument gets passed to the executable. The job has a maximum runtime of 10h and allocates 20GB of RAM for each core::

    $ esub exec_example.py --tasks='0 > 10' --n_cores=5 --function=all --mode=jobarray --something=10 --main_time=10 --main_memory=20000

Submitting a single MPI job using 5 cores. Something gets passed to the executable::

    $ esub exec_example.py --tasks='0' --n_cores=5 --function=main --mode=mpi --something=10

Running a single MPI job locally using 5 cores. Something gets passed to the executable::

    $ esub exec_example.py --tasks='0' --n_cores=5 --function=main --mode=run-mpi --something=10

Running 21 tasks in the active shell in parallel using 10 of the cores on the local machine (each core running 2 jobs except for one running 3). something argument gets passed to the executable::

    $ esub exec_example.py --tasks='0 > 21' --function=main --mode=run-tasks --something=10

Submitting a whole pipeline with an arbitrary number of jobs, dependencies and loops to a queing system::

    $ epipe pipeline_example.yaml

.. _exec_example:

esub executable example
=======================

Below is an example of an executable script that can be used by esub. Please
check the :ref:`usage` section to find an explanation for the different elements.

.. literalinclude:: ../example/exec_example.py
   :language: python

.. _pipeline_example:

epipe pipeline example
======================

Below is an example of an epipe pipeline file that can be used by epipe. Please
check the :ref:`usage` section to find an explanation for the different elements.

.. literalinclude:: ../example/pipeline_example.yml
   :language: bash

.. _source_example:

source file example
===================

This is an example of a source file (simple shell script) that can be used by
esub to set up the environement for the task that one wants to run.

.. literalinclude:: ../example/source_file_example.sh
   :language: bash
