============
Installation
============

At the command line either via easy_install or pip::

    $ easy_install esub-epipe
    $ pip install esub-epipe

Or, if you have virtualenvwrapper installed::

    $ mkvirtualenv esub-epipe
    $ pip install esub-epipe

(optional) If run-mpi mode should be used also require a local openmpi environement.
Example of how to setup a working MPI environemnt (tested for Linux Ubuntu 18.04 LTS)::

    $ apt-get install lam-runtime mpich openmpi-bin slurm-wlm-torque
    $ pip install mpi4py
