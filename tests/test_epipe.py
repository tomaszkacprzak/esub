# Copyright (C) 2019 ETH Zurich, Institute for Particle Physics and Astrophysics
# Author: Joerg Herbel

import os
import shutil
import shlex
import subprocess


def test_epipe():

    # create directory for test output
    path_testdir = os.path.join(os.getcwd(), 'esub_test_dir')
    if not os.path.isdir(path_testdir):
        os.mkdir(path_testdir)

    # run epipe
    cmd = 'epipe tests/pipe.yaml --ignore_jobid_errors'
    subprocess.call(shlex.split(cmd))

    # remove directory for test output
    shutil.rmtree(path_testdir)

    # check that log files were created and remove them
    log_dir = 'esub_logs'
    job_files = ['job1_done.dat',
                 'loopjob1__0_done.dat',
                 'loopjob1__1_done.dat',
                 'loopjob2__0_done.dat',
                 'loopjob2__1_done.dat']

    for job_file in job_files:
        print(os.path.join(log_dir, job_file))
        assert os.path.isfile(os.path.join(log_dir, job_file))

    shutil.rmtree(log_dir)
