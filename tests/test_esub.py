# Copyright (C) 2019 ETH Zurich, Institute for Particle Physics and Astrophysics
# Author: Joerg Herbel

import os
import shutil
import shlex
import subprocess
import pytest


def run_exec_example(test_dir, mode='run', tasks_string=None,
                     extra_esub_args=''):

    # path to example file
    path_example = 'example/exec_example.py'

    # build command
    cmd = 'esub {} --mode={} --output_directory={} {}'.format(path_example,
                                                              mode, test_dir,
                                                              extra_esub_args)
    if tasks_string is not None:
        cmd += ' --tasks={}'.format(tasks_string)

    # main function
    subprocess.call(shlex.split(cmd))
    subprocess.call(shlex.split(cmd + ' --function=main'))

    # rerun_missing
    subprocess.call(shlex.split(cmd + ' --function=rerun_missing'))

    # watchdog
    subprocess.call(shlex.split(cmd + ' --function=watchdog'))

    # merge
    subprocess.call(shlex.split(cmd + ' --function=merge'))

    # all functions
    subprocess.call(shlex.split(cmd + ' --function=all'))


def test_esub():

    # create directory for test output
    path_testdir = 'esub_test_dir'
    if not os.path.isdir(path_testdir):
        os.mkdir(path_testdir)

    # test with no tasks provided
    run_exec_example(path_testdir)

    # test with single task
    run_exec_example(path_testdir, tasks_string='99')

    # test with list of tasks
    run_exec_example(path_testdir, tasks_string='10,2,4')

    # test with range
    run_exec_example(path_testdir, tasks_string='"1 > 3"')

    # test run-tasks
    run_exec_example(path_testdir, mode='run-tasks',
                     tasks_string='"0 > 4"', extra_esub_args='--n_cores=2')

    # remove directory for test output
    shutil.rmtree(path_testdir)

    # test another example,
    # this time with renamed main function and missing merge function
    # works because main is renamed
    subprocess.call(shlex.split(
        'esub tests/executable_test.py --function=main'))
    # works because main is renamed
    subprocess.call(shlex.split(
        'esub tests/executable_test.py --function=all'))

    with pytest.raises(subprocess.CalledProcessError):
        subprocess.check_call(shlex.split(
            'esub tests/executable_test.py --function=main \
            --main_name=main_renamed'))

    # check that log directory was created and remove it then
    log_dir = 'esub_logs'
    assert os.path.isdir(log_dir)
    shutil.rmtree(log_dir)
