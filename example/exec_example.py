# Copyright (C) 2019 ETH Zurich, Institute for Particle Physics and Astrophysics
# Author: Dominik Zuercher

# exec_example.py

# This is an example for an executable that can be ran by esub

import argparse
import time
import numpy as np
import os


# the file should include at least the main function
# each function takes a list of integer indices as well as a parser object ARGS which can be used to
# pass arguments to the function from the command line directly

def resources(args):
    # The resource function can be used to tell esub which resources to allocate (only used if
    # job is submitted to queing system). Gets overwritten by commmand line arguments.
    return dict(main_memory=10000,
                main_time_per_index=4,
                main_scratch=25000,
                watchdog_memory=15000,
                watchdog_time=24,
                merge_memory=6000,
                merge_time=8)


def check_missing(indices, args):
    # The check_missing function. Per default esub will rerun all jobs that crashed. But in some
    # cases one might want to check if the jobs produced the desired output and rerun them
    # if the output is corrupted. The check_missing function is providing this functionality.

    random_seed, output_directory = setup(args)

    corrupted = []
    for index in indices:
        nums = np.load("{}/randoms_{}.npy".format(output_directory, index))
        if nums.size != 3:
            corrupted.append(index)

    return corrupted


def setup(args):
    # The setup function gets executed first before esub starts (useful to create directories for example)

    description = "This is an example script that can be used by esub"
    parser = argparse.ArgumentParser(description=description, add_help=True)
    parser.add_argument('--random_seed', type=int,
                        action='store', default=30, help='Some random seed')
    parser.add_argument('--output_directory', type=str, action='store',
                        default='.', help='Where to write output files to')
    args = parser.parse_args(args)

    return args.random_seed, args.output_directory


def main(indices, args):

    random_seed, output_directory = setup(args)

    np.random.seed(random_seed)

    for index in indices:
        # put here what you wish to do for the task with index 'index'
        # if mode has been chosen to be mpi or run-mpi. index is always 0 and this is just an MPI pool

        print("This is main index {}".format(index))

        nums = np.zeros(0)
        while nums.size < 3:
            num = np.random.randint(1000000)
            print("Generated random number: {}".format(num))
            nums = np.append(nums, num)
            time.sleep(0.5)

        np.save("{}/randoms_{}.npy".format(output_directory, index), nums)
        print("Saved output to file {}/randoms_{}.npy".format(output_directory, index))

        # IMPORTANT: In order for rerun missing to work properly have to add yield index in index loop of main function!!!
        yield index


def watchdog(indices, args):
    # The watchdog runs along the main function on a single thread executing all indices. Meant to collect files on the
    # fly for example.

    random_seed, output_directory = setup(args)

    total_nums = np.zeros(0)

    for index in indices:
        # put here what you wish to do for the task with index 'index'
        print("This is watchdog index {}".format(index))
        print(
            "Waiting to collect file {}/randoms_{}.npy...".format(output_directory, index))

        while not os.path.isfile("{}/randoms_{}.npy".format(output_directory, index)):
            time.sleep(5)

        nums = np.load("{}/randoms_{}.npy".format(output_directory, index))
        total_nums = np.append(total_nums, nums)

    np.save("{}/all_randoms.npy".format(output_directory), total_nums)
    print("Saved output to file {}/all_randoms.npy".format(output_directory))


def merge(indices, args):
    # The merge runs after the main function on a single thread executing all indices. Meant to finalize output files.

    random_seed, output_directory = setup(args)

    # in this example we completely ignore the indices and just run a single job on the file made by the watchdog.

    print("This is merge")

    nums = np.load("{}/all_randoms.npy".format(output_directory))

    print("The mean is {}".format(np.mean(nums)))
    print("The standard deviation is {}".format(np.std(nums)))
