#!/usr/bin/env python
# -*- coding: utf-8 -*-


# import needed libraries
import subprocess


def runs_deepwalk(input_file, output_file, threads, dim, nwalks, walklen, window, nprwalks, lr):
    """Performs path embedding of a knowledge graph edge list using the DeepWalk algorithm.
    https://github.com/xgfs/deepwalk-c

    Args:
        input_file (str): A file path/file storing an RDF graph.
        output_file (str): A string containing the name and file path to write out results.
        threads (int): An integer specifying the number of workers to dedicate to running the process.
        dim (int): An integer specifying the number of dimensions for the resulting embeddings.
        nwalks (int): An integer specifying the number of walks to perform.
        walklen (int): An integer specifying the length of walks.
        window (int): An integer specifying the number of dimensions for the window.
        nprwalks (nt): Number of random walks for HSM tree (default 100)
        lr (float): Initial learning rate

    Returns:
        None.
    """

    print('\n\n' + '=' * len('Running DeepWalk Algorithm'))
    print('Running DeepWalk-C Algorithm')
    print('=' * len('Running DeepWalk Algorithm'))

    outputargs = '_{dim}_{nwalks}_{walklen}_{window}_{nprwalks}_{lr}.txt'.format(dim=dim, nwalks=nwalks,
                                                                                 walklen=walklen,
                                                                                 window=10,
                                                                                 nprwalks=nprwalks,
                                                                                 lr=lr)

    # set command line argument
    try:
        subprocess.check_call(['./deepwalk-c/src/deepwalk',
                               '-input' + str(input_file),
                               '-output' + output_file + outputargs,
                               '-threads' + str(threads),
                               '-dim' + str(dim),
                               '-nwalks' + str(nwalks),
                               '-walklen' + str(walklen),
                               '-window' + str(window),
                               '-nprwalks' + str(nprwalks),
                               '-lr' + str(lr),
                               '-verbose 2'])

    except subprocess.CalledProcessError as error:
        print(error.output)

    return None
