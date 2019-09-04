#!/usr/bin/env python
# -*- coding: utf-8 -*-


# import needed libraries
import subprocess


def runs_deepwalk_embedder(input_loc, output_loc, workers, dimensions, window, walks, walk_length):
    """Performs path embedding of a knowledge graph edge list using the DeepWalk algorithm.

    Args:
        input_loc(str): A file path/file storing an RDF graph.
        output_loc (str): A string containing the name and file path to write out results.
        workers (int): An integer specifying the number of workers to dedicate to running the process.
        dimensions (int): An integer specifying the number of dimensions for the resulting embeddings.
        window (int): An integer specifying the number of dimensions for the window.
        walks (int): An integer specifying the number of walks to perform.
        walk_length (int): An integer specifying the length of walks.

    Returns:
        None.
    """

    print('\n\n' + '=' * len('Running DeepWalk Algorithm'))
    print('Running DeepWalk Algorithm')
    print('=' * len('Running DeepWalk Algorithm'))

    # set command line argument
    try:
        subprocess.check_call(['./walking-rdf-and-owl-master/deepwalk',
                               '--workers' + str(workers),
                               ' --representation-size' + str(dimensions),
                               '--format edgelist' + str(input_loc),
                               '--output' + str(output_loc),
                               '--window' + str(window),
                               '--number-walks' + str(walks),
                               '--walk-length' + str(walk_length)])

    except subprocess.CalledProcessError as error:
        print(error.output)

    return None
