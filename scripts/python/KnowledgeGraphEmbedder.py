#!/usr/bin/env python
# -*- coding: utf-8 -*-


# import needed libraries
import json
import numpy as np
import re
import subprocess

from tqdm import tqdm


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


def processes_embedded_nodes(file_list, original_edges, kg_node_int_label_map):
    """Takes a list of file paths that map to embedding files saved in binary compressed sparse row graph format
    converts them into numpy arrays  and then writes them to the same directory as text files.

    ASSUMPTION: A filename must have the following ordering:
        file_name_128_100_20_10_100_001.out, where:
            any combination of strings can be used before the first '_#'
            128 - dimensions (assumes dimensions are 2-3 digits long)
            100 - # walks
            20 - walk length
            10 - window
            100 - # random walks for HSM tree
            001 - learning rate (with '.' removed)

    Args:
        file_list (lst): A list of file paths.
        original_edges (str): A string naming the filepath to the original edge list.
        kg_node_int_label_map (str): A string naming the name and file path to write out results.

    Returns:
        None.
    """

    print('\n' + '=' * 75)

    # iterate over the embedding files and convert the data to numpy array
    for file in tqdm(file_list):

        print('\nProcessing Embedding File: {0}'.format(file.split('/')[-1]))

        # read map to convert node integers back to labels
        node_labeler = dict(map(reversed, json.loads(open(kg_node_int_label_map).read()).items()))

        # read in data and reverse mapping created by deepwalk-c
        original_nodes = set(x for y in [line.strip('\n').split('\t') for line in open(original_edges, 'r')] for x in y)

        # read in embedding data
        dim = int(re.search(r'\d{2,3}_', file).group(0).strip('_'))
        original_embeddings = np.fromfile(file, np.float32).reshape(len(original_nodes), dim)

        # re-order embeddings and write to drive
        out_loc = open('.' + file.split('.')[1] + '_formatted.txt', 'w')

        for nodes in sorted(list(dict(zip(sorted(map(int, original_nodes)), range(len(original_nodes)))).items())):
            out_loc.write(node_labeler[nodes[0]] + '\t' + str(list(original_embeddings[nodes[1]])).strip('[|]') + '\n')

        out_loc.close()

    print('=' * 75 + '\n')

    return None
