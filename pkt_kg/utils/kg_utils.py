#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Knowledge Graph Utility Functions.

Interacts with OWL Tools API
* gets_ontology_statistics
"""

# import needed libraries
import os
import subprocess


def gets_ontology_statistics(file_location: str) -> None:
    """Uses the OWL Tools API to generate summary statistics (i.e. counts of axioms, classes, object properties, and
    individuals).

    Args:
        file_location: A string that contains the file path and name of an ontology.

    Returns:
        None.

    Raises:
        TypeError: If the file_location is not type str.
        OSError: If file_location points to a non-existent file.
        ValueError: If file_location points to an empty file.
    """

    if not isinstance(file_location, str):
        raise TypeError('ERROR: file_location must be a string')
    elif not os.path.exists(file_location):
        raise OSError('The {} file does not exist!'.format(file_location))
    elif os.stat(file_location).st_size == 0:
        raise ValueError('FILE ERROR: input file: {} is empty'.format(file_location))
    else:
        output = subprocess.check_output(['./pkt_kg/libs/owltools', file_location, '--info'])

    # print stats
    res = output.decode('utf-8').split('\n')[-5:]
    cls, axs, op, ind = res[0].split(':')[-1], res[3].split(':')[-1], res[2].split(':')[-1], res[1].split(':')[-1]
    sent = '\nThe knowledge graph contains {0} classes, {1} axioms, {2} object properties, and {3} individuals\n'

    print(sent.format(cls, axs, op, ind))

    return None
