#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Knowledge Graph Utility Functions.

Interacts with OWL Tools API
* gets_ontology_statistics
* merges_ontologies
* ontology_file_formatter
"""

# import needed libraries
import glob
import os
import os.path
import subprocess

from typing import List


def gets_ontology_statistics(file_location: str, owltools_location: str = './pkt_kg/libs/owltools') -> None:
    """Uses the OWL Tools API to generate summary statistics (i.e. counts of axioms, classes, object properties, and
    individuals).

    Args:
        file_location: A string that contains the file path and name of an ontology.
        owltools_location: A string pointing to the location of the owl tools library.

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
        output = subprocess.check_output([os.path.abspath(owltools_location), file_location, '--info'])

    # print stats
    res = output.decode('utf-8').split('\n')[-5:]
    cls, axs, op, ind = res[0].split(':')[-1], res[3].split(':')[-1], res[2].split(':')[-1], res[1].split(':')[-1]
    sent = '\nThe knowledge graph contains {0} classes, {1} axioms, {2} object properties, and {3} individuals\n'

    print(sent.format(cls, axs, op, ind))

    return None


def merges_ontologies(ontology_files: List[str], write_location: str, merged_ont_kg: str,
                      owltools_location: str = './pkt_kg/libs/owltools') -> None:
    """Using the OWLTools API, each ontology listed in in the ontologies attribute is recursively merged with into a
    master merged ontology file and saved locally to the provided file path via the merged_ontology attribute. The
    function assumes that the file is written to the directory specified by the write_location attribute.

    Args:
        ontology_files: A list of ontology file paths.
        write_location: A string pointing to a local directory for writing data.
        merged_ont_kg: A string pointing to the location of the merged ontology file.
        owltools_location: A string pointing to the location of the owl tools library.

    Returns:
        None.
    """

    if not ontology_files:
        # gets_ontology_statistics(write_location + merged_ont_kg, owltools_location)
        return None
    else:
        if write_location + merged_ont_kg in glob.glob(write_location + '/*.owl'):
            ont1, ont2 = ontology_files.pop(), write_location + merged_ont_kg
        else:
            ont1, ont2 = ontology_files.pop(), ontology_files.pop()

        try:
            print('\nMerging Ontologies: {ont1}, {ont2}'.format(ont1=ont1.split('/')[-1], ont2=ont2.split('/')[-1]))

            subprocess.check_call([os.path.abspath(owltools_location), str(ont1), str(ont2),
                                   '--merge-support-ontologies',
                                   '-o', write_location + merged_ont_kg])
        except subprocess.CalledProcessError as error:
            print(error.output)

        return merges_ontologies(ontology_files, write_location, merged_ont_kg)


def ontology_file_formatter(write_location: str, full_kg: str, owltools_location: str = './pkt_kg/libs/owltools') -> \
        None:
    """Reformat an .owl file to be consistent with the formatting used by the OWL API. To do this, an ontology
    referenced by graph_location is read in and output to the same location via the OWLTools API.

    Args:
        write_location: A string pointing to a local directory for writing data.
        full_kg: A string containing the subdirectory and name of the the knowledge graph file.
        owltools_location: A string pointing to the location of the owl tools library.

    Returns:
        None.

    Raises:
        TypeError: If something other than an .owl file is passed to function.
        IOError: If the graph_location file is empty.
        TypeError: If the input file contains no data.
    """

    print('\n*** Applying OWL API Formatting to Knowledge Graph OWL File ***')
    graph_write_location = write_location + full_kg

    # check input owl file
    if '.owl' not in graph_write_location:
        raise TypeError('ERROR: The provided file is not type .owl')
    elif not os.path.exists(graph_write_location):
        raise IOError('The {} file does not exist!'.format(graph_write_location))
    elif os.stat(graph_write_location).st_size == 0:
        raise TypeError('ERROR: input file: {} is empty'.format(graph_write_location))
    else:
        try:
            subprocess.check_call([os.path.abspath(owltools_location),
                                   graph_write_location,
                                   '-o', graph_write_location])
        except subprocess.CalledProcessError as error:
            print(error.output)

    return None
