#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import argparse

import scripts.python.data_sources
import scripts.python.edge_dictionary
from scripts.python.knowledge_graph import *


def main():
    parser = argparse.ArgumentParser(description=('PheKnowLator: This program builds a biomedical knowledge graph using'
                                                  ' Open Biomedical Ontologies and linked open data. The program takes '
                                                  'the following arguments:'))
    parser.add_argument('-g', '--onts', help='name/path to text file containing ontologies', required=True)
    parser.add_argument('-c', '--cls', help='name/path to text file containing class sources', required=True)
    parser.add_argument('-i', '--inst', help='name/path to text file containing instance sources', required=True)
    parser.add_argument('-t', '--res', help='name/path to text file containing resource_info', required=True)
    parser.add_argument('-b', '--kg', help='the build, can be "partial", "full", or "post-closure"', required=True)
    parser.add_argument('-o', '--out', help='name/path to directory where to write knowledge graph', required=True)
    parser.add_argument('-n', '--nde', help='yes/no - adding node metadata to knowledge graph', required=True)
    parser.add_argument('-r', '--rel', help='yes/no - adding inverse relations to knowledge graph', required=True)
    parser.add_argument('-s', '--owl', help='yes/no - removing OWL Semantics from knowledge graph', required=True)

    args = parser.parse_args()

    ######################
    #### READ IN DATA ####
    ######################

    # STEP 1: CREATE INPUT DOCUMENTS
    # NOTE: please see the https://github.com/callahantiff/PheKnowLator/wiki/Dependencies page for instructions on
    # how to prepare input data files

    # STEP 2: PREPROCESS DATA
    # see the '.scripts/python/Data_Preparation.ipynb' file for instructions

    # STEP 3: PROCESS ONTOLOGIES
    print('\n' + '=' * 33 + '\nDOWNLOADING DATA: ONTOLOGY DATA\n' + '=' * 33 + '\n')
    ont = scripts.python.data_sources.OntData(data_path=args.onts)
    # ont = scripts.python.data_sources.OntData(data_path='resources/ontology_source_list.txt')
    ont.downloads_data_from_url('imports')
    ont.writes_source_metadata_locally()

    # STEP 4: PROCESS CLASS EDGES
    print('\n' + '=' * 33 + '\nDOWNLOADING DATA: CLASS DATA\n' + '=' * 33 + '\n')
    cls = scripts.python.data_sources.Data(data_path=args.cls)
    # cls = scripts.python.data_sources.Data(data_path='resources/class_source_list.txt')
    cls.downloads_data_from_url('')
    cls.writes_source_metadata_locally()

    # STEP 5: PROCESS INSTANCE EDGES
    print('\n' + '=' * 33 + '\nDOWNLOADING DATA: INSTANCE DATA\n' + '=' * 33 + '\n')
    inst = scripts.python.data_sources.Data(data_path=args.inst)
    # inst = scripts.python.data_sources.Data(data_path='resources/instance_source_list.txt')
    inst.downloads_data_from_url('')
    inst.writes_source_metadata_locally()

    #####################
    # CREATE EDGE LISTS #
    #####################

    print('\n' + '=' * 33 + '\nPROCESSING EDGE DATA\n' + '=' * 33 + '\n')

    # STEP 1: create master resource dictionary
    combined_edges = dict(dict(cls.data_files, **inst.data_files), **ont.data_files)
    # master_edges = scripts.python.edge_dictionary.EdgeList(data_files=combined_edges,
    #                                                       source_file='./resources/resource_info.txt')
    master_edges = scripts.python.edge_dictionary.EdgeList(data_files=combined_edges, source_file=args.res)
    master_edges.creates_knowledge_graph_edges()

    #########################
    # BUILD KNOWLEDGE GRAPH #
    #########################

    print('\n' + '=' * 33 + '\nBUILDING KNOWLEDGE GRAPH\n' + '=' * 33 + '\n')

    # set some vars for writing data
    # kg = scripts.python.knowledge_graph.PartialBuild(kg_version='v2.0.0',
    #                                                  write_location='./resources/knowledge_graphs',
    #                                                  edge_data='./resources/Master_Edge_List_Dict.json',
    #                                                  node_data='yes',
    #                                                  relations_data='no',
    #                                                  remove_owl_semantics='no')

    if args.kg == 'partial':
        kg = scripts.python.knowledge_graph.PartialBuild(kg_version='v2.0.0',
                                                         write_location=args.out,
                                                         edge_data='./resources/Master_Edge_List_Dict.json',
                                                         node_data=args.nde,
                                                         inverse_relations=args.rel,
                                                         remove_owl_semantics=args.owl)
    elif args.kg == 'post-closure':
        kg = scripts.python.knowledge_graph.PostClosureBuild(kg_version='v2.0.0',
                                                             write_location=args.out,
                                                             edge_data='./resources/Master_Edge_List_Dict.json',
                                                             node_data=args.nde,
                                                             inverse_relations=args.rel,
                                                             remove_owl_semantics=args.owl)
    else:
        kg = scripts.python.knowledge_graph.FullBuild(kg_version='v2.0.0',
                                                      write_location=args.out,
                                                      edge_data='./resources/Master_Edge_List_Dict.json',
                                                      node_data=args.nde,
                                                      inverse_relations=args.rel,
                                                      remove_owl_semantics=args.owl)

    kg.construct_knowledge_graph()


if __name__ == '__main__':
    main()
