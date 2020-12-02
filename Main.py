#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import argparse
import datetime
import os
import time

from pkt_kg.downloads import OntData, LinkedData
from pkt_kg.edge_list import CreatesEdgeList
from pkt_kg.knowledge_graph import FullBuild, PartialBuild, PostClosureBuild


def main():
    parser = argparse.ArgumentParser(description=('PheKnowLator: This program builds a biomedical knowledge graph using'
                                                  ' Open Biomedical Ontologies and linked open data. The program takes '
                                                  'the following arguments:'))
    parser.add_argument('-g', '--onts', help='name/path to text file containing ontologies', required=True)
    parser.add_argument('-e', '--edg', help='name/path to text file containing edge sources', required=True)
    parser.add_argument('-a', '--app', help='construction approach to use (i.e. instance or subclass', required=True)
    parser.add_argument('-t', '--res', help='name/path to text file containing resource_info', required=True)
    parser.add_argument('-b', '--kg', help='the build, can be "partial", "full", or "post-closure"', required=True)
    parser.add_argument('-o', '--out', help='name/path to directory where to write knowledge graph', required=True)
    parser.add_argument('-n', '--nde', help='yes/no - adding node metadata to knowledge graph', required=True)
    parser.add_argument('-r', '--rel', help='yes/no - adding inverse relations to knowledge graph', required=True)
    parser.add_argument('-s', '--owl', help='yes/no - removing OWL Semantics from knowledge graph', required=True)
    parser.add_argument('-m', '--kgm', help='yes/no - adding node metadata to knowledge graph', required=True)

    args = parser.parse_args()

    ######################
    #### READ IN DATA ####
    ######################

    # STEP 1: CREATE INPUT DOCUMENTS
    # NOTE: please https://github.com/callahantiff/PheKnowLator/wiki/Dependencies page for details on how to prepare
    # input data files

    # STEP 2: PREPROCESS DATA
    # see the 'Data_Preparation.ipynb' file for instructions

    # STEP 3: PROCESS ONTOLOGIES
    print('\n' + '=' * 33 + '\nDOWNLOADING DATA: ONTOLOGY DATA\n' + '=' * 33 + '\n')
    start = time.time()

    ont = OntData(data_path=args.onts, resource_data=args.res)
    # ont = OntData(data_path='resources/ontology_source_list.txt', resource_data='./resources/resource_info.txt')
    ont.downloads_data_from_url()

    end = time.time()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print('\nTOTAL SECONDS TO DOWNLOAD ONTOLOGIES: {} @ {}'.format(end-start, timestamp))

    # STEP 4: PROCESS EDGE DATA
    print('\n' + '=' * 33 + '\nDOWNLOADING DATA: CLASS DATA\n' + '=' * 33 + '\n')
    start = time.time()

    ent = LinkedData(data_path=args.edg, resource_data=args.res)
    # ent = LinkedData(data_path='resources/edge_source_list.txt', resource_data='./resources/resource_info.txt')
    ent.downloads_data_from_url()

    end = time.time()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print('\nTOTAL SECONDS TO DOWNLOAD NON-ONTOLOGY DATA: {} @ {}'.format(end - start, timestamp))

    #####################
    # CREATE EDGE LISTS #
    #####################

    print('\n' + '=' * 33 + '\nPROCESSING EDGE DATA\n' + '=' * 33 + '\n')
    start = time.time()

    # STEP 1: create master resource dictionary
    combined_edges = dict(ent.data_files, **ont.data_files)
    # master_edges = CreatesEdgeList(data_files=combined_edges, source_file='./resources/resource_info.txt')
    master_edges = CreatesEdgeList(data_files=combined_edges, source_file=args.res)
    master_edges.creates_knowledge_graph_edges()

    end = time.time()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print('\nTOTAL SECONDS TO BUILD THE MASTER EDGE LIST: {} @ {}'.format(end - start, timestamp))

    #########################
    # BUILD KNOWLEDGE GRAPH #
    #########################

    print('\n' + '=' * 33 + '\nBUILDING KNOWLEDGE GRAPH\n' + '=' * 33 + '\n')
    start = time.time()

    if args.kg == 'partial':
        kg = PartialBuild(kg_version='v2.0.0',
                          write_location=args.out,
                          construction=args.app,
                          edge_data='./resources/Master_Edge_List_Dict.json',
                          node_data=args.nde,
                          inverse_relations=args.rel,
                          decode_owl=args.owl,
                          kg_metadata_flag=args.kgm)
    elif args.kg == 'post-closure':
        kg = PostClosureBuild(kg_version='v2.0.0',
                              write_location=args.out,
                              construction=args.app,
                              edge_data='./resources/Master_Edge_List_Dict.json',
                              node_data=args.nde,
                              inverse_relations=args.rel,
                              decode_owl=args.owl,
                              kg_metadata_flag=args.kgm)
    else:
        kg = FullBuild(kg_version='v2.0.0',
                       write_location=args.out,
                       construction=args.app,
                       edge_data='./resources/Master_Edge_List_Dict.json',
                       node_data=args.nde,
                       inverse_relations=args.rel,
                       decode_owl=args.owl,
                       kg_metadata_flag=args.kgm)

    kg.construct_knowledge_graph()

    end = time.time()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print('\nTOTAL SECONDS TO CONSTRUCT A KG: {} @ {}'.format(end - start, timestamp))


if __name__ == '__main__':
    main()


gets_ontology_statistics(processed_data_location + 'human_pro_closed.owl')
