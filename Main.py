#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import argparse
import datetime
import os
import psutil  # type: ignore
import ray
import time

from pkt_kg.downloads import OntData, LinkedData
from pkt_kg.edge_list import CreatesEdgeList
from pkt_kg.knowledge_graph import FullBuild, PartialBuild, PostClosureBuild


def main():
    parser = argparse.ArgumentParser(description=('PheKnowLator: This program builds a biomedical knowledge graph using'
                                                  ' Open Biomedical Ontologies and linked open data. The program takes '
                                                  'the following arguments:'))
    parser.add_argument('-p', '--cpus', help='# workers to use; defaults to use all available cores', default=None)
    parser.add_argument('-g', '--onts', help='name/path to text file containing ontologies', required=True)
    parser.add_argument('-e', '--edg', help='name/path to text file containing edge sources', required=True)
    parser.add_argument('-a', '--app', help='construction approach to use (i.e. instance or subclass)', required=True)
    parser.add_argument('-t', '--res', help='name/path to text file containing resource_info', required=True)
    parser.add_argument('-b', '--kg', help='build type: "partial", "full", or "post-closure"', required=True)
    parser.add_argument('-r', '--rel', help='yes/no - adding inverse relations to knowledge graph', required=True)
    parser.add_argument('-s', '--owl', help='yes/no - removing OWL Semantics from knowledge graph', required=True)
    parser.add_argument('-m', '--nde', help='yes/no - adding node metadata to knowledge graph', required=True)
    parser.add_argument('-o', '--out', help='name/path to directory where to write knowledge graph', required=True)
    args = parser.parse_args()

    ######################
    #### READ IN DATA ####
    ######################

    # STEP 1: CREATE INPUT DOCUMENTS
    # see https://github.com/callahantiff/PheKnowLator/wiki/Dependencies page for how to prepare input data files

    # STEP 2: DOWNLOAD AND PREPROCESS DATA
    # see the 'Data_Preparation.ipynb' and 'Ontology_Cleaning.ipynb' file for examples and guidelines

    # STEP 3: DOWNLOAD ONTOLOGIES
    print('\n' + '=' * 40 + '\nPKT: DOWNLOADING DATA: ONTOLOGY DATA\n' + '=' * 40 + '\n')
    start = time.time()
    ont = OntData(data_path=args.onts, resource_data=args.res)
    # ont = OntData(data_path='resources/ontology_source_list.txt', resource_data='resources/resource_info.txt')
    ont.downloads_data_from_url()
    end = time.time(); timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print('\nPKT: TOTAL SECONDS TO DOWNLOAD ONTOLOGIES: {} @ {}'.format(end - start, timestamp))

    # STEP 4: DOWNLOAD EDGE DATA SOURCES
    print('\n' + '=' * 37 + '\nPKT: DOWNLOADING DATA: CLASS DATA\n' + '=' * 37 + '\n')
    start = time.time()
    ent = LinkedData(data_path=args.edg, resource_data=args.res)
    # ent = LinkedData(data_path='resources/edge_source_list.txt', resource_data='resources/resource_info.txt')
    ent.downloads_data_from_url()
    end = time.time(); timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print('\nPKT: TOTAL SECONDS TO DOWNLOAD NON-ONTOLOGY DATA: {} @ {}'.format(end - start, timestamp))

    #####################
    # CREATE EDGE LISTS #
    #####################

    # set-up environment
    cpus = psutil.cpu_count(logical=False) if args.cpus is None else args.cpus
    ray.init(ignore_reinit_error=True)  # preventing clobbering background ray processes

    print('\n' + '=' * 28 + '\nPKT: CONSTRUCT EDGE LISTS\n' + '=' * 28 + '\n')
    start = time.time()
    combined_edges = dict(ent.data_files, **ont.data_files)
    # master_edges = CreatesEdgeList(data_files=combined_edges, source_file='resources/resource_info.txt')
    master_edges = CreatesEdgeList(data_files=combined_edges, source_file=args.res)
    master_edges.runs_creates_knowledge_graph_edges(source_file=args.res, data_files=combined_edges, cpus=cpus)
    end = time.time(); timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print('\nPKT: TOTAL SECONDS TO BUILD THE MASTER EDGE LIST: {} @ {}'.format(end - start, timestamp))

    del ont, ent, master_edges  # clean up environment before build knowledge graph

    #########################
    # BUILD KNOWLEDGE GRAPH #
    #########################

    print('\n' + '=' * 33 + '\nPKT: BUILDING KNOWLEDGE GRAPH\n' + '=' * 33 + '\n')
    start = time.time()

    if args.kg == 'partial':
        kg = PartialBuild(construction=args.app,
                          node_data=args.nde,
                          inverse_relations=args.rel,
                          decode_owl=args.owl,
                          cpus=args.cpus,
                          write_location=args.out)
    elif args.kg == 'post-closure':
        kg = PostClosureBuild(construction=args.app,
                              node_data=args.nde,
                              inverse_relations=args.rel,
                              decode_owl=args.owl,
                              cpus=args.cpus,
                              write_location=args.out)
    else:
        kg = FullBuild(construction=args.app,
                       node_data=args.nde,
                       inverse_relations=args.rel,
                       decode_owl=args.owl,
                       cpus=args.cpus,
                       write_location=args.out)
    kg.construct_knowledge_graph()

    # ray.shutdown()  # uncomment if running this independently of the CI/CD builds
    end = time.time(); timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print('\nPKT: TOTAL SECONDS TO CONSTRUCT A KG: {} @ {}'.format(end - start, timestamp))


if __name__ == '__main__':
    main()
