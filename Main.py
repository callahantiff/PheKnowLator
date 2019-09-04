#!/usr/bin/env python
# -*- coding: utf-8 -*-


# import needed libraries
import argparse

import scripts.python.DataSources
import scripts.python.EdgeDictionary
from scripts.python.KnowledgeGraph import *
from scripts.python.KnowledgeGraphEmbedder import *


def main():
    parser = argparse.ArgumentParser(description='PheKnowLator: This program builds a biomedical knowledge graph using'
                                                 'Open Biomedical Ontologies and linked open data. The programs takes'
                                                 'the following arguments:')
    parser.add_argument('-o', '--onts', help='name/path to text file containing ontologies', required=True)
    parser.add_argument('-c', '--cls', help='name/path to text file containing class sources', required=True)
    parser.add_argument('-i', '--inst', help='name/path to text file containing instance sources', required=True)
    args = parser.parse_args()

    ######################
    # READ IN DATA #
    ######################
    # NOTE: for classes and instances you will be prompted to enter a file name for each source. Please use the
    # following pattern: edge_source_datatype_source_type.txt --> gene-pathway_string_instance_evidence.txt

    # STEP 1: BIOPORTAL MAPS
    # get mapping between CHEBI and MESH
    # run python/NCBO_rest_api.py file to get mappings between ChEBI and MESH, which writes to ./resources/text_files/

    # STEP 2: PROCESS ONTOLOGIES
    print('\nPROCESSING DATA: ONTOLOGY DATA\n')
    ont = scripts.python.DataSources.OntData(args.onts)
    # ont = scripts.python.DataSources.OntData('resources/ontology_source_list.txt')
    ont.parses_resource_file()
    ont.downloads_data_from_url('imports')
    ont.generates_source_metadata()
    ont.writes_source_metadata_locally()

    # STEP 3: PROCESS CLASS EDGES
    print('\nPROCESSING DATA: CLASS DATA\n')
    cls = scripts.python.DataSources.Data(args.cls)
    # cls = scripts.python.DataSources.Data('resources/class_source_list.txt')
    cls.parses_resource_file()
    cls.downloads_data_from_url('')
    cls.generates_source_metadata()
    cls.writes_source_metadata_locally()

    # STEP 4: PROCESS INSTANCE EDGES
    print('\nPROCESSING DATA: INSTANCE DATA\n')
    inst = scripts.python.DataSources.Data(args.inst)
    # inst = scripts.python.DataSources.Data('resources/instance_source_list.txt')
    inst.parses_resource_file()
    inst.downloads_data_from_url('')
    inst.generates_source_metadata()
    inst.writes_source_metadata_locally()

    #####################
    # CREATE EDGE LISTS #
    #####################

    # STEP 1: create master resource dictionary
    combined_edges = dict(dict(cls.data_files, **inst.data_files), **ont.data_files)
    master_edges = scripts.python.EdgeDictionary.EdgeList(combined_edges, './resources/resource_info.txt')
    master_edges.creates_knowledge_graph_edges()

    # # save nested edges locally
    with open('./resources/kg_master_edge_dictionary.json', 'w') as filepath:
        json.dump(master_edges.source_info, filepath)

    # load existing master_edge dictionary
    # with open('./resources/kg_master_edge_dictionary.json', 'r') as filepath:
    #     master_edges = json.load(filepath)

    #########################
    # BUILD KNOWLEDGE GRAPH #
    #########################

    # STEP 1: set-up vars for file manipulation
    ont_files = './resources/ontologies/'
    merged_onts = ont_files + 'merged_ontologies/'

    # create list of ontologies to merge
    ontology_list = [
        [ont_files + 'go_with_imports.owl', ont_files + 'hp_with_imports.owl', merged_onts + 'hp_go_merged.owl'],
        [merged_onts + 'hp_go_merged.owl', ont_files + 'chebi_lite.owl', merged_onts + 'hp_go_chebi_merged.owl'],
        [merged_onts + 'hp_go_chebi_merged.owl', ont_files + 'vo_with_imports.owl', merged_onts +
         'PheKnowLator_v2_MergedOntologies_BioKG.owl']
    ]

    # merge ontologies
    merges_ontologies(ontology_list)

    # STEP 2: make edge lists
    # set file path
    ont_kg = './resources/knowledge_graphs/'

    # separate edge lists by data type
    master_edges = master_edges.source_info.copy()
    class_edges = {}
    other_edges = {}

    for edge in master_edges.keys():
        if master_edges[edge]['data_type'] == 'class-instance' or master_edges[edge]['data_type'] == 'instance-class':
            class_edges[edge] = master_edges[edge]
        else:
            other_edges[edge] = master_edges[edge]

    # create class-instance edges
    class_kg = creates_knowledge_graph_edges(class_edges,
                                             'class',
                                             Graph().parse(merged_onts + 'PheKnowLator_v2_MergedOntologies_BioKG.owl'),
                                             ont_kg + 'PheKnowLator_v2_ClassInstancesOnly_BioKG.owl',
                                             kg_class_iri_map={})

    # create instance-instance and class-class edges
    class_inst_kg = creates_knowledge_graph_edges(other_edges,
                                                  'other',
                                                  class_kg,
                                                  ont_kg + 'PheKnowLator_v2_Full_BioKG.owl')

    # STEP 3: remove disjoint axioms
    removes_disointness_axioms(class_inst_kg, ont_kg + 'PheKnowLator_v2_Full_BioKG_NoDisjointness.owl')

    # STEP 4: deductively close graph
    closes_knowledge_graph(ont_kg + 'PheKnowLator_v2_Full_BioKG_NoDisjointness.owl',
                           'elk',
                           ont_kg + 'PheKnowLator_v2_Full_BioKG_NoDisjointness_Closed_ELK.owl')

    # STEP 5: remove metadata nodes
    removes_metadata_nodes(Graph().parse(ont_kg + 'PheKnowLator_v2_Full_BioKG_NoDisjointness_Closed._ELKowl'),
                           ont_kg + 'PheKnowLator_v2_Full_BioKG_NoDisjointness_ELK_Closed_NoMetadataNodes.owl',
                           ont_kg + 'PheKnowLator_v2_ClassInstancesOnly_BioKG_ClassInstanceMap.json')

    # STEP 6: convert triples to ints
    maps_str_to_int(Graph().parse(ont_kg + 'PheKnowLator_v2_Full_BioKG_NoDisjointness_ELK_Closed_NoMetadataNodes.owl'),
                    ont_kg + 'PheKnowLator_v2_Full_BioKG_NoDisjointness_Closed_ELK_Triples_Integers.txt',
                    ont_kg + 'PheKnowLator_v2_Full_BioKG_Triples_Integer_Labels_Map.json')

    ##############################
    # KNOWLEDGE GRAPH EMBEDDINGS #
    ##############################
    # set file path
    embed_path = './resources/embeddings/'

    runs_deepwalk_embedder(ont_kg + 'PheKnowLator_v2_Full_BioKG_NoDisjointness_Closed_ELK_Triples_Integers.txt',
                           embed_path + 'PheKnowLator_v2_Full_BioKG_DeepWalk_Embeddings.txt',
                           63, 512, 10, 100, 20)


if __name__ == '__main__':
    main()
