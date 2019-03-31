##########################################################################################
# main.py
# Purpose: script to download OWL ontology files and store source metadata
# version 1.1.0
# date: 07.28.2018
# Python 3.6.2
##########################################################################################


# import needed libraries
import argparse
import scripts.python.DataSources_ontologies
import scripts.python.DataSources_data
import scripts.python.EdgeDictionary
from scripts.python.KnowledgeGraph import *



''' TO DO LIST
1. fix and finish testing + add configurations
2. edit data source bash script- meaning, how do you want the different sources to be processed?
3. Add progress bar to class methods
'''




def main():
    parser = argparse.ArgumentParser(description='OpenBioGraph: This program builds a biomedical knowledge graph '
                                                 'using Open Biomedical Ontologies and other sources of open '
                                                 'biomedical data. Built on Semantic Web Technologies, the programs '
                                                 'takes the inputs specified below and outputs')
    parser.add_argument('-o', '--onts', help='name/path to text file containing ontologies', required=True)
    parser.add_argument('-c', '--cls', help='name/path to text file containing class sources', required=True)
    parser.add_argument('-i', '--inst', help='name/path to text file containing instance sources', required=True)
    # parser.add_argument('-f', '--res', help='name/path to text file containing list of entity edge relations',
    #                                         required=True)
    args = parser.parse_args()

    ######################
    # READ IN DATA #
    ######################
    # read in ontologies
    ont = scripts.python.DataSources_ontologies.OntData(args.onts)
    ont = scripts.python.DataSources_ontologies.OntData("resources/ontology_source_list.txt")
    ont.file_parser()
    ont.get_data_type()
    ont.get_source_list()
    ont.url_download()
    ont.get_data_files()
    ont.source_metadata()
    ont.get_source_metadata()
    ont.write_source_metadata()

    # read in class data
    cls = scripts.python.DataSources_data.Data(args.cls)
    cls = scripts.python.DataSources_data.Data("resources/class_source_list.txt")
    cls.file_parser()
    cls.get_data_type()
    cls.get_source_list()
    cls.url_download()
    cls.get_data_files()
    cls.source_metadata()
    cls.get_source_metadata()
    cls.write_source_metadata()

    # read in class and instance data
    inst = scripts.python.DataSources_data.Data(args.inst)
    inst = scripts.python.DataSources_data.Data("resources/instance_source_list.txt")
    inst.file_parser()
    inst.get_data_type()
    inst.get_source_list()
    inst.url_download()
    inst.get_data_files()
    inst.source_metadata()
    inst.get_source_metadata()
    inst.write_source_metadata()

    ######################
    # CREATE EDGE LISTS #
    ######################
    # create class-instance and instance-class-instance edges
    edges = scripts.python.EdgeDictionary.EdgeList(cls.get_data_files(), cls.get_data_type())
    source_dict_cls = edges.get_edge_dics()

    for i in cls.get_data_files():
        print(i)
        print(len(source_dict_cls[i]))
        print(sum([len(x) for x in source_dict_cls[i].values()]))
        print('\n')

    # create instance-instance edge lists
    edges2 = scripts.python.EdgeDictionary.EdgeList(inst.get_data_files()[:-1], inst.get_data_type())
    source_dict_inst = edges2.get_edge_dics()

    for i in inst.get_data_files()[:-1]:
        print(i)
        print(len(source_dict_inst[i]))
        print(sum([len(x) for x in source_dict_inst[i].values()]))
        print('\n')

    ######################
    # CONSTRUCT KG #
    ######################
    # merge ontologies - function from scripts.python.KnowledgeGraph
    OntologyMerger("./resources/ontologies/go_with_imports.owl",
                   "./resources/ontologies/hp_with_imports.owl",
                   './resources/ontologies/generated_ontologies/hp+go')

    # create class-instance and class instance-instance edges
    edge_dict = source_dict_cls
    graph_output = './resources/ontologies/generated_ontologies/hp+go_merged_instances.owl'
    iri_map_output = './resources/graphs/class_instance_map.json'
    input_graph = "./resources/ontologies/generated_ontologies/hp+go_merged.owl"
    KG = classEdges(edge_dict, graph_output, input_graph, iri_map_output)

    # create instance-instance edges in KG
    graph = KG
    edge_dict = source_dict_inst
    output = './resources/ontologies/generated_ontologies/hp+go_merged_instances_full.owl'
    KG = instanceEdges(graph, edge_dict, output)

    # remove disjoint axioms
    graph = KG
    output = './resources/ontologies/generated_ontologies/hp+go_merged_instances_full_nodisjoint.owl'
    removeDisointness(graph, output)

    # deductively close graph
    graph = 'resources/ontologies/generated_ontologies/hp+go_merged_instances_full_nodisjoint.owl'
    output = 'resources/ontologies/generated_ontologies/hp+go_merged_instances_full_nodisjoint'
    reasoner = 'elk'
    CloseGraph(graph, reasoner, output)

    graph = Graph()
    graph.parse('resources/ontologies/generated_ontologies/hp+go_merged_instances_full_nodisjoint_elk.owl')

    # write out edge list from graph
    input_graph = 'resources/ontologies/generated_ontologies/hp+go_merged_instances_full_nodisjoint_elk.owl'
    output = './resources/graphs/KG_triples.txt'
    iri_mapper = './resources/graphs/class_instance_map.json'
    KGEdgeList(input_graph, output, iri_mapper)

    # convert triples to ints
    graph = './resources/graphs/KG_triples.txt'
    output_trip_ints = './resources/graphs/KG_triples_ints.txt'
    output_map = './resources/graphs/KG_triples_ints_map.json'
    NodeMapper(graph, output_trip_ints, output_map)



if __name__ == '__main__':
    main()