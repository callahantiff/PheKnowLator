##########################################################################################
# main.py
# Purpose: script to download OWL ontology files and store source metadata
# Python 3.6.2
##########################################################################################


# import needed libraries
import argparse

import scripts.python.DataSources
import scripts.python.EdgeDictionary
from scripts.python.KnowledgeGraph import *



''' TO DO LIST
1. fix and finish testing + add configurations
2. edit data source bash script- meaning, how do you want the different sources to be processed?
3. Add progress bar to class methods
'''


def main():
    parser = argparse.ArgumentParser(description='PheKnowLator: This program builds a biomedical knowledge graph '
                                                 'using Open Biomedical Ontologies and other sources of open '
                                                 'biomedical data. Built on Semantic Web Technologies, the program '
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
    # ont = scripts.python.drafts.DataSources_ontologies.OntData(args.onts)
    ont = scripts.python.DataSources.OntData('resources/ontology_source_list.txt')
    ont.file_parser()
    ont.get_data_type()
    ont.get_source_list()
    ont.url_download()
    ont.get_data_files()
    ont.source_metadata()
    ont.get_source_metadata()
    ont.write_source_metadata()

    # read in class data
    # cls = scripts.python.DataSources_data.Data(args.cls)
    cls = scripts.python.DataSources.Data('resources/class_source_list.txt')
    cls.file_parser()
    cls.get_data_type()
    cls.get_source_list()
    cls.url_download()
    cls.get_data_files()
    cls.source_metadata()
    cls.get_source_metadata()
    cls.write_source_metadata()

    # read in class and instance data
    inst = scripts.python.drafts.DataSources_data.Data(args.inst)
    inst = scripts.python.drafts.DataSources_data.Data('resources/instance_source_list.txt')
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
    OntologyMerger('./resources/ontologies/go_with_imports.owl',
                   './resources/ontologies/hp_with_imports.owl',
                   './resources/ontologies/generated_ontologies/hp+go')

    # create class-instance and class instance-instance edges
    edge_dict = source_dict_cls
    graph_output = './resources/ontologies/generated_ontologies/hp+go_merged_instances.owl'
    iri_map_output = './resources/graphs/class_instance_map.json'
    input_graph = './resources/ontologies/generated_ontologies/hp+go_merged.owl'
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
    graph = 'hp+go_merged_instances_full_nodisjoint.owl'
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


# import needed libraries
from rdflib import Namespace
from rdflib import Graph
from rdflib import URIRef
import re
import urllib
import glob


def edge_loader(updated_edge):
    edit_list = []

    for idx, link in updated_edge.items():
        if len(link) > 1:
            if idx == '0':
                for i in link:
                    edit_list.append((i, updated_edge['1'][0], updated_edge['2'][0]))
            elif idx == '1':
                for j in link:
                    edit_list.append((updated_edge['0'][0], j, updated_edge['2'][0]))
            else:
                for k in link:
                    edit_list.append((updated_edge['0'][0], updated_edge['1'][0], k))
        else:
            edit_list.append((updated_edge['0'][0], updated_edge['1'][0], updated_edge['2'][0]))

        break

    return edit_list


def gene2protein(entity):
    params = {'from': 'P_ENTREZGENEID',
              'to': 'ID',
              'format': 'tab',
              'query': ' '.join(set([x.split('/')[-1].strip('>') for x in entity]))}

    data = urllib.parse.urlencode(params).encode('utf-8')
    request = urllib.request.Request('https://www.uniprot.org/uploadlists/', data)
    request.add_header('User-Agent', 'Python %s' % 'tiffany.callahan@ucdenver.edu')
    response = urllib.request.urlopen(request)
    results = response.read()

    # create dictionary mapping
    gene_dict = {}

    for res in str(results).split('\\n')[1:]:
        i = res.split('\\t')
        if len(i) > 1:
            gene = 'http://purl.uniprot.org/geneid/' + str(i[0])
            protein = 'https://www.uniprot.org/uniprot/' + str(i[1].split('_')[0])

            if gene in gene_dict.keys():
                gene_dict[gene].append(protein)
            else:
                gene_dict[gene] = [protein]

    return gene_dict


# read in graphs
# graph = Graph()
# graph.parse('./resources/ontologies/v1.0.0/doid_with_imports.owl')
# graph.parse('./resources/ontologies/v1.0.0/hp_with_imports.owl')
# graph.parse('./resources/ontologies/v1.0.0/go_with_imports.owl')

# read in ignacio's edges (n=674675)
it_edges = []
for file in glob.glob('./resources/graphs/ignacio_edges/*.txt'):
    f = open(file, 'r').read().split('\n')
    it_edges += f
it_edges = list(set(it_edges))
len(it_edges)

# ignacio_edge_types = {}
# edges = set()
#
# for edge in filter(None, it_edges):
#     triple = edge.strip(' ').split(' ')
#     print(edge)
#
#     edges.add(triple[1])
#
#     if 'obo' in triple[0] and 'obo' not in triple[2]:
#         s = triple[0].split('/')[-1].split('_')[0]
#         if 'uniprot' in triple[2]:
#             o = triple[2].split('/')[3]
#         else:
#             o = '/'.join(triple[2].split('/')[:-1])
#     elif 'obo' not in triple[0] and 'obo' in triple[2]:
#         if 'uniprot' in triple[0]:
#             s = triple[0].split('/')[3]
#         else:
#             s = '/'.join(triple[0].split('/')[:-1])
#         o = triple[2].split('/')[-1].split('_')[0]
#     elif 'obo' in triple[0] and 'obo' in triple[2]:
#         s = triple[0].split('/')[-1].split('_')[0]
#         o = triple[2].split('/')[-1].split('_')[0]
#     else:
#         if 'uniprot' in triple[2]:
#             s = triple[2].split('/')[3]
#         else:
#             s = '/'.join(triple[2].split('/')[:-1])
#         if 'uniprot' in triple[0]:
#             o = triple[0].split('/')[3]
#         else:
#             o = '/'.join(triple[0].split('/')[:-1])
#
#     if triple[1] in ignacio_edge_types:
#         ignacio_edge_types[triple[1]] |= set([str(s) + '-' + str(o)])
#     else:
#         ignacio_edge_types[triple[1]] = set([str(s) + '-' + str(o)])
#
#
# ignacio_edge_types['<AOPwiki_upstream_of>']
# ignacio_edge_types['<http://purl.obolibrary.org/obo/RO_0000056>']
# ignacio_edge_types['<http://purl.obolibrary.org/obo/RO_0000085>']
# ignacio_edge_types['<http://purl.obolibrary.org/obo/RO_0001025>']
# ignacio_edge_types['<http://purl.obolibrary.org/obo/RO_0002211>']
# ignacio_edge_types['<http://purl.obolibrary.org/obo/RO_0002434>']
# ignacio_edge_types['<http://purl.obolibrary.org/obo/RO_0002436>']
# ignacio_edge_types['<http://www.w3.org/2000/01/rdf-schema#subClassOf>']


# read in my edges and delete mesh terms and gene-gene interactions (n=895056)
tc_edges = set()
for edge in open('./resources/graphs/v1.0.0/KG_triples.txt').read().split('.\n')[:-1]:
    triple = edge.strip(' ').split(' ')
    if 'mesh' not in triple[0]:
        if 'geneid' not in triple[2]:
            tc_edges.add(edge)

tc_edges = list(tc_edges)
len(tc_edges)

# convert entrez genes back to uniprot
entity = [x for y in tc_edges for x in y.split(' ') if 'geneid' in x]
gene_dict = gene2protein(entity)
print(len(gene_dict))

# only keep edges with valid entrez to protein mappings (n=885902)
good_gene_edges = set()
for x in range(0, len(tc_edges)):
    print(str(x) + '/' + str(len(tc_edges)))
    edge = tc_edges[x]
    triple = edge.strip(' ').split(' ')
    if len(triple) > 1:
        for entity in triple:
            if 'geneid' not in entity:
                good_gene_edges.add(edge)
            if 'geneid' in entity:
                if entity.strip(' ').strip('<').strip('>') in gene_dict.keys():
                    good_gene_edges.add(edge)

            break

# update list
tc_edge_final = list(good_gene_edges)
len(tc_edge_final)

# combine edges (n=1560576)
comb_edges = list(filter(None, [*it_edges, *tc_edge_final]))
len(comb_edges)

# combine tc and ig edges and create graph
graph = Graph()
AOP = Namespace('https://aopwiki.org/ext/')
obo = Namespace('http://purl.obolibrary.org/obo/')
oboInOwl = Namespace('http://www.geneontology.org/formats/oboInOwl#')
dc = Namespace('http://purl.org/dc/elements/1.1/')
hsapdv = Namespace('http://purl.obolibrary.org/obo/hsapdv#')
owl = Namespace

for edge in comb_edges:
    triple = edge.strip(' ').split(' ')
    updated_edge = {}
    print(edge)

    if triple[0] != triple[2]:
        for entity in triple:
            entity = re.sub('<|>', '', entity)

            if 'geneid' in entity:
                new = [URIRef(x) for x in gene_dict[entity.strip(' ').strip('<').strip('>')]]
                updated_edge[str(triple.index('<' + str(entity) + '>'))] = new
            elif 'aopwiki' in str(entity).lower():
                new = [URIRef(str(AOP) + str(entity.split('/')[-1]))]
                updated_edge[str(triple.index('<' + str(entity) + '>'))] = new
            else:
                new = [URIRef(str(entity))]
                updated_edge[str(triple.index('<' + str(entity) + '>'))] = new

        for trip in edge_loader(updated_edge):
            graph.add(trip)

# serialize graph -- write it out (n=2033803)
graph.serialize(destination='./resources/graphs/ignacio_edges/tc_ig_combined_edges.owl', format='nt')
