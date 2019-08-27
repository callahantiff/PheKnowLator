##########################################################################################
# EdgeDictionary.py
# Purpose: script to create edge lists for each downloaded source
# version 1.0.0
# date: 11.12.2017
# Python 3.6.2
##########################################################################################


# import needed libraries
import urllib

from rdflib import Graph
from tqdm import tqdm
from urllib.parse import urlencode
from urllib.request import urlopen


class EdgeList(object):
    """Class creates edge lists based off data type.

    The class takes as input a string that represents the type of data being stored and a list of lists, where each item
    in the list is a file path/name of a data source. With these inputs the class reads and processes the data and then
    assembles it into a dictionary where the keys are tuples storing an entity identifier and a relation that
    connects the entity to another entity, which is stored as a value. The class returns a dictionary, keyed by data
    source, that contains a dictionary of edge relations

    Args:
        data_files: a list that contains the full file path and name of each downloaded data source
        data_type: a string containing the type of data sources
        source_info: a dictionary where the keys represent edges between entities and the
        edges: a dictionary where the keys are sources and the values are entities for a certain edge type

    """

    def __init__(self, data_files, data_type, source_info, edges=None):
        if edges is None:
            edges = dict()

        self.data_files = data_files
        self.data_type = data_type
        self.source_info = {x.split(', ')[0]: x.split(', ')[1:] for x in
                            open(source_info).read().split('\n')}
        self.edges = edges

    def get_data_files(self):
        """Function returns the list of data sources with file path"""
        return self.data_files

    @staticmethod
    def api_access(entity):
        """Function takes a list of entities to search against the API. The Uniprot API
        (http://www.uniprot.org/help/api_idmapping) takes a list of uniprot proteins and returns a list of human Entrez
        gene ids. The CTD API takes a list of genes, diseases, or chemicals and returns a list of pathways, chemicals,
        or genes (depending on the input entity)

        Args:
            entity (str): entity to search against the API

        Returns:
            results (list): a list of pathways, chemicals, or genes (depending on the input entity)
        """
        proteins = list(set([x.split('\\t')[1] for x in open(entity).read().split('!')[-1].split('\\n')[1:]]))

        params = {
            'from': 'ACC+ID',
            'to': 'P_ENTREZGENEID',
            'format': 'tab',
            'query': ' '.join(proteins)
        }

        data = urllib.parse.urlencode(params).encode('utf-8')
        requests = urllib.request.Request('https://www.uniprot.org/uploadlists/', data)
        results = urllib.request.urlopen(requests).read().decode('utf-8')

        # CHECK - all URLs returned an data file
        if len(results) == 0:
            raise Exception('ERROR: API returned no data')
        else:
            return results

    @staticmethod
    def ont_access(ontology):
        """Takes a string representing a path/file name to an ontology. The function uses the RDFLib library
        and creates a graph. The graph is then queried to return all classes and their database cross-references. The
        function returns a list of query results

        Args:
            ontology (str): entity to search against the API

         Returns:
                query (list): a list of results returned from querying the graph
            """

        # read in ontology as graph
        graph = Graph()
        graph.parse(ontology)

        # query graph to get all cross-referenced sources
        results = graph.query(
            """SELECT DISTINCT ?source ?c
               WHERE {
                  ?c rdf:type owl:Class .
                  ?c oboInOwl:hasDbXref ?source .}
               """, initNs={"rdf": 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
                            "owl": 'http://www.w3.org/2002/07/owl#',
                            "oboInOwl": 'http://www.geneontology.org/formats/oboInOwl#'})

        # CHECK - all URLs returned an data file
        if len(list(results)) == 0:
            raise Exception('ERROR: Query returned no data')
        else:
            return results

    def get_edge_dics(self):
        """Function takes a list of file names and paths to each input data source. For each data source a dictionary
        of edges is created. This edge list is added to a dictionary that is keyed by data source

        Returns:
            edges (dict): a dictionary of edge lists by data source (key: file name/path, values: edge list)

        """

        for source in self.data_files:

            if 'class' in self.data_type:
                print('Processing Class Edge List')
                print('Creating edge dictionary for: ' + str(source.split('/')[-1]) + '\n')

                # get gene ontology edges
                if 'go' in source.lower() or 'gene_association' in source.lower():
                    self.edges[source] = self.go_edges(source)

                # get human phenotype ontology edges
                if 'disease' in source.lower() or 'phenotype' in source.lower():
                    self.edges[source] = self.hp_edges(source)

            if 'instance' in self.data_type:
                print('Processing Instance Edge List')
                print('Creating edge dictionary for: ' + str(source.split('/')[-1]) + '\n')

                # get gene ontology edges
                self.edges[source] = self.inst_edges(source)

        return self.edges

    def go_edges(self, source):
        """Function takes a string containing a file name and path and a dictionary containing important information
        knowledge graph (e.g., relation, node prefixes). The function outputs a dictionary of edges

        Args:
            source (str): a file name and path

        Returns:
            source_edges (dict): dict where keys are tuples (GO id, relation) and values are Entrez gene ids or
            reactome pathway ids

        """
        source_edges = {}

        # for creating entrez - go ontology edges
        if 'go' in source.lower():  # entrez - go
            results = self.api_access(source)  # get entrez gene_id mapping to uniprot ids
            id_map = map_results(results, source)

            for line in tqdm(open(source).read().split('\\n')[:-1]):
                if "\\t" in line:
                    if line.split('\\t')[1] in id_map.keys():
                        value = []
                        key = id_map[line.split('\\t')[1]]
                        val = str(line.split('\\t')[4]).replace(':', '_')
                        data_type = str(line.split('\\t')[8])

                        # add relation term + entity labels
                        if data_type == 'P':
                            value = [self.source_info['gene-gobp'][1] + x for x in key]
                            key = tuple([self.source_info['gene-gobp'][2] + val,
                                         self.source_info['gene-gobp'][0]])
                        if data_type == 'F':
                            value = [self.source_info['gene-gomf'][1] + x for x in key]
                            key = tuple([self.source_info['gene-gomf'][2] + val,
                                         self.source_info['gene-gomf'][0]])
                        if data_type == 'C':
                            value = [self.source_info['gene-gocc'][1] + x for x in key]
                            key = tuple([self.source_info['gene-gocc'][2] + val,
                                         self.source_info['gene-gocc'][0]])

                        if key in source_edges.keys():
                            source_edges[key] |= set(value)
                        else:
                            source_edges[key] = set(value)

        # for creating pathway - go ontology edges
        else:
            for line in tqdm(open(source).read().split('\\n')[:-1]):
                if "\\t" in line and line.split('\\t')[12] == 'taxon:9606':
                    value = []
                    val = line.split('\\t')[5].split(':')[1]
                    key = str(line.split('\\t')[4]).replace(':', '_')
                    data_type = str(line.split('\\t')[8])

                    # add relation term + entity labels
                    if data_type == 'P':
                        value = [self.source_info['react-gobp'][1] + val]
                        key = tuple([self.source_info['react-gobp'][2] + key,
                                     self.source_info['react-gobp'][0]])
                    if data_type == 'F':
                        value = [self.source_info['react-gomf'][1] + val]
                        key = tuple([self.source_info['react-gomf'][2] + key,
                                     self.source_info['react-gomf'][0]])
                    if data_type == 'C':
                        value = [self.source_info['react-gocc'][1] + val]
                        key = tuple([self.source_info['react-gocc'][2] + key,
                                     self.source_info['react-gocc'][0]])

                    if key in source_edges.keys():
                        source_edges[key] |= set(value)
                    else:
                        source_edges[key] = set(value)

        return source_edges

    def hp_edges(self, source):
        """Function takes a string containing a file name and path and a dictionary containing important information
        knowledge graph (e.g., relation, node prefixes). The function outputs a dictionary of edges

        Args:
            source (str): a file name and path

        Returns:
            source_edges (dict): dictionary where keys are tuples (HP id, relation) and values are Entrez gene ids or
            DOID ids

        """
        source_edges = {}

        # for creating disease - human phenotype ontology edges
        if 'disease' in source.lower():
            results = self.ont_access("./resources/ontologies/doid_with_imports.owl")  # HP - DOID mapping
            id_map = map_results(results, source)

            # create initial mapping from HP ids to disease ids (e.g., OMIM, Orphanet)
            hp_dis = {}
            for entity in tqdm(open(source).read().split('\\n')[:-1]):
                if '\\t' in entity:
                    dis = str(entity.split('\\t')[0])
                    key = str(entity.split('\\t')[3])

                    if dis in hp_dis.keys():
                        hp_dis[dis].add(key)
                    else:
                        hp_dis[dis] = {key}

            # using the query results from DOID DbXref to convert diseases to DOID
            for key, value in tqdm(hp_dis.items()):
                if 'ORPHA' in key:
                    key = key.replace("ORPHA:", "ORDO:")
                else:
                    key = key

                if key in id_map.keys():
                    match = value
                    dis = id_map[key][0]
                    data_type = self.source_info['doid-hp'][0]

                    for i in match:
                        key = tuple([self.source_info['doid-hp'][1] + str(i).replace(":", "_"), data_type])

                        if key in source_edges.keys():
                            source_edges[key].add(dis)
                        else:
                            source_edges[key] = {dis}

        else:
            # for creating entrez - human phenotype ontology edges
            for entity in tqdm(open(source).read().split('\\n')[:-1]):
                if '\\t' in entity:
                    gene = self.source_info['gene-hp'][1] + str(entity.split('\\t')[0])
                    key = str(entity.split('\\t')[3]).replace(':', '_')
                    data_type = self.source_info['gene-hp'][0]
                    key = tuple([self.source_info['gene-hp'][2] + str(key), data_type])

                    if key in source_edges.keys():
                        source_edges[key].add(gene)
                    else:
                        source_edges[key] = {gene}

        return source_edges

    def inst_edges(self, source):
        """Function takes a string containing a file name and path and a dictionary containing important information
        knowledge graph (e.g., relation, node prefixes). The function outputs a dictionary of edges

        Args:
            source (str): a file name and path

        Return:
            source_edges (dict): dictionary where keys are tuples (id, relation) and values are ids

        """
        source_edges = {}
        data_type = []

        # find source type information
        if 'chem_gene' in source:
            data_type = self.source_info['chemical-gene']
        if 'chem_pathway' in source:
            data_type = self.source_info['chemical-pathway']
        if 'chemicals_diseases' in source:
            data_type = self.source_info['chemical-disease']
        if 'genes_pathways' in source:
            data_type = self.source_info['gene-pathway']
        if 'diseases_pathways' in source:
            data_type = self.source_info['pathway-disease']

        # create dictionaries
        if '9606' in source:
            data_type = self.source_info['gene-gene']
            string_entrez = {}

            for entry in tqdm(open('./resources/text_files/human.txt').read().split('\\n')[:-1]):
                if '\\t' in entry:
                    value = data_type[1] + str(entry.split('\\t')[1])
                    key = str(entry.split('\\t')[-1])

                    if key in string_entrez.keys():
                        string_entrez[key].add(value)
                    else:
                        string_entrez[key] = {value}

            # create dictionary (key: gene, values: gene)
            for line in tqdm(open(source).read().split('\\n')[:-1]):
                if '9606' in line:
                    if line.split(' ')[2] > '699':
                        if line.split(' ')[0] in string_entrez.keys() and line.split(' ')[1] in string_entrez.keys():
                            key = tuple([list(string_entrez[line.split(' ')[0]])[0], data_type[0]])
                            gene = list(string_entrez[line.split(' ')[1]])[0]

                            if key in source_edges.keys():
                                source_edges[key].add(gene)
                            else:
                                source_edges[key] = {gene}

        elif 'diseases' in source and 'pathways' not in source:
            results = EdgeList.ont_access("./resources/ontologies/doid_with_imports.owl")  # HP - DOID mapping
            id_map = map_results(results, source)

            for line in tqdm(open(source).read().split('\\n')[:-1]):
                if "#" not in line:
                    if str(line.split('\\t')[4]) in id_map.keys():
                        key = tuple([data_type[1] + line.split('\\t')[1], data_type[0]])
                        value = id_map[str(line.split('\\t')[4])][0]

                        if key in source_edges.keys():
                            source_edges[key].add(value)
                        else:
                            source_edges[key] = {value}

        elif 'diseases_pathways' in source:
            # get doid dictionary
            results = EdgeList.ont_access("./resources/ontologies/doid_with_imports.owl")  # HP - DOID mapping
            id_map = map_results(results, source)

            for line in tqdm(open(source).read().split('\\n')[:-1]):
                if "#" not in line and 'REACT' in line:
                    if str(line.split('\\t')[1]) in id_map.keys():
                        key = tuple([data_type[1] + line.split('\\t')[3].split(':')[-1], data_type[0]])
                        value = id_map[str(line.split('\\t')[1])][0]

                        if key in source_edges.keys():
                            source_edges[key].add(value)
                        else:
                            source_edges[key] = {value}

        elif 'genes_pathways' in source:
            for line in tqdm(open(source).read().split('\\n')[:-1]):
                if "#" not in line and 'REACT' in line:
                    key = tuple([data_type[1] + line.split('\\t')[1], data_type[0]])
                    value = str(data_type[2]) + str(line.split('\\t')[3]).replace('REACT:', '')

                    if key in source_edges.keys():
                        source_edges[key].add(value)
                    else:
                        source_edges[key] = {value}

        elif 'chem_gene' in source:
            for line in tqdm(open(source).read().split('\\n')[:-1]):
                if "\\t" in line and '9606' in line:
                    key = tuple([data_type[1] + line.split('\\t')[1], data_type[0]])
                    value = str(data_type[2]) + str(line.split('\\t')[4]).replace(':', '_')

                    if key in source_edges.keys():
                        source_edges[key].add(value)
                    else:
                        source_edges[key] = {value}

        else:
            for line in tqdm(open(source).read().split('\\n')[:-1]):
                if "\\t" in line and 'REACT' in line:
                    key = tuple([data_type[1] + line.split('\\t')[1], data_type[0]])
                    value = str(data_type[2]) + str(line.split('\\t')[4]).replace('REACT:', '')

                    if key in source_edges.keys():
                        source_edges[key].add(value)
                    else:
                        source_edges[key] = {value}

        return source_edges


def map_results(results, source):
    """Function takes a list of lists representing the results returned from pinging an AP or querying an ontology
    and creates a dictionary where the item being mapped to is the value and the item being mapped is the value

    Args:
        results (str): a list of lists of the results returned from pinging an API or querying an ontology
        source (str): a file name and path

    Returns:
        id_mapping (dict): a dictionary where the keys and values are bio entities

    """
    id_mapping = {}

    # write results to a dictionary
    if 'disease' in source.lower():
        # for data returned from querying an ontology
        for entry in list(results):
            if entry[0] in id_mapping.keys():
                id_mapping[str(entry[0])].append(str(entry[1]))
            else:
                id_mapping[str(entry[0])] = [str(entry[1])]
    else:
        # for data returned from querying an API
        for entry in results.split('\n')[:-1]:
            if 'From' not in entry:
                if entry.split('\t')[0] in id_mapping.keys():
                    id_mapping[str(entry.split('\t')[0])].append(str(entry.split('\t')[1]))
                else:
                    id_mapping[str(entry.split('\t')[0])] = [str(entry.split('\t')[1])]

    return id_mapping
