#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import copy
import glob
import json
import networkx  # type: ignore
import os
import os.path
import pandas  # type: ignore
import pickle
import subprocess
import uuid

from abc import ABCMeta, abstractmethod
from rdflib import Graph, Namespace, URIRef  # type: ignore
from rdflib.namespace import RDF, RDFS, OWL  # type: ignore
from tqdm import tqdm  # type: ignore
from typing import Any, Callable, Dict, IO, List, Optional, Tuple

from pkt_kg.utils import *
from pkt_kg.metadata import Metadata
from pkt_kg.owlnets import OwlNets


class KGBuilder(object):
    """Class creates a semantic knowledge graph (KG). The class is designed to facilitate two KG construction
    approaches and three build types.

        Two Construction Approaches: (1) Instance-based: Adds edge data that is not from an ontology by connecting
        each non-ontology data node to an instances of existing ontology class; and (2) Subclass-based: Adds edge data
        that is not from an ontology by connecting each non-ontology data node to an existing ontology class.

        Three Build Types: (1) Full: Runs all build steps in the algorithm; (2) Partial: Runs all of the build steps
        through adding new edges. Designed for running a reasoner; and (3) Post-Closure: Runs the remaining build steps
        over a closed knowledge graph.

    Attributes:
        kg_version: A string that contains the version of the knowledge graph build.
        build: A string that indicates what kind of build (i.e. partial, post-closure, or full).
        write_location: A file path used for writing knowledge graph data.
        construction: A string that indicates what type of construction approach to use (i.e. instance or subclass).
        edge_data: A path to a file that references a dictionary of edge list tuples used to build the knowledge graph.
        node_data: A filepath to a directory called 'node_data' containing a file for each instance node.
        inverse_relations: A filepath to a directory called 'relations_data' containing the relations data.
        decode_owl_semantics: A string indicating whether edges containing owl semantics should be removed.
        subclass_data_dict: A node data ontology class dict. Used for the subclass construction approach. Keys are
            non-ontology ids and values are lists of ontology classes mapped to each non-ontology id. For example,
            {'R-HSA-168277' :['http://purl.obolibrary.org/obo/PW_0001054','http://purl.obolibrary.org/obo/GO_0046730']}
        edge_dict: A nested master edge list dict. The outer key is an edge-type (e.g.go-gene) and inner key is a dict
            storing data from the resource_info.txt input document. For example:
            {'chemical-complex': {'source_labels': ';;', 'data_type': 'class-instance', 'edge_relation': 'RO_0002436',
             'uri': ['http://purl.obolibrary.org/obo/', 'https://reactome.org/content/detail/'], 'delimiter': 't',
             'column_idx': '0;1', 'identifier_maps': 'None', 'evidence_criteria': 'None', 'filter_criteria': 'None',
             'edge_list': [['CHEBI_24505', 'R-HSA-1006173'], ...] }, }
        node_dict: A nested node metadata dict. The outer key is a node id and each inner key is a node type with the
            inner-key value being the corresponding metadata for that node type. For example:
                {'6469': {'Label': 'SHH', 'Description': 'Sonic Hedgehog Signaling Molecule is a protein-coding gene
                 located on chromosome 7 (map_location: 7q36.3).', 'Synonym': 'HHG1|HLP3|HPE3|ShhNC|TPT'}}
        relations_dict: A dict storing the relation identifiers and labels. For example,
            {'RO_0002616': 'related via evidence or inference to', 'RO_0002442': 'mutualistically interacts with}
        inverse_relations_dict: A dict storing relations ids and their inverse relation ids. For example:
            {'RO_0000056': 'RO_0000057', 'RO_0000079': 'RO_0000085'}
        full_kg: A string containing the filename for the full knowledge graph.
        merged_ont_kg: A string containing the filename of the knowledge graph that only contains the merged ontologies.
        ontologies: A list of file paths to an .owl file containing ontology data.
        kg_uuid_map: An ontology class-UUID instance dict. Keys are ontology classes and values are class instance UUIDs
            For example, :{'http://purl.../obo/CHEBI_24505':'https://github.com/.../obo/ext/414a-86cb-885f8c'}
        graph: An rdflib graph object which stores the knowledge graph.
        nx_mdg: A networkx MultiDiGraph object which is only created if the user requests owl semantics be removed.
        owltools: A string pointing to the location of the owl tools library.
        add_metata_data_to_kg: A flag that indicates whether or not to add metadata to the knowledge graph.

    Raises:
        ValueError: If the formatting of kg_version is incorrect (i.e. not "v.#.#.#").
        ValueError: If write_location, edge_data does not contain a valid filepath.
        OSError: If the edge_data file does not exist.
        TypeError: If the edge_data contains no data.
        TypeError: If the relations_data, node_data, and ontologies directories do not contain any data files.
        ValueError: If relations_data, node_data and decode_owl_semantics do not contain "yes" or "no".
        Exception: An exception is raised if self.adds_metadata_to_kg == "yes" and node_data == "no".
    """

    __metaclass__ = ABCMeta

    def __init__(self, kg_version: str, write_location: str, construction: str, edge_data: Optional[str] = None,
                 node_data: Optional[str] = None, inverse_relations: Optional[str] = None,
                 decode_owl_semantics: Optional[str] = None, adds_metadata_to_kg: str = 'no') -> None:

        self.res_dir = os.path.relpath('/'.join(write_location.split('/')[:-1]))
        self.build: str = self.gets_build_type().lower().split()[0]
        self.edge_dict: Optional[Dict] = None
        self.inverse_relations: Optional[List] = None
        self.relations_dict: Optional[Dict] = None
        self.inverse_relations_dict: Optional[Dict] = None
        self.node_data: Optional[List] = None
        self.node_dict: Optional[Dict] = None
        self.decode_owl_semantics: Optional[str] = None
        self.kg_uuid_map: Optional[Dict[str, str]] = dict() if construction.lower() == 'instance' else None
        self.graph: Graph = Graph()
        self.owltools = './pkt_kg/libs/owltools'
        self.adds_metadata_to_kg = adds_metadata_to_kg.lower()
        self.subclass_data_dict: Optional[Dict] = None

        # BUILD VARIABLES
        if kg_version is None: raise ValueError('kg_version must contain a valid version e.g. v.2.0.0')
        else: self.kg_version = kg_version

        # WRITE LOCATION
        if write_location is None: raise ValueError('write_location must contain a valid filepath')
        else: self.write_location = write_location

        # KNOWLEDGE GRAPH EDGE LIST
        if edge_data is None:
            raise ValueError('edge_data must not contain a valid filepath')
        elif not os.path.exists(edge_data):
            raise OSError('The {} file does not exist!'.format(edge_data))
        elif os.stat(edge_data).st_size == 0:
            raise TypeError('The input file: {} is empty'.format(edge_data))
        else:
            with open(edge_data, 'r') as edge_filepath:
                self.edge_dict = json.load(edge_filepath)

        # RELATIONS DATA
        if inverse_relations and inverse_relations.lower() not in ['yes', 'no']:
            raise ValueError('relations_data must be "no" or "yes"')
        else:
            if inverse_relations and inverse_relations.lower() == 'yes':
                if len(glob.glob(self.res_dir + '/relations_data/*.txt', recursive=True)) == 0:
                    raise TypeError('The relations_data directory is empty')
                else:
                    self.inverse_relations = glob.glob(self.res_dir + '/relations_data/*.txt', recursive=True)
                    self.relations_dict, self.inverse_relations_dict = dict(), dict()
                    kg_rel = '/inverse_relations' + '/PheKnowLator_' + self.build + '_InverseRelations_'
            else:
                self.inverse_relations, self.relations_dict, self.inverse_relations_dict = None, None, None
                kg_rel = '/relations_only' + '/PheKnowLator_' + self.build + '_'

        # NODE METADATA
        if node_data and node_data.lower() not in ['yes', 'no']: raise ValueError('node_data must be "no" or "yes"')
        else:
            if node_data and node_data.lower() == 'yes':
                if len(glob.glob(self.res_dir + '/node_data/*.txt', recursive=True)) == 0:
                    raise TypeError('The node_data directory is empty')
                else:
                    self.node_data = glob.glob(self.res_dir + '/node_data/*.txt', recursive=True)
                    self.node_dict = dict()
                    kg_node = kg_rel + 'Closed_' if self.build == 'post-closure' else kg_rel + 'NotClosed_'
            else:
                self.node_data, self.node_dict = None, None
                kg_node = kg_rel + 'NoMetadata_Closed_' if 'closure' in self.build else kg_rel + 'NoMetadata_NotClosed_'

                if self.adds_metadata_to_kg == 'yes':
                    raise Exception('ERROR! Can only add metatdata to knowledge graph is node_data == "yes"')

        # OWL SEMANTICS
        if decode_owl_semantics and decode_owl_semantics.lower() not in ['yes', 'no']:
            raise ValueError('decode_semantics must be "no" or "yes"')
        else:
            if decode_owl_semantics and decode_owl_semantics.lower() == 'yes' and self.build != 'partial':
                self.full_kg: str = kg_node + 'NoOWLSemantics_KG.owl'
                self.decode_owl_semantics = decode_owl_semantics
            else:
                self.full_kg = kg_node + 'OWLSemantics_KG.owl'
                self.decode_owl_semantics = None

        # ONTOLOGIES
        if len(glob.glob(self.res_dir + '/ontologies/*.owl')) == 0: raise TypeError('The ontologies directory is empty')
        else: self.ontologies: List[str] = glob.glob(self.res_dir + '/ontologies/*.owl')

        # CONSTRUCTION APPROACH
        if construction is None: raise ValueError('Construction must contain a valid filepath')
        else:
            self.construct_approach = construction.lower()

            if self.construct_approach == 'subclass':
                if len(glob.glob(self.res_dir + '/*/subclass/*.pkl')) == 0:
                    raise TypeError('The subclass directory is empty')
                else:
                    with open(glob.glob(self.res_dir + '/*/subclass/*.pkl')[0], 'rb') as filepath:  # type: IO[Any]
                        self.subclass_data_dict = pickle.load(filepath, encoding='bytes')
            else:
                self.subclass_data_dict = None

        # merged ontology data
        merged_onts = glob.glob(self.write_location + '/PheKnowLator_MergedOntologies*.owl')
        self.merged_ont_kg: str = merged_onts[0] if len(merged_onts) > 0 else '/PheKnowLator_MergedOntologies.owl'

    def sets_up_environment(self) -> None:
        """Sets-up the environment by checking for the existence and/or creating the following directories:
            - 'knowledge_graphs' directory in the `resources` directory
            - `resources/knowledge_graphs/relations_only`, if full or partial build without inverse relations
            - `resources/knowledge_graphs/inverse_relations`, if full or partial build with inverse relations

        Returns:
            None.
        """

        if self.build in ['full', 'partial']:
            if isinstance(self.inverse_relations, list):
                if not os.path.isdir(self.write_location + '/inverse_relations'):
                    os.mkdir(self.write_location + '/inverse_relations')
            else:
                if not os.path.isdir(self.write_location + '/relations_only'):
                    os.mkdir(self.write_location + '/relations_only')

        return None

    def reverse_relation_processor(self) -> None:
        """Creates and converts a Pandas DataFrame to a specific dictionary depending on whether it contains
        inverse relation data or relation data identifiers and labels. Examples of each dictionary are provided below:
            relations_dict: {'RO_0002551': 'has skeleton', 'RO_0002442': 'mutualistically interacts with}
            inverse_relations_dict: {'RO_0000056': 'RO_0000057', 'RO_0000079': 'RO_0000085'}

        Returns:
            None.
        """

        if self.inverse_relations:
            for data in self.inverse_relations:
                df = pandas.read_csv(data, header=0, delimiter='\t')
                df.drop_duplicates(keep='first', inplace=True)
                df.set_index(list(df)[0], inplace=True)

                if 'inverse' in data.lower(): self.inverse_relations_dict = df.to_dict('index')
                else: self.relations_dict = df.to_dict('index')

        return None

    @staticmethod
    def finds_node_type(edge_info) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Takes a dictionary of edge information and parses the data type for each node in the edge. The function
        returns either None or a string containing a particular node from the edge.

        Args:
            edge_info: A dict of information needed to add edge to graph, for example:
                {'n1': 'subclass', 'n2': 'class','relation': 'RO_0003302',
                'url': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
                'edges': ['2', 'DOID_0110035']}

        Returns:
            cls: None or an edge node of type ontology class.
            ent1: None or an edge node of type instance or subclass.
            ent2: None or an edge node of type instance or subclass.
        """

        # initialize node types node type (cls=ontology class, ent1/ent2=instance or subclass node)
        cls, ent1, ent2 = None, None, None

        # catch and don't process class-class edge types
        if edge_info['n1'] == 'class' and edge_info['n2'] == 'class':
            pass
        elif edge_info['n1'] == 'class' and edge_info['n2'] != 'class':
            cls, ent1, ent2 = edge_info['edges'][0], edge_info['edges'][1], None
        elif edge_info['n1'] != 'class' and edge_info['n2'] == 'class':
            ent1, cls, ent2 = edge_info['edges'][0], edge_info['edges'][1], None
        else:
            ent1, ent2, cls = edge_info['edges'][0], edge_info['edges'][1], None

        return cls, ent1, ent2

    def handles_edge_constructors(self, edge_info: Dict) -> Tuple[List, Dict]:
        """Adds edges for the subclass construction approach. The following 3 edges are added: (1) node1, relation,
        node2; (2) node1, rdfs:subClassOf, node1-ontology-class; and (3) node1, rdf:Type, owl:Class.

        Assumption: All ontology class use the obo namespace.

        Args:
            edge_info: A dict of information needed to add edge to graph, for example:
                {'n1': 'subclass', 'n2': 'class','relation': 'RO_0003302',
                'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
                'edges': ['2', 'DOID_0110035']}

        Returns:
            added_edge_counter: A list of new edges added to the knowledge graph.
            gene_info: A dict of information needed to add edge to graph with an updated node value for
                instance-class and class-instance edge types.
        """

        # define instance namespace
        pkt_kg = Namespace('https://github.com/callahantiff/PheKnowLator/obo/ext/')
        obo = Namespace('http://purl.obolibrary.org/obo/')

        # assign edge according to node type (cls=ontology class, ent1/ent2=instance or subclass node)
        cls, ent1, ent2 = self.finds_node_type(edge_info)
        added_edge_counter: List = []

        if self.construct_approach == 'subclass' and self.subclass_data_dict:
            for entity, uri in [[ent1, edge_info['uri'][0]], [ent2, edge_info['uri'][1]]]:
                if entity:
                    for class_id in self.subclass_data_dict[entity]:
                        n1, n2 = URIRef(uri + entity), URIRef(obo + class_id)  # type: ignore
                        self.graph.add((n1, RDFS.subClassOf, n2))
                        self.graph.add((n1, RDF.type, OWL.Class))
                        added_edge_counter += [(n1, RDFS.subClassOf, n2), (n1, RDF.type, OWL.Class)]
        else:
            if not ent2:
                if obo + cls in self.kg_uuid_map.keys():  # type: ignore
                    ont_class_iri = self.kg_uuid_map[obo + cls]  # type: ignore
                else:
                    ont_class_iri = pkt_kg + str(uuid.uuid4())
                    self.kg_uuid_map[obo + cls] = ont_class_iri  # type: ignore

                self.graph.add((URIRef(ont_class_iri), RDF.type, URIRef(obo + cls)))
                added_edge_counter = [(URIRef(ont_class_iri), RDF.type, URIRef(obo + cls))]

                # update edge_info dictionary to replace class identifier with UUID map
                edge_info['edges'][0] = ont_class_iri if edge_info['n1'] == 'class' else edge_info['edges'][0]
                edge_info['edges'][1] = ont_class_iri if edge_info['n2'] == 'class' else edge_info['edges'][1]
            else:
                return added_edge_counter, edge_info

        return added_edge_counter, edge_info

    def checks_for_inverse_relations(self, relation: str, edge_list: List[List[str]]) -> Optional[str]:
        """Checks a relation to determine whether or not edges for an inverse relation should be created and added to
        the knowledge graph.

        Args:
            relation: A string that contains the relation assigned to edge in resource_info.txt (e.g. 'RO_0000056').
            edge_list: A list of knowledge graph edges. For example: [["8837", "4283"], ["8837", "839"]]

        Returns:
            A string containing an ontology identifier (e.g. "RO_0000056). The value depends on a set of conditions:
                - inverse relation, if the stored relation has an inverse relation in inverse_relations
                - current relation, if the stored relation string includes 'interact' and there is an equal count of
                  each node type (i.e., this is checking for symmetry in interaction-based edge types)
                - None, assuming the prior listed conditions are not met
        """

        if (self.inverse_relations and self.inverse_relations_dict) and relation in self.inverse_relations_dict.keys():
            return self.inverse_relations_dict[relation]['Inverse_Relation']
        elif (self.inverse_relations and self.relations_dict) and relation in self.relations_dict.keys():
            int_type = self.relations_dict[relation]['Label']

            if 'interact' in int_type and len(set([x[0] for x in edge_list])) != len(set([x[1] for x in edge_list])):
                return relation
            else:
                return None
        else:
            return None

    def adds_edge_relations(self, edge_info: Dict, inverse_relation: Optional[str]) -> List:
        """Function checks whether or not to create inverse relations. If not, the function adds an edge without its
        inverse.

        Args:
            edge_info: A dict of information needed to add edge to graph, for example:
                {'n1': 'subclass', 'n2': 'class','relation': 'RO_0003302',
                'uri': ['https://www.ncbi.nlm.nih.gov/gene/', 'http://purl.obolibrary.org/obo/'],
                'edges': ['2', 'DOID_0110035']}
            inverse_relation: A flag used to indicate whether or not to add inverse relations (None or string
                containing a relation).

        Returns:
            added_edge_counter: A list of new edges added to the knowledge graph.
        """

        # define instance namespace
        obo = Namespace('http://purl.obolibrary.org/obo/')

        # assign edge according to node type (cls=ontology class, ent1/ent2=instance or subclass node)
        added_edge_counter: List = []

        # add primary edge
        self.graph.add((URIRef(edge_info['uri'][0] + edge_info['edges'][0]),
                        URIRef(obo + edge_info['relation']),
                        URIRef(edge_info['uri'][1] + edge_info['edges'][1])))

        added_edge_counter += [(URIRef(edge_info['uri'][0] + edge_info['edges'][0]),
                                URIRef(obo + edge_info['relation']),
                                URIRef(edge_info['uri'][1] + edge_info['edges'][1]))]
        if inverse_relation:
            self.graph.add((URIRef(edge_info['uri'][1] + edge_info['edges'][1]),
                            URIRef(obo + inverse_relation),
                            URIRef(edge_info['uri'][0] + edge_info['edges'][0])))

            added_edge_counter += [(URIRef(edge_info['uri'][1] + edge_info['edges'][1]),
                                    URIRef(obo + inverse_relation),
                                    URIRef(edge_info['uri'][0] + edge_info['edges'][0]))]

        return added_edge_counter

    def creates_knowledge_graph_edges(self, node_metadata_func: Callable, ontology_annotator_func: Callable) -> None:
        """Takes a nested dictionary of edge lists and adds them to an existing knowledge graph by their edge_type (
        e.g. chemical-gene). Once the knowledge graph is complete, it is written out as an `.owl` file to the
        write_location  directory. Additionally, for instance-based constructions, the kg_uuid_map dictionary that
        stores the mapping between each class and its instance is written as a '.json' file to the same location.

        Args:
            node_metadata_func: A function that adds metadata for non-ontology classes to a knowledge graph.
            ontology_annotator_func: A function that adds annotations to an existing ontology.

        Returns:
            None.
        """

        if self.edge_dict:
            for edge_type in self.edge_dict.keys():
                node1_type, node2_type = self.edge_dict[edge_type]['data_type'].split('-')
                relation = self.edge_dict[edge_type]['edge_relation']
                uris = self.edge_dict[edge_type]['uri']
                edge_list = copy.deepcopy(self.edge_dict[edge_type]['edge_list'])
                edge_results: List = []

                # check for inverse relations
                if self.inverse_relations: inverse_relation = self.checks_for_inverse_relations(relation, edge_list)
                else: inverse_relation = None

                print('\nCreating {} ({}-{}) Edges ***'.format(edge_type.upper(), node1_type, node2_type))

                for edge in tqdm(edge_list):
                    edge_info = {'n1': node1_type, 'n2': node2_type, 'relation': relation, 'uri': uris, 'edges': edge}
                    results = self.handles_edge_constructors(edge_info)
                    edge_results += results[0]
                    edge_results += self.adds_edge_relations(results[1], inverse_relation)

                # print edge-type statistics
                n1, n2 = edge_type.split('-')
                print('\nUnique Edges: {}'.format(len([list(x) for x in set([tuple(y) for y in edge_results])])))
                print('Unique {}: {}'.format(n1, len(set([x[0] for x in self.edge_dict[edge_type]['edge_list']]))))
                print('Unique {}: {}'.format(n2, len(set([x[1] for x in self.edge_dict[edge_type]['edge_list']]))))

            # add ontology metadata and annotations, serialize graph, and apply OWL API formatting to output
            if self.adds_metadata_to_kg == 'yes': node_metadata_func(self.graph, self.edge_dict)
            self.graph = ontology_annotator_func(self.full_kg.split('/')[-1], self.graph)
            self.graph.serialize(destination=self.write_location + self.full_kg, format='xml')
            ontology_file_formatter(self.write_location, self.full_kg, self.owltools)
            gets_ontology_statistics(self.write_location + self.full_kg, self.owltools)

            # write class-instance uuid mapping dictionary to file (if instance construct approach)
            if self.construct_approach == 'instance':
                file_path = '/'.join(self.write_location.split('/')[:-1]) + '/construction_approach/instance/'
                with open(file_path + self.full_kg.split('/')[-1][:-7] + '_ClassInstanceMap.json', 'w') as file_name:
                    json.dump(self.kg_uuid_map, file_name)

        return None

    def construct_knowledge_graph(self) -> None:
        """Builds a knowledge graph. The knowledge graph build is completed differently depending on the build type
        that the user requested. The build types include: "full", "partial", or "post-closure". The knowledge graph
        is built through the following steps: (1) Set up environment; (2) Process relation/inverse relations; (3)
        Process node metadata; (4) Merge ontologies; (5) Add master edge list to merged ontologies; (6) Remove
        annotation assertions (if partial build); (7) Add annotation assertions (if post-closure build); (8)
        Extract and write node metadata; (9) Decode OWL-encoded classes; and (10) Output knowledge graph files and
        create edge lists.

        Returns:
            None.
        """

        pass

    @abstractmethod
    def gets_build_type(self) -> str:
        """"A string representing the type of knowledge graph build."""

        pass


class PartialBuild(KGBuilder):

    def gets_build_type(self) -> str:
        """"A string representing the type of knowledge graph build."""

        return 'Partial Build'

    def construct_knowledge_graph(self) -> None:
        """Builds a partial knowledge graph. A partial knowledge graph build is recommended when one intends to build a
        knowledge graph and intends to run a reasoner over it. The partial build includes the following steps: (1)
        Set up environment; (2) Process relation/inverse relation data; (3) Process node metadata; (4) Merge
        ontologies; (5) Add master edge list to merged ontologies; and (6) Remove annotation assertions.

        Returns:
            None.

        Raises:
            TypeError: If the ontologies directory is empty.
        """

        print('\n### Starting Knowledge Graph Build: PARTIAL ###')

        # STEP 1: SET-UP ENVIRONMENT
        print('*** Set-Up Environment ***')
        self.sets_up_environment()

        # STEP 2: PROCESS RELATION AND INVERSE RELATION DATA
        print('*** Loading Relations Data ***')
        self.reverse_relation_processor()

        # STEP 3: PROCESS NODE METADATA
        print('*** Loading Node Metadata Data ***')
        metadata = Metadata(self.kg_version, self.write_location, self.full_kg, self.node_data, self.node_dict)
        if self.node_data: metadata.node_metadata_processor()

        # STEP 4: MERGE ONTOLOGIES
        if self.merged_ont_kg in glob.glob(self.write_location + '/*.owl'):
            print('*** Loading Merged Ontologies ***')
            self.graph.parse(self.merged_ont_kg)
            gets_ontology_statistics(self.merged_ont_kg)
        else:
            if len(self.ontologies) == 0:
                raise TypeError('ERROR: the ontologies directory: {} is empty'.format(
                    self.write_location + '/' + glob.glob('*/ontologies')[0]))
            else:
                print('*** Merging Ontology Data ***')
                merges_ontologies(self.ontologies, self.write_location, '/' + self.merged_ont_kg.split('/')[-1])

        # STEP 5: ADD MASTER EDGE DATA TO KNOWLEDGE GRAPH
        # create temporary directory to store partial builds and update path to write data to
        temp_dir = '/'.join((self.write_location + self.full_kg).split('/')[:4])
        if temp_dir + '/partial' not in glob.glob(self.write_location + '/**/**'): os.mkdir(temp_dir + '/partial_build')
        self.full_kg = '/'.join(self.full_kg.split('/')[:2] + ['partial_build'] + self.full_kg.split('/')[2:])
        metadata.full_kg = self.full_kg

        # build knowledge graph
        print('*** Building Knowledge Graph Edges ***')
        self.creates_knowledge_graph_edges(metadata.adds_node_metadata, metadata.adds_ontology_annotations)
        del self.graph, self.edge_dict, self.node_dict, self.relations_dict, self.inverse_relations_dict

        # STEP 6: REMOVE ANNOTATION ASSERTIONS
        print('*** Removing Annotation Assertions ***')
        metadata.removes_annotation_assertions()
        del metadata

        return None


class PostClosureBuild(KGBuilder):

    def gets_build_type(self) -> str:
        """"A string representing the type of knowledge graph being built."""

        return 'Post-Closure Build'

    def construct_knowledge_graph(self) -> None:
        """Builds a post-closure knowledge graph. This build is recommended when one has previously performed a
        "partial" knowledge graph build and then ran a reasoner over it. This build type inputs the closed partially
        built knowledge graph and completes the build process.

        The post-closure build utilizes the following steps: (1) Set up environment; (2) Process relation and inverse
        relation data; (3) Process node metadata; (4) Load closed knowledge graph, annotation assertions, and
        class-instance UUID map; (5) Add annotation assertions; (6) Extract and write node metadata; (7) Decode
        OWL-encoded classes; and (8) Output knowledge graph files and create edge lists.

        Returns:
            None.

        Raises:
            TypeError: If the class-instance UUID Map file type is not json.
            TypeError: If the knowledge graph file type is not owl.
            TypeError: If the annotation assertion file type is not owl.
            IOError: If the uuid_location, closed_kg_location, and annotation_assertions files do not exist.
            TypeError: If uuid_location, closed_kg_location, and annotation_assertions files are empty.
        """

        print('\n### Starting Knowledge Graph Build: post-closure ###')

        # STEP 1: SET-UP ENVIRONMENT
        print('*** Set-Up Environment ***')
        self.sets_up_environment()

        # STEP 2: PROCESS RELATION AND INVERSE RELATION DATA
        print('*** Loading Relations Data ***')
        self.reverse_relation_processor()

        # STEP 3: PROCESS NODE METADATA
        print('*** Loading Node Metadata Data ***')
        metadata = Metadata(self.kg_version, self.write_location, self.full_kg, self.node_data, self.node_dict)
        if self.node_data: metadata.node_metadata_processor()

        # STEP 4: LOAD CLOSED KNOWLEDGE GRAPH DATA
        if self.construct_approach == 'instance':
            uuid_location = input('Filepath to the class-instance UUID Map: ')

            # check uuid input file
            if '.json' not in uuid_location:
                raise TypeError('The provided file is not type .json')
            elif not os.path.exists(uuid_location):
                raise IOError('The {} file does not exist!'.format(uuid_location))
            elif os.stat(uuid_location).st_size == 0:
                raise TypeError('input file: {} is empty'.format(uuid_location))
            else:
                # load closed knowledge graph and move file out of 'partial build' directory
                print('*** Loading Knowledge Graph Class-Instance UUID Map ***')
                self.kg_uuid_map = json.load(open(uuid_location, 'r'))
                os.rename(uuid_location, uuid_location.replace('partial_', '').replace('build/', '').replace('Not', ''))

        closed_kg_location = input('Filepath to the closed knowledge graph: ')

        # check input owl file
        if '.owl' not in closed_kg_location:
            raise TypeError('The provided file is not type .owl')
        elif not os.path.exists(closed_kg_location):
            raise IOError('The {} file does not exist!'.format(closed_kg_location))
        elif os.stat(closed_kg_location).st_size == 0:
            raise TypeError('input file: {} is empty'.format(closed_kg_location))
        else:
            print('*** Loading Closed Knowledge Graph ***')
            self.graph = Graph()
            self.graph.parse(closed_kg_location)
            gets_ontology_statistics(closed_kg_location)

        # STEP 5: ADD ANNOTATION ASSERTIONS
        annotation_assertions = input('Filepath to the knowledge graph with annotation assertions: ')

        if '.txt' not in annotation_assertions:
            raise TypeError('The provided file is not type .owl')
        elif not os.path.exists(annotation_assertions):
            raise IOError('The {} file does not exist!'.format(annotation_assertions))
        elif os.stat(annotation_assertions).st_size == 0:
            raise TypeError('input file: {} is empty'.format(annotation_assertions))
        else:
            print('*** Loading Annotation Assertions Edge List ***')
            self.graph = metadata.adds_annotation_assertions(self.graph, annotation_assertions)

        # STEP 6: EXTRACT AND WRITE NODE METADATA
        print('\n*** Processing Knowledge Graph Metadata ***')
        if self.node_data is not None: metadata.output_knowledge_graph_metadata(self.graph)
        del metadata, self.edge_dict, self.node_dict, self.relations_dict, self.inverse_relations_dict

        # STEP 7: DECODE OWL SEMANTICS
        if self.decode_owl_semantics:
            print('*** Running OWL-NETS - Decoding OWL-Encoded Classes and Removing OWL Semantics ***')
            owl_nets = OwlNets(self.graph, self.kg_uuid_map, self.write_location, self.full_kg)
            self.graph = owl_nets.run_owl_nets()
            del owl_nets, self.kg_uuid_map

        # STEP 8: WRITE OUT KNOWLEDGE GRAPH DATA AND CREATE EDGE LISTS
        print('*** Writing Knowledge Graph Edge Lists ***')
        maps_node_ids_to_integers(self.graph, self.write_location,
                                  self.full_kg[:-6] + 'Triples_Integers.txt',
                                  self.full_kg[:-6] + 'Triples_Integer_Identifier_Map.json')

        converts_rdflib_to_networkx(self.write_location, self.full_kg, self.graph)

        # clean up by asking user if partial directory can be removed
        remove_partial_dir = input('\nDelete the "partial" directory?: (please type "yes" or "no")')

        if 'yes' in remove_partial_dir.lower():
            os.remove(glob.glob(self.write_location + '/'.join(self.full_kg.split('/')[0:2]) + '/partial_build')[0])

        return None


class FullBuild(KGBuilder):

    def gets_build_type(self) -> str:
        """"A string representing the type of knowledge graph being built."""

        return 'Full Build'

    def construct_knowledge_graph(self) -> None:
        """Builds a full knowledge graph. Please note that the process to build this version of the knowledge graph
        does not include running a reasoner. The full build includes the following steps: (1) Set up environment; (2)
        Process relation/inverse relations; (3) Process node metadata; (4) Merge ontologies; and (5) Add master edge
        list to merged ontologies; (6) Extract and write node metadata; (7) Decode OWL-encoded classes; and (8)
        Output knowledge graphs and create edge lists.

        Returns:
            None.

        Raises:
            TypeError: If the ontology directory is empty.
        """

        print('\n### Starting Knowledge Graph Build: FULL ###')

        # STEP 1: SET-UP ENVIRONMENT
        print('*** Set-Up Environment ***')
        self.sets_up_environment()

        # STEP 2: PROCESS RELATION AND INVERSE RELATION DATA
        print('*** Loading Relations Data ***')
        self.reverse_relation_processor()

        # STEP 3: PROCESS NODE METADATA
        print('*** Loading Node Metadata Data ***')
        metadata = Metadata(self.kg_version, self.write_location, self.full_kg, self.node_data, self.node_dict)
        if self.node_data: metadata.node_metadata_processor()

        # STEP 4: MERGE ONTOLOGIES
        if self.merged_ont_kg in glob.glob(self.write_location + '/*.owl'):
            print('*** Loading Merged Ontologies ***')
            self.graph.parse(self.merged_ont_kg)
            gets_ontology_statistics(self.merged_ont_kg)
        else:
            if len(self.ontologies) == 0:
                raise TypeError('The ontologies directory is empty')
            else:
                print('*** Merging Ontology Data ***')

                merges_ontologies(self.ontologies, self.write_location, '/' + self.merged_ont_kg.split('/')[-1])

        # STEP 5: ADD EDGE DATA TO KNOWLEDGE GRAPH DATA
        print('\n*** Building Knowledge Graph Edges ***')
        self.creates_knowledge_graph_edges(metadata.adds_node_metadata, metadata.adds_ontology_annotations)

        # STEP 6: EXTRACT AND WRITE NODE METADATA
        print('\n*** Processing Knowledge Graph Metadata ***')
        if self.node_data is not None: metadata.output_knowledge_graph_metadata(self.graph)
        del metadata, self.edge_dict, self.node_dict, self.relations_dict, self.inverse_relations_dict

        # STEP 7: DECODE OWL SEMANTICS
        if self.decode_owl_semantics:
            print('\n*** Running OWL-NETS - Decoding OWL-Encoded Classes and Removing OWL Semantics ***')
            owl_nets = OwlNets(self.graph, self.kg_uuid_map, self.write_location, self.full_kg)
            self.graph = owl_nets.run_owl_nets()
            del owl_nets, self.kg_uuid_map

        # STEP 8: WRITE OUT KNOWLEDGE GRAPH DATA AND CREATE EDGE LISTS
        print('\n*** Writing Knowledge Graph Edge Lists ***')
        maps_node_ids_to_integers(self.graph, self.write_location,
                                  self.full_kg[:-6] + 'Triples_Integers.txt',
                                  self.full_kg[:-6] + 'Triples_Integer_Identifier_Map.json')

        converts_rdflib_to_networkx(self.write_location, self.full_kg, self.graph)

        return None
