import glob
import json
import os
import os.path
import pandas
import pickle
import shutil
import unittest

from rdflib import Graph
from typing import Dict, List

from pkt_kg.knowledge_graph import PostClosureBuild


class TestPostClosureBuild(unittest.TestCase):
    """Class to test the PostClosureBuild class from the knowledge graph script."""

    def setUp(self):
        # initialize file location
        current_directory = os.path.dirname(__file__)
        dir_loc = os.path.join(current_directory, 'data')
        self.dir_loc = os.path.abspath(dir_loc)

        # set-up environment - make temp directory
        dir_loc_resources = os.path.join(current_directory, 'resources')
        self.dir_loc_resources = os.path.abspath(dir_loc_resources)
        os.mkdir(self.dir_loc_resources)
        os.mkdir(self.dir_loc_resources + '/knowledge_graphs')
        os.mkdir(self.dir_loc_resources + '/knowledge_graphs/inverse_relations')
        os.mkdir(self.dir_loc_resources + '/relations_data')
        os.mkdir(self.dir_loc_resources + '/node_data')
        os.mkdir(self.dir_loc_resources + '/ontologies')
        os.mkdir(self.dir_loc_resources + '/construction_approach')
        os.mkdir(self.dir_loc_resources + '/owl_decoding')

        # copy needed data
        # node metadata
        shutil.copyfile(self.dir_loc + '/node_data/gene-phenotype_GENE_METADATA.txt',
                        self.dir_loc_resources + '/node_data/gene-phenotype_GENE_METADATA.txt')
        shutil.copyfile(self.dir_loc + '/node_data/gene-gene_GENE_METADATA.txt',
                        self.dir_loc_resources + '/node_data/gene-gene_GENE_METADATA.txt')

        # ontology data
        shutil.copyfile(self.dir_loc + '/ontologies/empty_hp_with_imports.owl',
                        self.dir_loc_resources + '/ontologies/hp_with_imports.owl')

        # merged ontology data
        shutil.copyfile(self.dir_loc + '/ontologies/so_with_imports.owl',
                        self.dir_loc_resources + '/knowledge_graphs/PheKnowLator_MergedOntologies.owl')

        # closed kg file
        shutil.copyfile(self.dir_loc + '/ontologies/so_with_imports.owl',
                        self.dir_loc_resources + '/knowledge_graphs/PheKnowLator_Closed_KG.owl')

        # relations data
        shutil.copyfile(self.dir_loc + '/RELATIONS_LABELS.txt',
                        self.dir_loc_resources + '/relations_data/RELATIONS_LABELS.txt')

        # inverse relations
        shutil.copyfile(self.dir_loc + '/INVERSE_RELATIONS.txt',
                        self.dir_loc_resources + '/relations_data/INVERSE_RELATIONS.txt')

        # owl nets properties file
        shutil.copyfile(self.dir_loc + '/OWL_NETS_Property_Types.txt',
                        self.dir_loc_resources + '/owl_decoding/OWL_NETS_Property_Types.txt')

        # empty master edges
        shutil.copyfile(self.dir_loc + '/Master_Edge_List_Dict_empty.json',
                        self.dir_loc_resources + '/Master_Edge_List_Dict_empty.json')

        # create edge list
        edge_dict = {"gene-phenotype": {"data_type": "entity-class",
                                        "edge_relation": "RO_0003302",
                                        "uri": ["https://www.ncbi.nlm.nih.gov/gene/",
                                                "http://purl.obolibrary.org/obo/"],
                                        "edge_list": [["2", "HP_0002511"], ["2", "HP_0000716"],
                                                      ["2", "HP_0000100"], ["9", "HP_0030955"],
                                                      ["9", "HP_0009725"], ["9", "HP_0100787"],
                                                      ["9", "HP_0012125"], ["10", "HP_0009725"],
                                                      ["10", "HP_0010301"], ["10", "HP_0045005"]]},
                     "gene-gene": {"data_type": "entity-entity",
                                   "edge_relation": "RO_0002435",
                                   "uri": ["https://www.ncbi.nlm.nih.gov/gene/",
                                           "https://www.ncbi.nlm.nih.gov/gene/"],
                                   "edge_list": [["3075", "1080"], ["3075", "4267"], ["4800", "10190"],
                                                 ["4800", "80219"], ["2729", "1962"], ["2729", "5096"],
                                                 ["8837", "6774"], ["8837", "8754"]]}
                     }

        # save data
        with open(self.dir_loc_resources + '/Master_Edge_List_Dict.json', 'w') as filepath:
            json.dump(edge_dict, filepath)

        # create subclass mapping data
        subcls_map = {"2": ['SO_0001217'], "9": ['SO_0001217'], "10": ['SO_0001217'], "1080": ['SO_0001217'],
                      "1962": ['SO_0001217'], "2729": ['SO_0001217'], "3075": ['SO_0001217'],
                      "4267": ['SO_0001217'],
                      "4800": ['SO_0001217'], "5096": ['SO_0001217'], "6774": ['SO_0001217'],
                      "8754": ['SO_0001217'],
                      "8837": ['SO_0001217'], "10190": ['SO_0001217'], "80219": ['SO_0001217']}

        # save data
        with open(self.dir_loc_resources + '/construction_approach/subclass_construction_map.pkl', 'wb') as f:
            pickle.dump(subcls_map, f, protocol=4)

        # build 3 different knowledge graphs
        self.kg = PostClosureBuild('subclass', 'yes', 'yes', 'yes')

        # update class attributes
        dir_loc_owltools = os.path.join(current_directory, 'utils/owltools')
        self.kg.owl_tools = os.path.abspath(dir_loc_owltools)

        return None

    def test_class_initialization(self):
        """Tests initialization of the class."""

        # check build type
        self.assertEqual(self.kg.gets_build_type(), 'Post-Closure Build')
        self.assertFalse(self.kg.gets_build_type() == 'Partial Build')
        self.assertFalse(self.kg.gets_build_type() == 'Full Build')

        return None

    def test_construct_knowledge_graph(self):
        """Tests the construct_knowledge_graph method."""

        # test out the build
        self.kg.construct_knowledge_graph()

        # check for output files
        # kg
        kg = 'PheKnowLator_v2.0.0_post-closure_subclass_inverseRelations_noOWL.owl'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/' + kg))

        # kg - owl
        kg_owl = 'PheKnowLator_v2.0.0_post-closure_subclass_inverseRelations_noOWL_OWLNETS.nt'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/' + kg_owl))

        # kg - nx.multiDiGraph
        kg_mdg = 'PheKnowLator_v2.0.0_post-closure_subclass_inverseRelations_noOWL_OWLNETS_NetworkxMultiDiGraph.gpickle'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/' + kg_mdg))

        # node metadata
        meta = 'PheKnowLator_v2.0.0_post-closure_subclass_inverseRelations_noOWL_NodeLabels.txt'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/' + meta))

        # edge list - identifiers
        ids = 'PheKnowLator_v2.0.0_post-closure_subclass_inverseRelations_noOWL_Triples_Identifiers.txt'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/' + ids))

        # edge list - integers
        ints = 'PheKnowLator_v2.0.0_post-closure_subclass_inverseRelations_noOWL_Triples_Integers.txt'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/' + ints))

        # edge list map
        int_map = 'PheKnowLator_v2.0.0_post-closure_subclass_inverseRelations_noOWL_Triples_Integer_Identifier_Map.json'
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/' + int_map))

        return None

    def tearDown(self):

        # remove resource directory
        shutil.rmtree(self.dir_loc_resources)

        return None
