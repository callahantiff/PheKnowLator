import glob
import json
import logging
import os
import os.path
import pandas
import pickle
import shutil
import unittest


from pkt_kg.knowledge_graph import PartialBuild


class TestPartialBuild(unittest.TestCase):
    """Class to test the partialBuild class from the knowledge graph script."""

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
        os.mkdir(self.dir_loc_resources + '/relations_data')
        os.mkdir(self.dir_loc_resources + '/node_data')
        os.mkdir(self.dir_loc_resources + '/ontologies')
        os.mkdir(self.dir_loc_resources + '/construction_approach')

        # handle logging
        self.logs = os.path.abspath(current_directory + '/builds/logs')
        logging.disable(logging.CRITICAL)
        if len(glob.glob(self.logs + '/*.log')) > 0: os.remove(glob.glob(self.logs + '/*.log')[0])

        # copy needed data data
        # node metadata
        shutil.copyfile(self.dir_loc + '/node_data/node_metadata_dict.pkl',
                        self.dir_loc_resources + '/node_data/node_metadata_dict.pkl')
        # ontology data
        shutil.copyfile(self.dir_loc + '/ontologies/empty_hp_with_imports.owl',
                        self.dir_loc_resources + '/ontologies/hp_with_imports.owl')
        # merged ontology data
        shutil.copyfile(self.dir_loc + '/ontologies/so_with_imports.owl',
                        self.dir_loc_resources + '/knowledge_graphs/PheKnowLator_MergedOntologies.owl')
        # relations data
        shutil.copyfile(self.dir_loc + '/RELATIONS_LABELS.txt',
                        self.dir_loc_resources + '/relations_data/RELATIONS_LABELS.txt')
        # inverse relations
        shutil.copyfile(self.dir_loc + '/INVERSE_RELATIONS.txt',
                        self.dir_loc_resources + '/relations_data/INVERSE_RELATIONS.txt')
        # empty master edges
        shutil.copyfile(self.dir_loc + '/Master_Edge_List_Dict_empty.json',
                        self.dir_loc_resources + '/Master_Edge_List_Dict_empty.json')

        # create edge list
        edge_dict = {"gene-phenotype": {"data_type": "entity-class",
                                        "edge_relation": "RO_0003302",
                                        "uri": ["http://www.ncbi.nlm.nih.gov/gene/",
                                                "http://purl.obolibrary.org/obo/"],
                                        "edge_list": [["2", "SO_0000162"], ["2", "SO_0000196"],
                                                      ["2", "SO_0000323"], ["9", "SO_0001490"],
                                                      ["9", "SO_0000301"], ["9", "SO_0001560"],
                                                      ["9", "SO_0001560"], ["10", "SO_0000444"],
                                                      ["10", "SO_0002138"], ["10", "SO_0000511"]]},
                     "gene-gene": {"data_type": "entity-entity",
                                   "edge_relation": "RO_0002435",
                                   "uri": ["http://www.ncbi.nlm.nih.gov/gene/",
                                           "http://www.ncbi.nlm.nih.gov/gene/"],
                                   "edge_list": [["1", "2"], ["2", "3"], ["3", "18"],
                                                 ["17", "19"], ["4", "17"], ["5", "11"],
                                                 ["11", "12"], ["4", "5"]]},
                     "disease-disease": {"data_type": "class-class",
                                         "edge_relation": "RO_0002435",
                                         "uri": ["http://www.ncbi.nlm.nih.gov/gene/",
                                                 "http://www.ncbi.nlm.nih.gov/gene/"],
                                         "edge_list": [["DOID_3075", "DOID_1080"], ["DOID_3075", "DOID_4267"],
                                                       ["DOID_4800", "DOID_10190"], ["DOID_4800", "DOID_80219"],
                                                       ["DOID_2729", "DOID_1962"], ["DOID_2729", "DOID_5096"],
                                                       ["DOID_8837", "DOID_6774"], ["DOID_8837", "DOID_8754"]]},
                     "entity_namespaces": {"gene": "http://purl.uniprot.org/geneid/"}
                     }

        # save data
        with open(self.dir_loc_resources + '/Master_Edge_List_Dict.json', 'w') as filepath:
            json.dump(edge_dict, filepath)

        # create subclass mapping data
        subcls_map = {"1": ['SO_0001217'], "2": ['SO_0001217'], "3": ['SO_0001217'], "4": ['SO_0001217'],
                      "5": ['SO_0001217'], "11": ['SO_0001217'], "12": ['SO_0001217'], "17": ['SO_0001217'],
                      "18": ['SO_0001217'], "5096": ['SO_0001217'], "6774": ['SO_0001217'], "19": ['SO_0001217']}

        # save data
        with open(self.dir_loc_resources + '/construction_approach/subclass_construction_map.pkl', 'wb') as f:
            pickle.dump(subcls_map, f, protocol=4)

        # set write location
        self.write_location = self.dir_loc_resources + '/knowledge_graphs'

        # instantiate class
        self.kg = PartialBuild('subclass', 'yes', 'yes', 'yes', self.write_location)

        # update class attributes
        dir_loc_owltools = os.path.join(current_directory, 'utils/owltools')
        self.kg.owl_tools = os.path.abspath(dir_loc_owltools)

        return None

    def test_class_initialization(self):
        """Tests initialization of the class."""

        # check build type
        self.assertEqual(self.kg.gets_build_type(), 'Partial Build')
        self.assertFalse(self.kg.gets_build_type() == 'Full Build')
        self.assertFalse(self.kg.gets_build_type() == 'Post-Closure Build')

        return None

    def test_construct_knowledge_graph(self):
        """Tests the construct_knowledge_graph method."""

        # test out the build
        self.kg.construct_knowledge_graph()

        # check for output files
        f_name = self.kg.full_kg.replace('_noOWL.owl', '_OWL.owl')
        self.assertTrue(os.path.exists(self.dir_loc_resources + '/knowledge_graphs/' + f_name))

        return None

    def tearDown(self):

        # remove resource directory
        shutil.rmtree(self.dir_loc_resources)

        return None
