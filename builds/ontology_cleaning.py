#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import datetime
import fnmatch
import glob
import logging.config
import pickle
import os
import re
import subprocess

from google.cloud import storage  # type: ignore
from owlready2 import get_ontology, OwlReadyOntologyParsingError  # type: ignore
from rdflib import BNode, Graph, Literal, Namespace, URIRef  # type: ignore
from rdflib.namespace import OWL, RDF, RDFS  # type: ignore
from tqdm import tqdm  # type: ignore
from typing import Dict, List, Optional, Union

# import script containing helper functions
from builds.build_utilities import downloads_data_from_gcs_bucket, uploads_data_to_gcs_bucket
from pkt_kg.utils import *

# set environment variables
# global namespace
schema = Namespace('http://www.w3.org/2001/XMLSchema#')
obo = Namespace('http://purl.obolibrary.org/obo/')
oboinowl = Namespace('http://www.geneontology.org/formats/oboInOwl#')
# logging
log_dir, log, log_config = 'builds/logs', 'pkt_builder_phases12_log.log', glob.glob('**/logging.ini', recursive=True)
if not os.path.exists(log_dir): os.mkdir(log_dir)
logger = logging.getLogger(__name__)
logging.config.fileConfig(log_config[0], disable_existing_loggers=False, defaults={'log_file': log_dir + '/' + log})


class OntologyCleaner(object):
    """Class provides a container for the ontology cleaning methods, original housed in the Ontology_Cleaning.ipynb
    Jupyter Notebook. See notebook (https://github.com/callahantiff/PheKnowLator/blob/master/Ontology_Cleaning.ipynb)
    for more detailed descriptions of each error check performed and how they are resolved.

    Attributes:
        gcs_bucket: A storage Bucket object specifying a Google Cloud Storage bucket.
        org_data: A string specifying the location of the original_data directory for a specific build.
        proc_data: A string specifying the location of the original_data directory for a specific build.
        temp: A string specifying a temporary directory to use while processing data locally.
    """

    def __init__(self, gcs_bucket: Union[storage.bucket.Bucket, str], org_data: str, proc_data: str, temp: str) -> None:

        # GOOGLE CLOUD STORAGE VARIABLES
        self.bucket: Union[storage.bucket.Bucket, str] = gcs_bucket
        self.original_data: str = org_data
        self.processed_data: str = proc_data
        self.current_build: str = 'current_build/'
        # LOCAL VARIABLES
        self.owltools_location = './builds/owltools'
        # self.owltools_location = './pkt_kg/libs/owltools'
        self.temp_dir = temp
        self.merged_ontology_filename: str = 'PheKnowLator_MergedOntologies.owl'
        # ONTOLOGY INFORMATION DICTIONARY
        if isinstance(self.bucket, storage.bucket.Bucket):
            self.onts = [x.name.split('/')[-1] for x in self.bucket.list_blobs(prefix=self.original_data)
                         if x.name.endswith('.owl')] + [self.merged_ontology_filename]
        else: self.onts = glob.glob(self.temp_dir + '*/*.owl')
        self.ontology_info: Dict = {x: {} for x in self.onts}
        # OTHER CLASS ATTRIBUTES
        self.ont_file_location: str = ''
        self.ont_graph: Graph = Graph()
        self.ontology_classes: List = []
        # GENE IDENTIFIER DATA
        self.withdrawn_map = {  # keep list of withdrawn hgnc gene ids in HPO that need updating
            'http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=21508': ['653067', '653220'],
            'http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=23418': ['653450'],
            'http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=18372': ['653067', '653220'],
            'http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=26418': ['132332'],
            'http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=30679': ['653067', '653220'],
            'http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=26844': ['5414'],
            'http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=31447': ['10529'],
            'http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=33870': ['114112'],
            'http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=8103': ['10896'],
            'http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=26619': ['5058'],
            'http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=31424': ['101362076'],
            'http://identifiers.org/hgnc/12764': ['7467'],
            'http://identifiers.org/hgnc/1881': ['10167']}
        f_data = self.temp_dir + '/Merged_gene_rna_protein_identifiers.pkl'
        if not os.path.exists(f_data):
            url = 'https://storage.googleapis.com/pheknowlator/{}data/processed_data/'.format(self.current_build)
            data_downloader(url + f_data.split('/')[-1], self.temp_dir + '/')
        with open(f_data, 'rb') as out:
            self.gene_ids = pickle.load(out, encoding='bytes')

    def reads_gcs_bucket_data_to_graph(self, f_name: str) -> Graph:
        """Reads data corresponding to the input file_location variable into a Pandas DataFrame.

        Args:
            f_name: A string containing the name of file that exists in a Google Cloud Storage bucket.

        Returns:
             graph: An RDFLib graph object containing data read from a Google Cloud Storage bucket.
        """

        x = downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, f_name, self.temp_dir)
        graph = Graph().parse(x, format='xml')

        return graph

    def checks_for_downloaded_ontology_data(self) -> List:
        """Checks to see if the processed ontology files are in the current temp directory. If not, they are downloaded
        from the processed_data bucket for the current build. The local path to each downloaded ontology file is added
        to a list which is returned. This method is meant to be a helper method to assist with the automatic build
        process should something interrupt the build between the individual and merged ontology processing steps.

        Returns:
            ont_list: A list of strings where each string is
        """

        onts = [self.temp_dir + '/' + x for x in list(self.ontology_info.keys()) if x != self.merged_ontology_filename]
        if len(onts) == 0 and isinstance(self.bucket, storage.bucket.Bucket):
            # look in bucket for ontology files
            _files = [_.name for _ in self.bucket.list_blobs(prefix=self.processed_data)]
            ont_list = [x.split('/')[-1] for x in fnmatch.filter(_files, '*/*_with_imports.owl')]
            # download the files to local temp directory
            for ont in ont_list:
                downloads_data_from_gcs_bucket(self.bucket, self.original_data, self.processed_data, ont, self.temp_dir)
        else: ont_list = onts

        return ont_list

    def merge_ontologies(self, ontology_files: List[str], write_location: str, merged_ont_kg: str) -> Graph:
        """Using the OWLTools API, each ontology listed in in the ontologies attribute is recursively merged with into a
        master merged ontology file and saved locally to the provided file path via the merged_ontology attribute. The
        function assumes that the file is written to the directory specified by the write_location attribute.

        Args:
            ontology_files: A list of ontology file paths.
            write_location: A string pointing to a local directory for writing data.
            merged_ont_kg: A string pointing to the location of the merged ontology file.

        Returns:
            None.
        """

        if not ontology_files: return None
        else:
            if write_location + merged_ont_kg in glob.glob(write_location + '/*.owl'):
                ont1, ont2 = ontology_files.pop(), write_location + merged_ont_kg
            else: ont1, ont2 = ontology_files.pop(), ontology_files.pop()
            print('Merging Ontologies: {ont1}, {ont2}'.format(ont1=ont1.split('/')[-1], ont2=ont2.split('/')[-1]))
            logger.info('Merging Ontologies: {ont1}, {ont2}'.format(ont1=ont1.split('/')[-1], ont2=ont2.split('/')[-1]))
            command = '{} {} {} --merge-support-ontologies -o {}'
            return_code = os.system(command.format(self.owltools_location, str(ont1), str(ont2),
                                                   write_location + merged_ont_kg))
            if return_code == 0: return self.merge_ontologies(ontology_files, write_location, merged_ont_kg)
            else:
                logger.error('ERROR: OWL API Merging Failed: {}'.format(return_code))
                raise Exception('ERROR: OWL API Merging Failed: {}'.format(return_code))

    def _logically_verifies_cleaned_ontologies(self) -> None:
        """Logically verifies an ontology by running the ELK deductive logic reasoner. Before running the reasoner
        the instantiated RDFLib object is saved locally.

        Returns:
            None.
        """

        print('Logically Verifying Ontology')
        logger.info('PKT: Logically Verifying Ontology')

        # save graph in order to run reasoner
        filename = self.temp_dir + '/' + self.ont_file_location
        self.ont_graph.serialize(destination=filename, format='xml')
        command = "{} {} --reasoner {} --run-reasoner --assert-implied -o {}"
        return_code = os.system(command.format(self.owltools_location, filename, 'elk', filename))
        if return_code == 0:
            ontology_file_formatter(self.temp_dir, '/' + self.ont_file_location, self.owltools_location)
            uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, self.ont_file_location)
        else:
            logger.error('ERROR: Reasoner Finished with Errors - {}: {}'.format(filename, return_code))
            raise Exception('ERROR: Reasoner Finished with Errors - {}: {}'.format(filename, return_code))

        return None

    def updates_ontology_reporter(self) -> None:
        """Initializes the ontology_info dictionary which tracks ontology changes throughout the cleaning process.

        Returns:
            None.
        """

        print('Obtaining Ontology Statistics')
        logger.info('Obtaining Ontology Statistics')

        if isinstance(self.bucket, str):
            gcs_org_url = '{}/{}'.format(self.temp_dir, self.original_data)
            gcs_proc_url = gcs_org_url
        else:
            gcs_org_url = 'https://storage.googleapis.com/pheknowlator/{}'.format(self.original_data)
            gcs_proc_url = 'https://storage.googleapis.com/pheknowlator/{}'.format(self.processed_data)

        # write out statistics
        if 'Starting Statistics' not in self.ontology_info[self.ont_file_location].keys():
            self.ontology_info[self.ont_file_location] = {}
            self.ontology_info[self.ont_file_location]['Original GCS URL'] = gcs_org_url + self.ont_file_location
            self.ontology_info[self.ont_file_location]['Processed GCS URL'] = gcs_proc_url + self.ont_file_location
            key = 'Starting Statistics'
        else:
            key = 'Final Statistics'
        classes, obj_props = len(gets_ontology_classes(self.ont_graph)), len(gets_object_properties(self.ont_graph))
        triples, indv = len(self.ont_graph), len(list(self.ont_graph.triples((None, RDF.type, OWL.NamedIndividual))))
        stats = '{} Classes; {} Object Properties; {} Triples; {} Individuals'.format(classes, obj_props, triples, indv)
        self.ontology_info[self.ont_file_location][key] = stats

        return None

    def _finds_ontology_errors(self) -> Dict:
        """Catches different types of errors when attempting to load an ontology using the owlready2 library.

        Returns:
             errors: A dictionary keyed by error type with associated error message stored as the value.
        """

        errors = {}
        try: _ = get_ontology(self.temp_dir + '/' + self.ont_file_location).load()  # type: ignore
        except OwlReadyOntologyParsingError as o: errors['OwlReadyOntologyParsingError'] = str(o)  # type: ignore
        except KeyError as k: errors['KeyError'] = str(k)
        except TypeError as p: errors['PunningError'] = str(p)

        return errors

    def fixes_ontology_parsing_errors(self) -> None:
        """Fixes parsing errors detected when attempting to load the ontology. Currently, this method only fixes
        value errors (i.e. string-typed entities mis-typed as ints) because these are the only types of errors
        relevant to the set of ontologies utilized in the v2.0.0 PheKnowLator release. It can be easily extended in the
        future to accommodate other error types.

        Returns:
            None.
        """

        print('Finding Parsing Errors')
        logger.info('Finding Parsing Errors')

        errors, error_key, key = self._finds_ontology_errors(), 'OwlReadyOntologyParsingError', self.ont_file_location
        if error_key in errors.keys():
            line_num = int(re.findall(r'(?<=line\s).*(?=,)', str(errors[error_key]))[0]) - 1  # get error line
            raw_data = open(self.temp_dir + '/' + key).readlines()  # read raw data
            bad_content = re.findall(r'(?<=>).*(?=<)', raw_data[line_num])[0]
            bad_triple = [x for x in self.ont_graph if bad_content in str(x[0]) or bad_content in str(x[2])]
            for e in bad_triple:  # repair bad triple -- assuming for now the errors are miss-typed string errors
                self.ont_graph.add((e[0], e[1], Literal(str(e[2]), datatype=schema.string)))
                self.ont_graph.remove(e)

            self.ontology_info[key]['ValueErrors'] = errors[error_key]  # update ontology information dictionary

        return None

    def fixes_identifier_errors(self) -> None:
        """Identifies identifier prefix errors (e.g. "PRO" for the Protein Ontology, which should be "PR"). This is a
        tricky task to do in an automated manner and is something that should be updated if any new ontologies are
        added to the PheKnowLator build. Currently it checks and logs any hits, but only fixes the following known
        errors: Vaccine Ontology: "PRO" which should be "PR".

        Returns:
            None.
        """

        print('Fixing Identifier Errors')
        logger.info('Fixing Identifier Errors')

        known_errors = ['PRO', 'PR']  # known errors
        kg_classes, bad_cls, key = gets_ontology_classes(self.ont_graph), set(), self.ont_file_location
        class_list = [res for res in kg_classes if isinstance(res, URIRef) and 'obo/' in str(res)]
        all_cls = set([x.split('/')[-1].split('_')[0] for x in class_list])
        errors = [x for x in all_cls if any(i for i in all_cls if i != x and (i in x or x in i))]
        self.ontology_info[key]['IdentifierErrors'] = 'Possible ' + '-'.join(errors) if len(errors) > 0 else 'None'
        for edge in self.ont_graph:
            if 'http://purl.obolibrary.org/obo/{}_'.format(known_errors[0]) in str(edge[0]):
                updated_subj = str(edge[0]).replace(known_errors[0], known_errors[1])
                self.ont_graph.add((URIRef(updated_subj), edge[1], edge[2]))
                self.ont_graph.remove(edge)
                bad_cls.add(str(edge[0]))
            if 'http://purl.obolibrary.org/obo/{}_'.format(known_errors[0]) in str(edge[2]):
                updated_obj = str(edge[2]).replace(known_errors[0], known_errors[1])
                self.ont_graph.add((URIRef(updated_obj), edge[1], edge[2]))
                self.ont_graph.remove(edge)
                bad_cls.add(str(edge[2]))

            self.ontology_info[key]['IdentifierErrors'] = ', '.join(list(bad_cls)) if len(bad_cls) > 0 else 'None'

        return None

    def removes_deprecated_obsolete_entities(self) -> None:
        """Identifies and removes all deprecated and obsolete classes.

        Returns:
            None.
        """

        print('Removing Deprecated and Obsolete Classes')
        logger.info('Removing Deprecated and Obsolete Classes')

        ont, key, schma = self.ont_file_location.split('/')[-1].split('_')[0], self.ont_file_location, schema.boolean
        # get deprecated entity information
        dep_cls = [x[0] for x in list(self.ont_graph.triples((None, OWL.deprecated, Literal('true', datatype=schma))))]
        dep_triples = [x for x in self.ont_graph
                       if 'deprecated' in ', '.join([str(x[0]).lower(), str(x[1]).lower(), str(x[2]).lower()])
                       and len(list(self.ont_graph.triples((x[0], RDF.type, OWL.Class)))) == 1]
        deprecated_classes = set(dep_cls + [x[0] for x in dep_triples])
        # get obsolete entity information
        obs_cls = [x[0] for x in list(self.ont_graph.triples((None, RDFS.subClassOf, oboinowl.ObsoleteClass)))]
        obs_triples = [x for x in self.ont_graph
                       if 'obsolete' in ', '.join([str(x[0]).lower(), str(x[1]).lower(), str(x[2]).lower()])
                       and len(list(self.ont_graph.triples((x[0], RDF.type, OWL.Class)))) == 1 and '#' not in str(x[0])]
        obsolete_classes = set(obs_cls + [x[0] for x in obs_triples])
        # remove deprecated and obsolete information
        for node in list(deprecated_classes) + list(obsolete_classes):
            self.ont_graph.remove((node, None, None))

        self.ontology_info[key]['Deprecated'] = deprecated_classes if len(deprecated_classes) > 0 else 'None'
        self.ontology_info[key]['Obsolete'] = obsolete_classes if len(obsolete_classes) > 0 else 'None'

        return None

    def fixes_punning_errors(self) -> None:
        """Identifies and resolves three types of punning or entity redeclaration errors:
            (1) Entities typed as an owl:Class and owl:ObjectProperty --> removes owl:ObjectProperty
            (2) Entities typed as an owl:Class and owl:NamedIndividual --> removes owl:NamedIndividual
            (3) Entities typed as an OWL:ObjectProperty and owl:AnnotationProperty --> removes owl:AnnotationProperty
              - Any owl:ObjectProperty redeclared as an owl:AnnotationProperty never used in other triples are removed

        Returns:
             None.
        """

        print('Resolving Punning Errors')
        logger.info('Resolving Punning Errors')

        key, bad_cls, bad_obj = self.ont_file_location, set(), set()
        onts_entities = set([x[0] for x in tqdm(self.ont_graph)])
        for x in tqdm(onts_entities):
            ent_types = list(self.ont_graph.triples((x, RDF.type, None)))
            if len(ent_types) > 1:
                if not any([x for x in ent_types if 'owl' not in str(x[2])]):
                    # class + object properties --> remove object properties
                    class_prop, obj_prop = (x, RDF.type, OWL.Class), (x, RDF.type, OWL.ObjectProperty)
                    if class_prop in ent_types and obj_prop in ent_types and str(x) not in bad_cls:
                        self.ont_graph.remove(obj_prop)
                        bad_cls.add(str(x))
                    # class + individual --> remove individual
                    class_prop, ind_prop = (x, RDF.type, OWL.Class), (x, RDF.type, OWL.NamedIndividual)
                    if class_prop in ent_types and ind_prop in ent_types and str(x) not in bad_cls:
                        self.ont_graph.remove(ind_prop)
                        bad_cls.add(str(x))
                    # object property + annotation property --> remove annotation property
                    obj_prop, a_prop = (x, RDF.type, OWL.ObjectProperty), (x, RDF.type, OWL.AnnotationProperty)
                    if obj_prop in ent_types and a_prop in ent_types and str(x) not in bad_obj:
                        self.ont_graph.remove(a_prop)
                        bad_obj.add(str(x))
                        # check if the object property is used beyond definition -- if not, delete it
                        out_edges = list(self.ont_graph.triples((x, None, None)))
                        in_edges = list(self.ont_graph.triples((None, None, x)))
                        val_nodes = [i for i in out_edges + in_edges if len(
                            [j[2] for j in list(self.ont_graph.triples((i, RDF.type, OWL.Class)))] +
                            [j[2] for j in list(self.ont_graph.triples((i, RDF.type, OWL.NamedIndividual)))]) > 0]
                        if len(val_nodes) == 0:
                            self.ont_graph.remove((x, None, None))
                            bad_obj.add(str(x))

        self.ontology_info[key]['PunningErrors - Classes'] = ', '.join(bad_cls) if len(bad_cls) > 0 else 'None'
        self.ontology_info[key]['PunningErrors - ObjectProperty'] = ', '.join(bad_obj) if len(bad_obj) > 0 else 'None'

        return None

    def normalizes_duplicate_classes(self) -> None:
        """Makes sure that all classes representing the same entity are connected to each other. For example, the
        Sequence Ontology, ChEBI, PRotein Ontology all include terms for protein, but none of these classes are
        connected to each other. The follow classes occur in all of the ontologies used in the current build and have
        to be normalized so that there are not multiple versions of the same concept:
            - Gene: VO OGG_0000000002. Make the OGG class a subclass of SO gene (SO_0000704)
            - Protein: SO_0000104, PR_000000001, CHEBI_36080. Make CHEBI and PR a subclass of SO protein (SO_0000104)
            - Disorder: VO OGMS_0000045. Make OGMS a subclass of MONDO disease (MONDO_0000001)
            - Antigen: VO OBI_1110034. Make OBI a subclass of CHEBI antigen (CHEBI_59132)
            - Gelatin: VO_0003030. Make the VO a subclass of CHEBI gelatin (CHEBI_5291)
            - Hormone: VO FMA_12278. Make FMA a subclass of CHEBI hormone (CHEBI_24621)

        Returns:
             None.
        """

        print('Normalizing Duplicate Concepts')
        logger.info('Normalizing Duplicate Concepts')

        self.ont_graph.add((obo.OGG_0000000002, RDFS.subClassOf, obo.SO_0000704))  # fix gene class inconsistencies
        self.ont_graph.add((obo.PR_000000001, RDFS.subClassOf, obo.SO_0000104))  # fix protein class inconsistencies
        self.ont_graph.add((obo.CHEBI_36080, RDFS.subClassOf, obo.SO_0000104))  # fix protein class inconsistencies
        self.ont_graph.add((obo.OGMS_0000045, RDFS.subClassOf, obo.MONDO_0000001))  # fix disorder class inconsistencies
        self.ont_graph.add((obo.OBI_1110034, RDFS.subClassOf, obo.CHEBI_59132))  # fix antigen class inconsistencies
        self.ont_graph.add((obo.VO_0003030, RDFS.subClassOf, obo.CHEBI_5291))  # fix gelatin class inconsistencies
        self.ont_graph.add((obo.FMA_12278, RDFS.subClassOf, obo.CHEBI_24621))  # fix hormone class inconsistencies
        # update ontology information dictionary
        results = ['OGG_0000000002 rdfs:subClassOf SO_0000704', 'PR_000000001 rdfs:subClassOf SO_0000104',
                   'CHEBI_36080 rdfs:subClassOf SO_0000104', 'OGMS_0000045 rdfs:subClassOf MONDO_0000001',
                   'OBI_1110034 rdfs:subClassOf CHEBI_59132', 'VO_0003030 rdfs:subClassOf CHEBI_5291',
                   'FMA_12278 rdfs:subClassOf CHEBI_24621']
        self.ontology_info[self.ont_file_location]['Normalized - Duplicates'] = ', '.join(results)

        return None

    def normalizes_existing_classes(self) -> None:
        """Checks for inconsistencies in ontology classes that overlap with non-ontology entity identifiers (e.g. if
        HP includes HGNC identifiers, but PheKnowLator utilizes Entrez gene identifiers). While there are other types of
        identifiers, we focus primarily on resolving the genomic types, since we have a master dictionary we can used to
        help with this (Merged_gene_rna_protein_identifiers.pkl). This can be updated in future iterations to include
        other types of identifiers, but given our detailed examination of the v2.0.0 ontologies, these were the
        identifier types that needed repair.

        Returns:
            None.
        """

        print('Normalizing Existing Classes')
        logger.info('Normalizing Existing Classes')

        ents, missing = None, []
        non_ont = set([x for x in gets_ontology_classes(self.ont_graph) if not str(x).startswith(str(obo))])
        hgnc, url, g = set([x for x in non_ont if 'hgnc' in str(x)]), 'http://www.ncbi.nlm.nih.gov/gene/', self.gene_ids
        for node in tqdm(hgnc):
            trips = list(self.ont_graph.triples((node, None, None))) + list(self.ont_graph.triples((None, None, node)))
            nd = 'hgnc_id_' + str(node).split('/')[-1].split('=')[-1]
            gene_dat = [str(x[2]) for x in trips if x[1] == RDFS.label]
            if nd in g.keys(): ents = [URIRef(url + x) for x in g[nd] if x.startswith('entrez_id_')]
            elif str(node) not in self.withdrawn_map.keys() and len(gene_dat) > 0:
                if 'symbol_' + gene_dat[0] in g.keys() and 'entrez_id' in g['symbol_' + gene_dat[0]]:
                    ents = [URIRef(url + x) for x in g['symbol_' + gene_dat[0]] if 'entrez_id' in x][0]
                else: missing += [str(node)]
            elif str(node) in self.withdrawn_map.keys(): ents = [URIRef(url + x) for x in self.withdrawn_map[str(node)]]
            else: missing += [str(node)]
            if ents:
                for edge in trips:
                    if node in edge[0]:
                        if isinstance(edge[2], URIRef) or isinstance(edge[2], BNode):
                            for i in ents:
                                self.ont_graph.add((i, edge[1], edge[2]))
                                self.ont_graph.remove(edge)
                        else: self.ont_graph.remove(edge)
                    if node in edge[2]:
                        for i in ents:
                            self.ont_graph.add((edge[0], edge[1], i))
                            self.ont_graph.remove(edge)

        no_ont = len(non_ont) - len(hgnc)
        self.ontology_info[self.ont_file_location]['Normalized - NonOnt'] = no_ont if no_ont != 0 else 'None'
        self.ontology_info[self.ont_file_location]['Normalized - Dep'] = missing if len(missing) > 0 != 0 else 'None'
        self.ontology_info[self.ont_file_location]['Normalized - Gene IDs'] = len(hgnc) if len(hgnc) > 0 else 'None'

        return None

    def generates_ontology_report(self) -> None:
        """Parses the ontology_info dictionary in order to create a final ontology report summarizing the cleaning
        results performed on each ontology and the statistics on the final merged set of ontologies.

        Returns:
             None.
        """

        ontology_report_filename = 'ontology_cleaning_report.txt'
        ont_order = sorted([x for x in self.ontology_info.keys() if not x.startswith('Phe')]) + [self.ont_file_location]
        with open(self.temp_dir + '/' + ontology_report_filename, 'w') as o:
            o.write('=' * 50 + '\n{}'.format('ONTOLOGY CLEANING REPORT'))
            o.write('\n{}\n'.format(str(datetime.datetime.utcnow().strftime('%a %b %d %X UTC %Y'))) + '=' * 50 + '\n\n')
            for key in ont_order:
                o.write('\nONTOLOGY: {}\n'.format(key))
                x = self.ontology_info[key]
                if 'Original GCS URL' in x.keys(): o.write('\t- Original GCS URL: {}\n'.format(x['Original GCS URL']))
                if 'Processed GCS URL' in x: o.write('\t- Processed GCS URL: {}\n'.format(x['Processed GCS URL']))
                o.write('\t- Statistics:\n\t\t- Before Cleaning: {}\n'.format(x['Starting Statistics']))
                if 'Final Statistics' in x.keys(): o.write('\t\t- After Cleaning: {}\n'.format(x['Final Statistics']))
                if 'ValueErrors' in x.keys(): o.write('\t- Value Errors: {}\n'.format(x['ValueErrors']))
                if 'IdentifierErrors' in x.keys(): o.write('\t- Identifier Errors: {}\n'.format(x['IdentifierErrors']))
                if 'PheKnowLator_MergedOntologies' not in key:
                    if x['Deprecated'] != 'None':
                        o.write('\t- Deprecated Classes:\n')
                        for i in x['Deprecated']: o.write('\t\t- {}\n'.format(str(i)))
                    else: o.write('\t\t\t- {}\n'.format(x['Deprecated']))
                    if x['Obsolete'] != 'None':
                        o.write('\t- Obsolete Classes:\n')
                        for i in x['Obsolete']: o.write('\t\t- {}\n'.format(str(i)))
                    else: o.write('\t\t\t- {}\n'.format(x['Obsolete']))
                o.write('\t- Punning Error:\n\t\t- Classes:\n')
                if x['PunningErrors - Classes'] != 'None':
                    for i in x['PunningErrors - Classes'].split(', '):
                        o.write('\t\t\t- {}\n'.format(i))
                else: o.write('\t\t\t- {}\n'.format(x['PunningErrors - Classes']))
                o.write('\t\t- ObjectProperties:\n')
                if x['PunningErrors - ObjectProperty'] != 'None':
                    for i in x['PunningErrors - ObjectProperty'].split(', '): o.write('\t\t\t- {}\n'.format(i))
                else: o.write('\t\t\t- {}\n'.format(x['PunningErrors - ObjectProperty']))
                if 'Normalized - Duplicates' in x.keys():
                    o.write('\t- Entity Normalization:\n')
                    if x['Normalized - Duplicates'] != 'None':
                        for i in x['Normalized - Duplicates'].split(', '): o.write('\t\t- {}\n'.format(i))
                    else: o.write('\t\t- {}\n'.format(x['Normalized - Duplicates']))
                    o.write('\t\t- Other Classes that May Need Normalization: {}\n'.format(x['Normalized - NonOnt']))
                    o.write('\t\t- Normalized HGNC IDs: {}\n'.format(x['Normalized - Gene IDs']))
                    o.write('\t- Deprecated Ontology HGNC Identifiers Needing Alignment:\n')
                    if x['Normalized - Dep'] != 'None':
                        for i in x['Normalized - Dep']: o.write('\t\t- {}\n'.format(i))
                    else: o.write('\t\t- {}\n'.format(x['Normalized - Dep']))

        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, ontology_report_filename)

        return None

    def cleans_ontology_data(self) -> None:
        """Performs all needed ontology cleaning tasks by resolving different types of ontology cleaning steps at the
        individual ontology- and the merged ontology-level, each are described below:
            - Individual Ontologies: (1) Parsing Errors, (2) Identifier Errors, (3) Deprecated/Obsolete Errors, and (4)
              Punning Errors.
            - Merged Ontologies: (1) Identifier Error, (2) Normalizes Duplicate and Existing Concepts, and (3) Punning
              Errors.

        Returns:
            None.
        """

        print('*** CLEANING INDIVIDUAL ONTOLOGY DATA SOURCES ***')
        logger.info('*** CLEANING INDIVIDUAL ONTOLOGY DATA SOURCES ***')
        for ont in self.ontology_info.keys():
            if ont != self.merged_ontology_filename:
                print('\nProcessing Ontology: {}'.format(ont.upper()))
                self.ont_file_location, self.ont_graph = ont, self.reads_gcs_bucket_data_to_graph(ont)
                self.updates_ontology_reporter()  # get starting statistics
                self.fixes_ontology_parsing_errors()
                self.fixes_identifier_errors()
                self.removes_deprecated_obsolete_entities()
                self.fixes_punning_errors()
                self.updates_ontology_reporter()  # get finishing statistics
                self._logically_verifies_cleaned_ontologies()
                if self.bucket != '': uploads_data_to_gcs_bucket(self.bucket, self.current_build, log_dir, log)

        print('\n\n*** CLEANING MERGED ONTOLOGY DATA ***')
        logger.info('\n\n*** CLEANING MERGED ONTOLOGY DATA ***')
        self.ont_file_location = self.merged_ontology_filename
        individual_ontologies = self.checks_for_downloaded_ontology_data()
        self.merge_ontologies(individual_ontologies, self.temp_dir + '/', self.ont_file_location)
        if self.bucket != '': uploads_data_to_gcs_bucket(self.bucket, self.current_build, log_dir, log)
        print('\nLoading Merged Ontology')
        self.ont_graph = Graph().parse(self.temp_dir + '/' + self.ont_file_location)
        self.updates_ontology_reporter()  # get starting statistics
        self.fixes_identifier_errors()
        self.normalizes_duplicate_classes()
        self.normalizes_existing_classes()
        self.fixes_punning_errors()
        self.updates_ontology_reporter()  # get finishing statistics
        if self.bucket != '': uploads_data_to_gcs_bucket(self.bucket, self.current_build, log_dir, log)
        # serializes final ontology graph and uploads graph data and ontology report to gcs
        self.ont_graph.serialize(destination=self.temp_dir + '/' + self.ont_file_location, format='xml')
        ontology_file_formatter(self.temp_dir, '/' + self.ont_file_location, self.owltools_location)
        uploads_data_to_gcs_bucket(self.bucket, self.processed_data, self.temp_dir, self.ont_file_location)
        if self.bucket != '': uploads_data_to_gcs_bucket(self.bucket, self.current_build, log_dir, log)

        print('\n\n*** GENERATING ONTOLOGY CLEANING REPORT ***')
        logger.info('\n\n*** GENERATING ONTOLOGY CLEANING REPORT ***')
        self.generates_ontology_report()
        if self.bucket != '': uploads_data_to_gcs_bucket(self.bucket, self.current_build, log_dir, log)

        return None
