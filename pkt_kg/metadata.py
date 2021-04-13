#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries
import glob
import json
import logging.config
import os
import os.path
import pandas  # type: ignore
import pickle
import re
import subprocess

from datetime import date, datetime
from rdflib import Graph, Literal, Namespace, URIRef   # type: ignore
from rdflib.namespace import RDF, RDFS, OWL  # type: ignore
from tqdm import tqdm  # type: ignore
from typing import Dict, List, Optional, Set, Union

from pkt_kg.utils import *

# set environmental variables
oboinowl = Namespace('http://www.geneontology.org/formats/oboInOwl#')
obo = Namespace('http://purl.obolibrary.org/obo/')
# logging
log_dir, log, log_config = 'builds/logs', 'pkt_build_log.log', glob.glob('**/logging.ini', recursive=True)
try:
    if not os.path.exists(log_dir): os.mkdir(log_dir)
except FileNotFoundError:
    log_dir, log_config = '../builds/logs', glob.glob('../builds/logging.ini', recursive=True)
    if not os.path.exists(log_dir): os.mkdir(log_dir)
logger = logging.getLogger(__name__)
logging.config.fileConfig(log_config[0], disable_existing_loggers=False, defaults={'log_file': log_dir + '/' + log})


class Metadata(object):
    """Class helps manage knowledge graph metadata.

    Attributes:
        kg_version: A string that contains the version of the knowledge graph build.
        write_location: A filepath to the knowledge graph directory (e.g. './resources/knowledge_graphs).
        kg_location: The subdirectory and name of the knowledge graph (e.g.'/relations_only/KG.owl').
        node_data: A filepath to a directory called 'node_data' containing a file for each instance/subclass node.
        node_dict: A nested dictionary storing metadata for the nodes in the edge_dict. Where the outer key is a node
            identifier and each inner key is an identifier type keyed by the identifier type with the value being the
            corresponding metadata for that identifier type. For example:
                { 'nodes': {
                    'http://www.ncbi.nlm.nih.gov/gene/1': {
                        'Label': 'A1BG',
                        'Description': "A1BG is protein-coding' and is located on chromosome 19 (19q13.43).",
                        'Synonym': 'HYST2477alpha-1B-glycoprotein|HEL-S-163pA|ABG|A1B|GAB'} ... },
                'relations': {
                    'http://purl.obolibrary.org/obo/RO_0002533': {
                        'Label': 'sequence atomic unit',
                        'Description': 'Any individual unit of a collection of like units arranged in a linear order',
                        'Synonym': 'None'} ... }
                }
    """

    def __init__(self, kg_version: str, write_location: str, kg_location: str, node_data: Optional[List],
                 node_dict: Optional[Dict]) -> None:

        self.kg_version: str = kg_version
        self.write_location: str = write_location
        self.full_kg: str = kg_location
        self.node_data = node_data
        self.node_dict = node_dict

    def metadata_processor(self) -> None:
        """Loads a directory of node and relations data. The dictionary is nested with the outer keys corresponding
        to the metadata type (i.e. "nodes" or "relations") and the values containing dictionaries keyed by URI and
        values containing a dictionary of metadata.

        Returns:
            None.
        """

        if self.node_data:
            log_str = 'Loading and Processing Node Metadata'; print(log_str); logger.info(log_str)
            with open(self.node_data[0], 'rb') as out:
                self.node_dict = pickle.load(out, encoding="utf8")

        return None

    def extract_metadata(self, graph: Graph) -> None:
        """Functions queries the knowledge graph to obtain labels, definitions/descriptions, and synonyms for all
        owl:Class, owl:NamedIndividual, and owl:ObjectProperty objects. This information is then added to the existing
        self.node_dict dictionary under the key of "nodes" (for owl:Class and owl:NamedIndividual) or "relations" (for
        owl:ObjectProperty). Each metadata type is saved as a dictionary key with the actual string stored as the
        value. The metadata types are packaged as a dictionary which is stored as the value to the node identifier as
        the key.

        Args:
            graph: An rdflib graph object.

        Returns:
            None.
        """

        log_str = 'Extracting Class and Relation Metadata'; print('\n' + log_str); logger.info(log_str)

        if self.node_dict:
            domains = [
                ('nodes', [i[0] for i in list(graph.triples((None, RDF.type, None)))
                           if isinstance(i[0], URIRef)
                           and (OWL.Class in i[2] or OWL.NamedIndividual in i[2])
                           and ('#' not in str(i[0]) or '#' not in str(i[2]))]),
                ('relations', [i[0] for i in list(graph.triples((None, RDF.type, None)))
                               if isinstance(i[0], URIRef) and i[2] == OWL.ObjectProperty])
            ]
            for key, entities in domains:
                temp_dict = dict()
                for i in tqdm(entities):
                    labels = [x for x in list(graph.triples((i, RDFS.label, None)))]
                    descriptions = [x for x in list(graph.triples((i, obo.IAO_0000115, None)))]
                    synonyms = [x for x in list(graph.triples((i, None, None))) if 'synonym' in str(x[1]).lower()]
                    if len(labels) != 0:
                        temp_dict[str(i)] = {
                            'Label': str(labels[0][2]) if len(labels) > 0 else None,
                            'Description': str(descriptions[0][2]) if len(descriptions) > 0 else None,
                            'Synonym': '|'.join([str(c[2]) for c in synonyms]) if len(synonyms) > 0 else None
                        }
                self.node_dict[key] = {**self.node_dict[key], **temp_dict}

            # add rdfs:subclassof and rdf:type
            self.node_dict['relations'] = {
                **self.node_dict['relations'],
                **{'http://www.w3.org/2000/01/rdf-schema#subClassOf': {
                    'Label': 'subClassOf', 'Description': 'The subject is a subclass of a class.', 'Synonym': 'None'},
                    'http://www.w3.org/1999/02/22-rdf-syntax-ns#type': {
                        'Label': 'type', 'Description': 'The subject is an instance of a class.', 'Synonym': 'None'}}}

            if self.node_data:
                with open(self.node_data[0], 'wb') as out:
                    pickle.dump(self.node_dict, out)

        return None

    def creates_node_metadata(self, ent: List, e_type: Optional[List] = None, key_type: str = 'nodes') -> Graph:
        """Given a node in the knowledge graph, if the node is not an ontology class and if it has metadata information,
        then new edges are created to add the metadata to the knowledge graph. Metadata that is added includes: labels,
        descriptions, and synonyms.

        Args:
            ent: A list of two node identifiers (e.g. ['http://example/3075', 'http://example/1080']).
            e_type: A list of types for each node in nodes (e.g. ['entity', 'entity']).
            key_type: A string indicating if the key should be 'nodes' or 'relations (default='nodes').

        Returns:
            edges: A list of tuples containing RDFLib objects used to add metadata to a knowledge graph.
        """

        key, edges, x = key_type, [], []
        if self.node_dict and isinstance(self.node_dict, Dict):
            if key == 'nodes' and isinstance(e_type, List):
                x = [i for i in ent if e_type[ent.index(i)] != 'class' and i in self.node_dict[key].keys()]
            elif key == 'relations': x = [i for i in ent if i in self.node_dict[key].keys()]
            else: return None
            # check for matches
            if (key == 'relations' and e_type is None) and len(x) == 0: return None
            elif e_type == ['class', 'class'] and len(x) == 0: return None
            elif (e_type == ['class', 'entity'] or e_type == ['entity', 'class']) and len(x) == 0: return None
            elif e_type == ['entity', 'entity'] and len(x) != 2: return None
            else:
                for i in x:
                    metadata_info = self.node_dict[key][i]
                    if 'Label' in metadata_info.keys():
                        if metadata_info['Label'] is not None and 'None' not in metadata_info['Label']:
                            edges += [(URIRef(i), RDFS.label, Literal(metadata_info['Label']))]
                    if 'Description' in metadata_info.keys():
                        if metadata_info['Description'] is not None and 'None' not in metadata_info['Description']:
                            edges += [(URIRef(i), URIRef(obo + 'IAO_0000115'), Literal(metadata_info['Description']))]
                    if 'Synonym' in metadata_info.keys():
                        if metadata_info['Synonym'] is not None and 'None' not in metadata_info['Synonym']:
                            for syn in metadata_info['Synonym'].split('|'):
                                edges += [(URIRef(i), URIRef(oboinowl + 'hasSynonym'), Literal(syn))]
                return edges
        else: return None

    def adds_ontology_annotations(self, filename: str, graph: Graph) -> Graph:
        """Updates the ontology annotation information for an input knowledge graph or ontology.

        Args:
            filename: A string containing the name of a knowledge graph.
            graph: An rdflib graph object.

        Returns:
            graph: An rdflib graph object with edited ontology annotations.
        """

        log_str = 'Adding Ontology Annotations'; print('\n' + log_str); logger.info(log_str)

        # set annotation variables
        authors = 'Authors: Tiffany J. Callahan, William A. Baumgartner, Ignacio Tripodi, Adrianne L. ' \
                  'Stefanski, Lawrence E. Hunter'
        date_full = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
        # convert filename to permanent url
        parsed_filename = '_'.join(filename.lower().split('/')[-1].split('_')[2:])
        url = 'https://pheknowlator.com/pheknowlator_' + parsed_filename[:-4]
        pkt_url = 'https://github.com/callahantiff/PheKnowLator'

        # query ontology to obtain existing ontology annotations and remove them
        results = graph.query(
            """SELECT DISTINCT ?o ?p ?s
                WHERE {
                    ?o rdf:type owl:Ontology .
                    ?o ?p ?s . }
            """, initNs={'rdf': RDF, 'owl': OWL})
        for res in results:
            graph.remove(res)

        # add new annotations
        graph.add((URIRef(url + '.owl'), RDF.type, URIRef(OWL + 'Ontology')))
        graph.add((URIRef(url + '.owl'), URIRef(oboinowl + 'default-namespace'), Literal(filename)))
        graph.add((URIRef(url + '.owl'), URIRef(OWL + 'versionIRI'), URIRef(pkt_url + '/wiki/' + self.kg_version)))
        graph.add((URIRef(url + '.owl'), RDFS.comment, Literal('PheKnowLator Release version ' + self.kg_version)))
        graph.add((URIRef(url + '.owl'), URIRef(oboinowl + 'date'), Literal(date_full)))
        graph.add((URIRef(url + '.owl'), RDFS.comment, Literal(authors)))
        graph.add((URIRef(url + '.owl'), RDFS.comment, Literal('For more information visit: ' + pkt_url)))

        return graph

    def output_metadata(self, node_integer_map: Dict, graph: Union[Set, Graph]) -> None:
        """Loops over the self.node_dict dictionary and writes out the data to a file locally. The data is stored as
        a tab-delimited '.txt' file with four columns: (1) node identifier; (2) node label; (3) node description or
        definition; and (4) node synonym.

        NOTE. Not every node in the knowledge class will have metadata. There are some non-ontology nodes that are
        added (e.g. Ensembl transcript identifiers) that at the time of adding did not include labels, synonyms,
        or definitions. While these nodes have valid metadata through their original provider, this data may not have
        been available for download and thus would not have been added to the node_dict.

        Args:
            node_integer_map: A dictionary where keys are integers and values are node and relation identifiers.
            graph: A set of RDFLib Graph object triples or an RDFLib Graph.

        Returns:
            None.
        """

        if self.node_dict:
            log_str = 'Writing Class Metadata'; print(log_str); logger.info(log_str)
            entities = set([i for j in tqdm(graph) for i in j]); filename = self.full_kg[:-4] + '_NodeLabels.txt'
            with open(self.write_location + filename, 'w', encoding='utf-8') as out:
                out.write('entity_type' + '\t' + 'integer_id' + '\t' + 'entity_uri' + '\t' + 'label' + '\t' +
                          'description/definition' + '\t' + 'synonym' + '\n')
                for x in tqdm(entities):
                    nid, nint = n3(x), node_integer_map[n3(x)]
                    if str(x) in self.node_dict['nodes'].keys():
                        etyp, meta = 'NODES', self.node_dict['nodes'][str(x)]
                    elif str(x) in self.node_dict['relations'].keys():
                        etyp, meta = 'RELATIONS', self.node_dict['relations'][str(x)]
                    else: meta, etyp, lab, dsc, syn = None, 'NA', 'NA', 'NA', 'NA'
                    if meta is not None:
                        lab = meta['Label'] if meta['Label'] is not None else 'None'
                        dsc = meta['Description'] if meta['Description'] is not None else 'None'
                        syn = meta['Synonym'] if meta['Synonym'] is not None else 'None'
                    try: out.write(etyp + '\t' + str(nint) + '\t' + nid + '\t' + lab + '\t' + dsc + '\t' + syn + '\n')
                    except UnicodeEncodeError:
                        out.write(etyp.encode('utf-8').decode() + '\t' + str(nint).encode('utf-8').decode() +
                                  '\t' + nid.encode('utf-8').decode() + '\t' + lab.encode('utf-8').decode() +
                                  '\t' + dsc.encode('utf-8').decode() + '\t' + syn.encode('utf-8').decode() + '\n')

        return None
