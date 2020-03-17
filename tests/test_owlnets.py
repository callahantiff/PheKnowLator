#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import needed libraries


class_list2 = [rdflib.URIRef('http://purl.obolibrary.org/obo/GO_0000785'),
               rdflib.URIRef('http://purl.obolibrary.org/obo/CL_0000995'),
               rdflib.URIRef('http://purl.obolibrary.org/obo/PATO_0000380'),
               rdflib.URIRef('http://purl.obolibrary.org/obo/HP_0000340'),
               rdflib.URIRef('http://purl.obolibrary.org/obo/GO_0000228'),
               rdflib.URIRef('http://purl.obolibrary.org/obo/PR_000050170'),
               rdflib.URIRef('http://purl.obolibrary.org/obo/CL_0000037'),
               rdflib.URIRef('http://purl.obolibrary.org/obo/ENVO_01001057')]



for x in cleaned_classes:
    print(str(x[0]) + '\t' + str(x[1]) + '\t' + str(x[2]))