#!/bin/bash

# run script from project root
path+="resources/ontology_source_list.txt"
echo $path

if ! [[ -e README.md ]]; then
    echo "Please run from the root of the project."
    exit 1
fi

# run python script to download ontologies and generate data source metadata documentation
python scripts/python/OntologyData.py -f "$path"


