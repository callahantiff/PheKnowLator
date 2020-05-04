#!/bin/bash

cd /PheKnowLator && \

python Main.py \
            --onts resources/ontology_source_list.txt \
            --edg resources/edge_source_list.txt \
            --res resources/resource_info.txt \
            --out resources/knowledge_graphs \
            "$@"

