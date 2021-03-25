#!/bin/bash

# move into pkt_kg root directory "PheKnowLator"
cd /PheKnowLator && \

# set base parameters to pass when initializing script
python Main.py \
            --onts resources/ontology_source_list.txt \
            --edg resources/edge_source_list.txt \
            --res resources/resource_info.txt \
            --cpus None \
            --out resources/knowledge_graphs \
            "$@"
