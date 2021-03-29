#!/usr/local/bin/docker
# -*- version: 20.10.2 -*-

############################################
## MULTI-STAGE CONTAINER CONFIGURATION ##
FROM python:3.6.2
RUN apt-get update && apt-get install -y \
    apt-transport-https \
    software-properties-common \
    unzip \
    curl
RUN wget -O- https://apt.corretto.aws/corretto.key | apt-key add - && \
    add-apt-repository 'deb https://apt.corretto.aws stable main' && \
    apt-get update && \
    apt-get install -y java-1.8.0-amazon-corretto-jdk


############################################
## PHEKNOWLATOR (PKT_KG) PROJECT SETTINGS ##
# create needed project directories
WORKDIR /PheKnowLator
RUN mkdir -p /PheKnowLator
RUN mkdir -p /PheKnowLator/resources
RUN mkdir -p /PheKnowLator/resources/construction_approach
RUN mkdir -p /PheKnowLator/resources/edge_data
RUN mkdir -p /PheKnowLator/resources/knowledge_graphs
RUN mkdir -p /PheKnowLator/resources/node_data
RUN mkdir -p /PheKnowLator/resources/ontologies
RUN mkdir -p /PheKnowLator/resources/processed_data
RUN mkdir -p /PheKnowLator/resources/relations_data

# copy scripts/files needed to run pkt_kg
COPY pkt_kg /PheKnowLator/pkt_kg
COPY Main.py /PheKnowLator
COPY setup.py /PheKnowLator
COPY README.rst /PheKnowLator
COPY resources /PheKnowLator/resources

# download and copy needed data
RUN curl -O https://storage.googleapis.com/pheknowlator/current_build/data/processed_data/edge_source_list.txt && mv edge_source_list.txt resources/
RUN curl -O https://storage.googleapis.com/pheknowlator/current_build/data/processed_data/ontology_source_list.txt && mv ontology_source_list.txt resources/
RUN curl -O https://storage.googleapis.com/pheknowlator/current_build/data/processed_data/resource_info.txt && mv resource_info.txt resources/
RUN curl -O https://storage.googleapis.com/pheknowlator/current_build/data/processed_data/subclass_construction_map.pkl && mv subclass_construction_map.pkl resources/construction_approach/
RUN curl -O https://storage.googleapis.com/pheknowlator/current_build/data/processed_data/PheKnowLator_MergedOntologies.owl && mv PheKnowLator_MergedOntologies.owl resources/knowledge_graphs/
RUN curl -O https://storage.googleapis.com/pheknowlator/current_build/data/processed_data/node_metadata_dict.pkl && mv node_metadata_dict.pkl resources/node_data/
RUN curl -O https://storage.googleapis.com/pheknowlator/current_build/data/processed_data/DISEASE_MONDO_MAP.txt && mv DISEASE_MONDO_MAP.txt resources/processed_data/
RUN curl -O https://storage.googleapis.com/pheknowlator/current_build/data/processed_data/ENSEMBL_GENE_ENTREZ_GENE_MAP.txt && mv ENSEMBL_GENE_ENTREZ_GENE_MAP.txt resources/processed_data/
RUN curl -O https://storage.googleapis.com/pheknowlator/current_build/data/processed_data/ENTREZ_GENE_PRO_ONTOLOGY_MAP.txt && mv ENTREZ_GENE_PRO_ONTOLOGY_MAP.txt resources/processed_data/
RUN curl -O https://storage.googleapis.com/pheknowlator/current_build/data/processed_data/GENE_SYMBOL_ENSEMBL_TRANSCRIPT_MAP.txt && mv GENE_SYMBOL_ENSEMBL_TRANSCRIPT_MAP.txt resources/processed_data/
RUN curl -O https://storage.googleapis.com/pheknowlator/current_build/data/processed_data/ENTREZ_GENE_ENSEMBL_TRANSCRIPT_MAP.txt && mv ENTREZ_GENE_ENSEMBL_TRANSCRIPT_MAP.txt resources/processed_data/
RUN curl -O https://storage.googleapis.com/pheknowlator/current_build/data/processed_data/HPA_GTEx_TISSUE_CELL_MAP.txt && mv HPA_GTEx_TISSUE_CELL_MAP.txt resources/processed_data/
RUN curl -O https://storage.googleapis.com/pheknowlator/current_build/data/processed_data/MESH_CHEBI_MAP.txt && mv MESH_CHEBI_MAP.txt resources/processed_data/
RUN curl -O https://storage.googleapis.com/pheknowlator/current_build/data/processed_data/PHENOTYPE_HPO_MAP.txt && mv PHENOTYPE_HPO_MAP.txt resources/processed_data/
RUN curl -O https://storage.googleapis.com/pheknowlator/current_build/data/processed_data/STRING_PRO_ONTOLOGY_MAP.txt && mv STRING_PRO_ONTOLOGY_MAP.txt resources/processed_data/
RUN curl -O https://storage.googleapis.com/pheknowlator/current_build/data/processed_data/UNIPROT_ACCESSION_PRO_ONTOLOGY_MAP.txt && mv UNIPROT_ACCESSION_PRO_ONTOLOGY_MAP.txt resources/processed_data/
RUN curl -O https://storage.googleapis.com/pheknowlator/current_build/data/processed_data/INVERSE_RELATIONS.txt && mv INVERSE_RELATIONS.txt resources/relations_data/
RUN curl -O https://storage.googleapis.com/pheknowlator/current_build/data/processed_data/RELATIONS_LABELS.txt && mv RELATIONS_LABELS.txt resources/relations_data/

# install needed python libraries
RUN pip install --upgrade pip setuptools
WORKDIR /PheKnowLator
RUN pip install .


############################################
## GLOBAL ENVRIONMENT SETTINGS ##
# copy files needed to run docker container
COPY entrypoint.sh /PheKnowLator

# update permissions for all files
RUN chmod -R 755 /PheKnowLator

# set OWlTools memory (set to a high value, system will only use available memory)
ENV OWLTOOLS_MEMORY=500g
RUN echo $OWLTOOLS_MEMORY

# set python envrionment encoding
RUN export PYTHONIOENCODING=utf-8


############################################
## CONTAINER ACCESS ##
# call bash script which contains entrypoint parameters
ENTRYPOINT ["/PheKnowLator/entrypoint.sh"]
CMD ["-h"]
