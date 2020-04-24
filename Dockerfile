FROM python:3.6.2

# install library from GitHub
RUN pip install git+https://github.com/callahantiff/PheKnowLator.git@adding_docker

# create new project directory and add needed pkt_kg subdirectories
RUN mkdir /home/PheKnowLator
RUN mkdir /home/PheKnowLator/resources
RUN mkdir /home/PheKnowLator/resources/construction_approach
RUN mkdir /home/PheKnowLator/resources/edge_data
RUN mkdir /home/PheKnowLator/resources/knowledge_graphs
RUN mkdir /home/PheKnowLator/resources/owl_decoding
RUN mkdir /home/PheKnowLator/resources/processed_data

# copy directories
COPY resources/node_data /home/PheKnowLator/resources/node_data
COPY resources/relations_data /home/PheKnowLator/resources/relations_data

# copy filess
COPY Main.py /home/PheKnowLator
COPY resources/edge_source_list.txt /home/PheKnowLator/resources/edge_source_list.txt
COPY resources/ontology_source_list.txt /home/PheKnowLator/resources/ontology_source_list.txt
COPY resources/resource_info.txt /home/PheKnowLator/resources/resource_info.txt
COPY resources/construction_approach/subclass_construction_map.pkl /home/PheKnowLator/resources/construction_approach/subclass_construction_map.pkl
COPY resources/knowledge_graphs/PheKnowLator_MergedOntologiesGeneID_Normalized_Cleaned.owl /home/PheKnowLator/resources/knowledge_graphs/PheKnowLator_MergedOntologiesGeneID_Normalized_Cleaned.owl
COPY resources/owl_decoding/OWL_NETS_Property_Types.txt /home/PheKnowLator/resources/owl_decoding/OWL_NETS_Property_Types.txt
COPY resources/processed_data/MESH_CHEBI_MAP.txt /home/PheKnowLator/resources/processed_data/MESH_CHEBI_MAP.tx
COPY resources/processed_data/DISEASE_DOID_MAP.txt /home/PheKnowLator/resources/processed_data/DISEASE_DOID_MAP.txt
COPY resources/processed_data/PHENOTYPE_HPO_MAP.txt /home/PheKnowLator/resources/processed_data/PHENOTYPE_HPO_MAP.txt
COPY resources/processed_data/ENTREZ_GENE_PRO_ONTOLOGY_MAP.txt /home/PheKnowLator/resources/processed_data/ENTREZ_GENE_PRO_ONTOLOGY_MAP.txt
COPY resources/processed_data/GENE_SYMBOL_ENSEMBL_TRANSCRIPT_MAP.txt /home/PheKnowLator/resources/processed_data/GENE_SYMBOL_ENSEMBL_TRANSCRIPT_MAP.txt
COPY resources/processed_data/ENSEMBL_GENE_ENTREZ_GENE_MAP.txt /home/PheKnowLator/resources/processed_data/ENSEMBL_GENE_ENTREZ_GENE_MAP.txt
COPY resources/processed_data/UNIPROT_ACCESSION_PRO_ONTOLOGY_MAP.txt /home/PheKnowLator/resources/processed_data/UNIPROT_ACCESSION_PRO_ONTOLOGY_MAP.txt
COPY resources/processed_data/HPA_GTEx_TISSUE_CELL_MAP.txt /home/PheKnowLator/resources/processed_data/HPA_GTEx_TISSUE_CELL_MAP.txt
COPY resources/processed_data/STRING_PRO_ONTOLOGY_MAP.txt /home/PheKnowLator/resources/processed_data/STRING_PRO_ONTOLOGY_MAP.txt

# change permissions
RUN chmod -R 755 /home/PheKnowLator

ENTRYPOINT ["python",  "./home/PheKnowLator/Main.py", "--onts", "/home/PheKnowLator/resources/ontology_source_list.txt", "--edg", "/home/PheKnowLator/resources/edge_source_list.txt", "--res", "/home/PheKnowLator/resources/resource_info.txt", "--out", "/home/PheKnowLator/resources/knowledge_graphs"]
CMD ["-h"]
