FROM python:3.6.2

# install library from GitHub
RUN pip install git+https://github.com/callahantiff/PheKnowLator.git@adding_docker

# create new project directory and add needed pkt_kg subdirectories
RUN mkdir /home/resources
RUN mkdir /home/resources/construction_approach
RUN mkdir /home/resources/edge_data
RUN mkdir /home/resources/knowledge_graphs
RUN mkdir /home/resources/owl_decoding
RUN mkdir /home/resources/processed_data

# copy directories
COPY resources/node_data /home/resources/node_data
COPY resources/relations_data /home/resources/relations_data

# copy filess
COPY Main.py /home
COPY resources/edge_source_list.txt /home/resources/edge_source_list.txt
COPY resources/ontology_source_list.txt /home/resources/ontology_source_list.txt
COPY resources/resource_info.txt /home/resources/resource_info.txt
COPY resources/construction_approach/subclass_construction_map.pkl /home/resources/construction_approach/subclass_construction_map.pkl
COPY resources/knowledge_graphs/PheKnowLator_MergedOntologiesGeneID_Normalized_Cleaned.owl /home/resources/knowledge_graphs/PheKnowLator_MergedOntologiesGeneID_Normalized_Cleaned.owl
COPY resources/owl_decoding/OWL_NETS_Property_Types.txt /home/resources/owl_decoding/OWL_NETS_Property_Types.txt
COPY resources/processed_data/MESH_CHEBI_MAP.txt /home/resources/processed_data/MESH_CHEBI_MAP.tx
COPY resources/processed_data/DISEASE_DOID_MAP.txt /home/resources/processed_data/DISEASE_DOID_MAP.txt
COPY resources/processed_data/PHENOTYPE_HPO_MAP.txt /home/resources/processed_data/PHENOTYPE_HPO_MAP.txt
COPY resources/processed_data/ENTREZ_GENE_PRO_ONTOLOGY_MAP.txt /home/resources/processed_data/ENTREZ_GENE_PRO_ONTOLOGY_MAP.txt
COPY resources/processed_data/GENE_SYMBOL_ENSEMBL_TRANSCRIPT_MAP.txt /home/resources/processed_data/GENE_SYMBOL_ENSEMBL_TRANSCRIPT_MAP.txt
COPY resources/processed_data/ENSEMBL_GENE_ENTREZ_GENE_MAP.txt /home/resources/processed_data/ENSEMBL_GENE_ENTREZ_GENE_MAP.txt
COPY resources/processed_data/UNIPROT_ACCESSION_PRO_ONTOLOGY_MAP.txt /home/resources/processed_data/UNIPROT_ACCESSION_PRO_ONTOLOGY_MAP.txt
COPY resources/processed_data/HPA_GTEx_TISSUE_CELL_MAP.txt /home/resources/processed_data/HPA_GTEx_TISSUE_CELL_MAP.txt
COPY resources/processed_data/STRING_PRO_ONTOLOGY_MAP.txt /home/resources/processed_data/STRING_PRO_ONTOLOGY_MAP.txt

# change permissions
RUN chmod -R 755 /home/resources
RUN chmod 755 /home/Main.py

ENTRYPOINT ["python",  "./home/Main.py", "--onts", "/home/resources/ontology_source_list.txt", "--edg", "/home/resources/edge_source_list.txt", "--res", "/home/resources/resource_info.txt", "--out", "/home/resources/knowledge_graphs"]
CMD ["-h"]
