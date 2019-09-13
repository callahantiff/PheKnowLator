cls_data_files = {'chemical-disease': './resources/edge_data/chemical_disease_ctd_class_data.txt',
                      'chemical-gene': './resources/edge_data/chemical-gene_ctd_class_data.txt',
                      'chemical-go': './resources/edge_data/chemical-go_ctd_class_data.txt',
                      'chemical-pathway': './resources/edge_data/chemical-pathway_reactome_class_data.txt',
                      'disease-gobp': './resources/edge_data/disease-gobp_ctd_class_data.txt',
                      'disease-gocc': './resources/edge_data/disease-gocc_ctd_class_data.txt',
                      'disease-gomf': './resources/edge_data/disease-gomf_ctd_class_data.txt',
                      'disease-phenotype': './resources/edge_data/disease-phenotype_hp_class_data.txt',
                      'gene-gobp': './resources/edge_data/gene-go_goa_class_data.txt',
                      'gene-gomf': './resources/edge_data/gene-go_goa_class_data.txt',
                      'gene-gocc': './resources/edge_data/gene-go_goa_class_data.txt',
                      'gene-phenotype': './resources/edge_data/gene-phenotype_hp_class_data.txt',
                      'pathway-disease': './resources/edge_data/pathway-disease_reactome_instance_data.txt',
                      'pathway-gobp': './resources/edge_data/pathway-go_reactome_class_data.txt',
                      'pathway-gomf': './resources/edge_data/pathway-go_reactome_class_data.txt',
                      'pathway-gocc': './resources/edge_data/pathway-go_reactome_class_data.txt'}

    inst_data_files = {'gene-gene': './resources/edge_data/gene-gene_string_instance_data.txt',
                       'gene-pathway': './resources/edge_data/gene-pathway_reactome_instance_data.txt'}

    ontologies = {'disease': './resources/ontologies/doid_with_imports.owl',
                  'phenotype': './resources/ontologies/hp_with_imports.owl',
                  'go': './resources/ontologies/go_with_imports.owl',
                  'vaccine': 'http://purl.obolibrary.org/obo/vo.owl',
                  'chemical': './resources/ontologies/chebi_lite.owl'}

    combined_edges = dict(dict(cls_data_files, **inst_data_files), **ontologies)