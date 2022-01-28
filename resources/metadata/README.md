***
## Preparing Node and Entity Metadata  
***
***

**Wiki Page:** **[`Dependencies`](https://github.com/callahantiff/PheKnowLator/wiki/Dependencies#node-metadata)**  
**Jupyter Notebook:** **[`Data_Preparation.ipynb`](https://github.com/callahantiff/PheKnowLator/blob/master/notebooks/Data_Preparation.ipynb)**  

**Generated Output:** `./resources/metadata/entity_metadata_dict.pkl`  

___

A variety of <u>metadata</u> are pulled from the data sources that are used to support external edges added to 
enhance the core set of ontologies. For the monthly PheKnowLator builds, please see [`pheknowlator_source_metadata.
xlsx`](https://github.com/callahantiff/PheKnowLator/blob/master/resources/metadata/pheknowlator_source_metadata.xlsx) 
spreadsheet. This spreadsheet has two tabs, one for nodes and one for edges. Each entity (i.e., node or relation) there are several columns, including descriptions of the metadata, the variable type, and even examples of values for each type of metadata.  

*Example Metadata Dictionary Output*. The code snippet below is meant to provide a snapshot of how data are organized in the metadata dictionary. As demonstrated by this example, there are three high-level keys:  
  - `nodes`: Nodes are keyed by CURIE. Every node has a `Label`, `Description`, `Synonym`, and `Dbxref` (whenever possible). Metadata that are obtained from specific sources that are not ontologies are added as a nested dictionary keyed by the filename.   
  - `edges`: Edges are keyed by a label which represents the edge type (the same label that is used in `resource_info.txt` and `edge_source_list.txt` files). Metadata that are obtained from specific sources that are not ontologies are added as a nested dictionary keyed by the filename.    
  - `relations`: Relations or `owl:ObjectProperty` objects are keyed by CURIE. Similar to nodes, every relation has a `Label`, `Description`, and `Synonym` (whenever possible). Metadata that are obtained from specific sources that are not ontologies are added as a nested dictionary keyed by the filename.     

```python
{
    'nodes': {
        'NCBIGene_2052': {
            'Label': 'EPHX1',
            'Description': "EPHX1 has locus group 'protein-coding' and is located on chromosome 1 (1q42.12).",
            'Synonym': 'epoxide hydrolase 1, microsomal (xenobiotic)|epoxide hydratase|EPHX|HYL1|MEHepoxide hydrolase 1|epoxide hydrolase 1 microsomal|EPOX',
            'Dbxref': 'MIM:132810|HGNC:HGNC:3401|Ensembl:ENSG00000143819', ... },
        'CHEBI_4592': {
            'Label': 'Dihydroxycarbazepine',
            'Description': "None",
            'Synonym': '10,11-Dihydro-10,11-dihydroxy-5H-dibenzazepine-5-carboxamide|10,11-Dihydroxycarbamazepine',
            'Dbxref': 'CAS:35079-97-1|KEGG:C07495',
            'CTD_chem_gene_ixns.tsv.gz': {  
                'CTD_ChemicalID': {'MESH:C004822'},
                'CTD_CasRN': {'35079-97-1'},
                'CTD_ChemicalName': {'10,11-dihydro-10,11-dihydroxy-5H-dibenzazepine-5-carboxamide'}}, ... }, ... },
    'edges': {
        'chemical-gene': {
            'CHEBI_4592-NCBIGene_2052': {
                {'CTD_chem_gene_ixns.tsv': {
                    'CTD_Evidence': [{'CTD_Interaction': '[EPHX1 gene SNP affects the metabolism of carbamazepine epoxide] which affects the chemical synthesis of 10,11-dihydro-10,11-dihydroxy-5H-dibenzazepine-5-carboxamide',
                     'CTD_InteractionActions': 'affects^chemical synthesis|affects^metabolic processing',
                     'CTD_PubMedIDs': '15692831'}]}}, ... }, ... }, ... }, 
    'relations': {
        'RO_0002434': {
            'Label': 'interacts with',
            'Description': 'A relationship that holds between two entities in which the processes executed by the two entities are causally connected.',
            'Synonym': 'in pairwise interaction with'}, ... }
}
```

<br>

**Purpose:** 
The knowledge graph can be built with or without the inclusion of node and relation metadata (i.e. 
labels, descriptions or definitions, and synonyms). If you'd like to create and use node metadata, please run the 
[`Data_Preparation.ipynb`](https://github.com/callahantiff/PheKnowLator/blob/master/notebooks/Data_Preparation.ipynb)
Jupyter Notebook and run the code chunks listed under the **NODE AND RELATION METADATA** section. These code chunks 
should be run before the knowledge graph is constructed. For more details on what these data sources are and how 
they are created, please see the `metatadata` [`README.md`](https://github.com/callahantiff/PheKnowLator/blob/master/resources/metadata/README.md).

<br>

🛑 *<b>CONSTRAINTS</b>* 🛑  
The algorithm makes the following assumptions:
- If metadata is provided, only those edges with nodes that have metadata will be created; valid edges without metadata will be discarded.  
- Metadata will be divided into `nodes`, `relations`, and `edges`. For `nodes` and `relations`, entities will be 
  keyed by CURIE. For `edges`, entities will be keyed by their edge type (i.e., the same label that is used in 
  `resource_info.txt` and `edge_source_list.txt` files).
- For each `node` and `node` entity identifier we try to obtain at least the following metadata: `Label`, 
  `Description`, and `Synonym`.  
- Metadata that are obtained from specific sources that are not ontologies will be added as a nested dictionary that is 
  keyed by the filename.  
