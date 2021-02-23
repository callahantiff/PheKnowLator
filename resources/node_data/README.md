***
## Creating Instance Data Node Metadata  
***
***

**Wiki Page:** **[`Dependencies`](https://github.com/callahantiff/PheKnowLator/wiki/Dependencies#node-metadata)**  
**Jupyter Notebook:** **[`Data_Preparation.ipynb`](https://github.com/callahantiff/PheKnowLator/blob/master/notebooks/Data_Preparation.ipynb)**  

___

**Purpose:** The knowledge graph can be built with or without the inclusion of node and relation metadata (i.e. 
labels, descriptions or definitions, and synonyms). If you'd like to create and use node metadata, please run the 
[`Data_Preparation.ipynb`](https://github.com/callahantiff/PheKnowLator/blob/master/notebooks/Data_Preparation.ipynb) Jupyter Notebook and run the code chunks listed under the **INSTANCE AND/OR SUBCLASS (NON-ONTOLOGY CLASS) METADATA** section. These code chunks should be run before the knowledge graph is constructed. For more details on what these data sources are and how they are created, please see the `node_data` [`README.md`](https://github.com/callahantiff/PheKnowLator/blob/master/resources/node_data/README.md).

Example structure of the metadata dictionary is shown below:

```python
{
    'nodes': {
        'http://www.ncbi.nlm.nih.gov/gene/1': {
            'Label': 'A1BG',
            'Description': "A1BG has locus group protein-coding' and is located on chromosome 19 (19q13.43).",
            'Synonym': 'HYST2477alpha-1B-glycoprotein|HEL-S-163pA|ABG|A1B|GAB'} ... },
    'relations': {
        'http://purl.obolibrary.org/obo/RO_0002533': {
            'Label': 'sequence atomic unit',
            'Description': 'Any individual unit of a collection of like units arranged in a linear order',
            'Synonym': 'None'} ... }
} 
```  

<br>

🛑 *<b>CONSTRAINTS</b>* 🛑  
The algorithm makes the following assumptions:
- If metadata is provided, only those edges with nodes that have metadata will be created; valid edges without metadata will be discarded.  
- Metadata for all non-ontology nodes and all relations for edges added to the core set of ontologies will be saved as a dictionary in the `./resources/node_data/node_metadata_dict.pkl` repository.
- For each identifier we try to obtain the following metadata: `Label`, `Description`, and `Synonym`. An example of these data types is shown below for a [`gene`](https://github.com/callahantiff/PheKnowLator/wiki/v2-Data-Sources#ncbi-gene) identifier `5620`:  

| **Metadata Type** | **Definition** | **Metadata**  | 
| :---: | :--- | :--- | 
| ID | Node identifiers for instance data sources | `5620` |
| Label | The primary label or name for the node | `LANCL2` |       
| Description | A definition or other useful details about the node | `Lanc Like 2` is a `protein-coding` gene that is located on chromosome `7` (map_location: `7p11.2`) |        
| Synonym | Alternative terms used for a node | `GPR69B`, `TASP`, `lanC-like protein 2`, `G protein-coupled receptor 69B`, `LanC (bacterial lantibiotic synthetase component C)-like 2`, `LanC lantibiotic synthetase component C-like 2`, `testis-specific adriamycin sensitivity protein` |

<br>

#### Metadata + PheKnowLator
***  
The metadata will be used to create the following edges in the knowledge graph:  
- **Label** ➞ node `rdfs:label`  
- **Description** ➞ node `obo:IAO_0000115` description 
- **Synonyms** ➞ node `oboInOwl:hasExactSynonym` synonym 
