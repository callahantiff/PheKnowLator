***
## Creating Node and Relation Metadata  
***
***

**Wiki Page:** **[`Dependencies`](https://github.com/callahantiff/PheKnowLator/wiki/Dependencies#node-metadata)**  
**Jupyter Notebook:** **[`Data_Preparation.ipynb`](https://github.com/callahantiff/PheKnowLator/blob/master/notebooks/Data_Preparation.ipynb)**  

___

**Purpose:** The knowledge graph can be built with or without the inclusion of node and relation metadata (i.e. labels, descriptions or definitions, and synonyms). If you'd like to create and use node metadata, please see the [`Data_Preparation.ipynb`](https://github.com/callahantiff/PheKnowLator/blob/master/notebooks/Data_Preparation.ipynb) Jupyter Notebook and run the code chunks listed under the **NODE AND RELATION METADATA** section. These code chunks should be run before the knowledge graph is constructed. For more details on what these data sources are and how they are created, please see the `node_data` [`README.md`](https://github.com/callahantiff/PheKnowLator/blob/master/resources/node_data/README.md).

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
- Metadata for all non-ontology nodes and all relations for edges added to the core set of ontologies will be saved 
  as a dictionary in the `./resources/metadata/entity_metadata_dict.pkl` repository.

<br>

#### Metadata + PheKnowLator
***  
A variety of metadata is pulled from the data sources that are used to support external edges added to enhance the core set of ontologies. For the monthly PheknowLator builds, please see [`pheknowlator_source_metadata.xlsx`](https://github.com/callahantiff/PheKnowLator/blob/master/resources/pheknowlator_source_metadata.xlsx) spreadsheet. This spreadsheet has two tabs, one for nodes and one for edges. For each entity (i.e., node or edge) there are several columns, including descriptions of the metadata, the variable type, and even examples of values for eah type of metadata. 
