***
## Creating Instance Data Node Metadata  
***
***

**Wiki Page:** **[`Dependencies`](https://github.com/callahantiff/PheKnowLator/wiki/Dependencies#node-metadata)**  
**Jupyter Notebook:** **[`Data_Preparation.ipynb`](https://github.com/callahantiff/PheKnowLator/blob/master/Data_Preparation.ipynb)**  

___

### Purpose
The knowledge graph can be built with or without the inclusion of instance node metadata (i.e. labels, descriptions or definitions, and, synonyms). If you's like to create and use node metadata, please see the Jupyter Notebook referenced above and run the code chunks listed under the **Gather Node Metadata Data** section. These code chunks should only be run once the edge lists have been created, but before the knowledge graph is constructed.

<br>

##### 🛑 ASSUMPTIONS 🛑  
**The algorithm makes the following assumptions:**
- If metadata is provided, only those edges with nodes that have metadata will be created. All valid edges without meatdata     will be discarded.
- All metadata data sets should be written to `./resources/node_data`  
  - There will be one file per edge type and is labeled such that the edge type occurs at the beginning of the file
   (e.g. `chemical-gene_metadata.txt`).
- Each metadata file, in addition to containing the primary node identifier (labeled as `ID`), will contain 1 to 3
 additional columns labeled: `Label`, `Description`, and `Synonym`. An example of these data types is shown
  below for the Ensembl transcript identifier `ENST00000000412`:  

| **Metadata Type** | **Definition** | **Metadata**  | 
| :---: | :--- | :--- | 
| ID | Node identifiers for instance data sources | `ENST00000000412` |
| Label | The primary label or name for the node | `M6PR` |       
| Description | A definition or other useful details about the node | This transcript was transcribed from `Mannose-6-Phosphate Receptor, Cation Dependent`, a `protein-coding gene` that is located on chromosome `12` (map_location: `12p13.31`). |        
| Synonym | Alternative terms used for a node | `CD-M6PR, CD-MPR, MPR 46, MPR-46, MPR46, SMPR, cation-dependent mannose-6-phosphate receptor, 46-kDa mannose 6-phosphate receptor, CD Man-6-P receptor, Mr 46,000 Man6PR, small mannose 6-phosphate receptor` |           

<br>

#### Metadata + PheKnowLator
The metadata will be used to create the following edges in the knowledge graph:  
- **Label** ➞ node `rdfs:label`  
- **Description** ➞ node `obo:IAO_0000115` description 
- **Synonyms** ➞ node `oboInOwl:hasExactSynonym` synonym 
