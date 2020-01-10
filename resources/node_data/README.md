***
## Creating Instance Data Node Metadata  
***
***

**Wiki Page:** **[`Dependencies`](https://github.com/callahantiff/PheKnowLator/wiki/Dependencies)**  
**Jupyter Notebook:** **[`Data_Preparation.ipynb`](https://github.com/callahantiff/PheKnowLator/blob/master/Data_Preparation.ipynb)**  

___

### Purpose
This repository is a required component for building the PheKnowLator Knowledge graph. It should contain a `tab
` delimited data set for each instance data node that you would like to include metadata for.

<br>

##### 🛑 ASSUMPTIONS 🛑  
The algorithm makes the following assumptions:
- All metadata data sets should be written to `./resources/node_data`
- Each metadata file, in addition to containing the primary node identifier (labeled as `ID`), will contain 1 to 4
 additional columns labeled: `Label`, `Description`, `Synonym`, and `DbXref`. An example of these data types is shown
  below for the Ensembl transcript identifier `ENST00000000412`:  

| **Metadata Type** | **Definition** | **Metadata**  | 
| :---: | :--- | :--- | 
| ID | Node identifiers for instance data sources | `ENST00000000412` |
| Label | The primary label or name for the node | `M6PR` |       
| Description | A definition or other useful details about the node | This transcript was transcribed from `Mannose-6-Phosphate Receptor, Cation Dependent`, a `protein-coding gene` that is located on chromosome `12` (map_location: `12p13.31`). |        
| Synonym | Alternative terms used for a node | `CD-M6PR, CD-MPR, MPR 46, MPR-46, MPR46, SMPR, cation-dependent mannose-6-phosphate receptor, 46-kDa mannose 6-phosphate receptor, CD Man-6-P receptor, Mr 46,000 Man6PR, small mannose 6-phosphate receptor` |        
| DbXref | Mapped identifiers used when constructing the knowledge graph | `4074` |    

<br>

#### Metadata + PheKnowLator
The metadata will be used to create the following edges in the knowledge graph:  
- **Label** ➞ node `rdfs:label`  
- **Description** ➞ node `obo:IAO_0000115` description 
- **Synonyms** ➞ node `oboInOwl:hasExactSynonym` synonym 
- **DbXref** ➞ node `oboInOwl:hasDbXref` DbXref  