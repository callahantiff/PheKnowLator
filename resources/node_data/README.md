***
## Creating Instance Data Node Metadata  
***
***

**Wiki Page:** **[`Dependencies`](https://github.com/callahantiff/PheKnowLator/wiki/Dependencies#node-metadata)**  
**Jupyter Notebook:** **[`Data_Preparation.ipynb`](https://github.com/callahantiff/PheKnowLator/blob/master/Data_Preparation.ipynb)**  

___

### Purpose
The knowledge graph is built without inclusion of instance node metadata (i.e. labels, descriptions or definitions, and, synonyms). If you'd like to create node metadata, please see the Jupyter Notebook referenced above and run the code chunks listed under the **Gather Node Metadata Data** section. These code chunks should only be run once the edge lists have been created, but before the knowledge graph is constructed.

<br>

🛑 *<b>ASSUMPTIONS</b>* 🛑  
**The algorithm makes the following assumptions:**
- Metadata is used to create a `.txt` document that is output with the knowledge graph. Metadata is not added to the knowledge graph without running the steps described below.  
- To add metadata to the knowledge graph, set the `add_meatadata_to_kg` flag to `yes` when running building the knowledge graph.  
- All metadata data sets should be written to `./resources/node_data`.  
  - There will be one file per edge type and is labeled such that the edge type occurs at the beginning of the file
   (e.g. `chemical-gene_metadata.txt`).
- Each metadata file, in addition to containing the primary node identifier (labeled as `ID`), will contain 1 to 3
 additional columns labeled: `Label`, `Description`, and `Synonym`. An example of these data types is shown
  below for the Ensembl transcript identifier `ENST00000000412`:  

<br>

| **Metadata Type** | **Definition** | **Metadata**  | 
| :---: | :--- | :--- | 
| ID | Node identifiers for instance data sources | `5620` |
| Label | The primary label or name for the node | `LANCL2` |       
| Description | A definition or other useful details about the node | `Lanc Like 2` is a `protein-coding` gene that is located on chromosome `7` (map_location: `7p11.2`) |        
| Synonym | Alternative terms used for a node | `GPR69B`, `TASP`, `lanC-like protein 2`, `G protein-coupled receptor 69B`, `LanC (bacterial lantibiotic synthetase component C)-like 2`, `LanC lantibiotic synthetase component C-like 2`, `testis-specific adriamycin sensitivity protein` |          

<br>

#### Metadata + PheKnowLator
The metadata will be used to create the following edges in the knowledge graph:  
- **Label** ➞ node `rdfs:label`  
- **Description** ➞ node `obo:IAO_0000115` description 
- **Synonyms** ➞ node `oboInOwl:hasExactSynonym` synonym 
