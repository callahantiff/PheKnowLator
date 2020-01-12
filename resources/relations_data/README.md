***
## Knowledge Graph Relations  
***
***

**Wiki Page:** **[`Dependencies`](https://github.com/callahantiff/PheKnowLator/wiki/Dependencies)**  
**Jupyter Notebook:** **[`Data_Preparation.ipynb`](https://github.com/callahantiff/PheKnowLator/blob/master/Data_Preparation.ipynb)**  

___

### Purpose
PheKnowLator can be built using a single set of provided relations (i.e. the `owl:ObjectProperty` or edge which is used to connect the nodes in the graph) with or without the inclusion of each relation's inverse.

<br>

🛑 *<b>CONSTRAINTS</b>* 🛑   
If you would like the knowledge graph to include relations and their inverse relations, you must add the following to
 the `./resources/relations_data` repository (an example of what should be included in each of these is included  below):    
- A `.txt` file of all relations and their labels     
- A `.txt` file of the relations and their inverse relations    

<br>

___ 

##### Relations and Inverse Relations Data 

**Filename:** `INVERSE_RELATIONS.txt`

<br>

The `owl:inverseOf` property is used to identify each relation's inverse. To make it easier to look up the inverse relations when building the knowledge graph, each relation/inverse relation pair is listed twice, for example:  
- [location of](http://www.ontobee.org/ontology/RO?iri=http://purl.obolibrary.org/obo/RO_0001015) `owl:inverseOf` [located in](http://www.ontobee.org/ontology/RO?iri=http://purl.obolibrary.org/obo/RO_0001025)  
- [located in](http://www.ontobee.org/ontology/RO?iri=http://purl.obolibrary.org/obo/RO_0001025) `owl:inverseOf` [location of](http://www.ontobee.org/ontology/RO?iri=http://purl.obolibrary.org/obo/RO_0001015) 

The data in this file should look like:     
```text
  RO_0003000  RO_0003001
  RO_0003001  RO_0003000
  RO_0002233  RO_0002352
  RO_0002352  RO_0002233
```

<br> 

___

##### Relations and Labels  

**Filename:** `RELATIONS_LABELS.txt`

<br>

Not all relations have an inverse (e.g. interactions). Even though there might not be an inverse relations, we still want to ensure that all interactions relations are symmetrically represented in the graph. To aid in this process, we need to be able to quickly look-up an edge and determine if it is an interaction. To help make this process more efficient, the algorithm expects a list of all relations and their labels in as a `.txt` file.  
 
The data in this file should look like:     
```text
  RO_0002285  developmentally replaces
  RO_0002287  part of developmental precursor of
  RO_0002490  existence overlaps
  RO_0002214  has prototype
```