***
## Knowledge Graph Construction Approaches    
***
***

**Wiki Page:** **[`KG-Construction`](https://github.com/callahantiff/PheKnowLator/wiki/KG-Construction)**  

**Jupyter Notebook:** [`Data_Preparation.ipynb`](https://github.com/callahantiff/PheKnowLator/blob/master/Data_Preparation.ipynb)  

____

### Purpose
New data can be added to the knowledge graph using 2 different construction approaches: (1) `instance-based` or (2) `subclass-based`. Each of these approaches is described further below.

<br>

_____

### Instance-Based Construction    

<img src="https://user-images.githubusercontent.com/8030363/78377026-562ecb80-758c-11ea-8451-a12f89ba6ad2.png">

<br>

In this approach, each new edge is added as an `instance` of an existing class (via `rdf:Type`) in the knowledge graph.  
  
EXAMPLE: Adding the edge: Morphine ➞ `isSubstanceThatTreats` ➞ Migraine

Would require adding:
- `isSubstanceThatTreats`(Morphine, `x1`)
- `Type`(`x1`, Migraine)

In this example, Morphine is a non-ontology data node and Migraine is an HPO ontology term. 

<br>

_NOTE._ While the instance of the class Migraines can be treated as an anonymous node in the knowledge graph, we generate a new international resource identifier for each asserted instance.

<br>

**Input Requirements:** There are no additional input requirements for this construction approach. 

<br>

**Outputs:** As mentioned above, a UUID is created for each anonymous node representing an instance of a class. In order to fully utilize the knowledge graph, a `.json` file containing the mapping from each UUID instance to it's ontology class is output to the `./resources/construction_approach/instance` directory. For example, 

```json
{
"http://purl.obolibrary.org/obo/CHEBI_24505": "https://github.com/callahantiff/PheKnowLator/obo/ext/c2591241-8952-44ea-a313-e4b3c5fb6d35",
"http://purl.obolibrary.org/obo/PR_000013648": "https://github.com/callahantiff/PheKnowLator/obo/ext/0ea74deb-0002-4f48-b7e4-81a8fd947312",
"http://purl.obolibrary.org/obo/GO_0050031": "https://github.com/callahantiff/PheKnowLator/obo/ext/8f5c81d4-92dd-426e-a2d9-2be87edb1520",
}
```

<br><br>

_____

### Subclass-Based Construction   

<img src="https://user-images.githubusercontent.com/8030363/78376386-79a54680-758b-11ea-8a8d-58b91b57fed2.png">

<br>


In this approach, each new edge is added as a subclass of aan existing ontology class (via `rdfs:subClassOf`) in the knowledge graph.

EXAMPLE: Adding the edge: TGFB1 ➞ `participatesIn` ➞ Influenza Virus Induced Apoptosis

Would require adding:
- `participatesIn`(TGFB1, Influenza Virus Induced Apoptosis)
- `subClassOf`(Influenza Virus Induced Apoptosis, Influenza A pathway)   
- `Type`(Influenza Virus Induced Apoptosis, `owl:Class`)  

Where TGFB1 is an PR ontology term and Influenza Virus Induced Apoptosis is a non-ontology data node. In this example, Influenza A pathway is an existing ontology class.

<br>

**Input Requirements:** A `pickled` dictionary where the keys are node identifiers (ono-ontology node data) and the values are lists of ontology class identifiers to subclass has been added to the `./resources/construction_approach/subclass/` directory. An example of this dictionary is shown below:  

```python
{
  'R-HSA-168277'  : ['http://purl.obolibrary.org/obo/PW_0001054',         'http://purl.obolibrary.org/obo/GO_0046730'],
  'R-HSA-9026286' : ['http://purl.obolibrary.org/obo/PW_000000001',         'http://purl.obolibrary.org/obo/GO_0019372'],
  '100129357'     : ['SO_0000043'],
  '100129358'     : ['SO_0000336'],
}                  

```

Please see the [Reactome Pathways - Pathway Ontology](https://render.githubusercontent.com/view/ipynb?commit=0dd39969d80cf99a634337c24a2f5efd8fd1a49c&enc_url=68747470733a2f2f7261772e67697468756275736572636f6e74656e742e636f6d2f63616c6c6168616e746966662f5068654b6e6f774c61746f722f306464333939363964383063663939613633343333376332346132663565666438666431613439632f446174615f5072657061726174696f6e2e6970796e62&nwo=callahantiff%2FPheKnowLator&path=Data_Preparation.ipynb&repository_id=149909076&repository_type=Repository#reactome-pw) and [Genomic Identifiers - Sequence Ontology](https://render.githubusercontent.com/view/ipynb?commit=0dd39969d80cf99a634337c24a2f5efd8fd1a49c&enc_url=68747470733a2f2f7261772e67697468756275736572636f6e74656e742e636f6d2f63616c6c6168616e746966662f5068654b6e6f774c61746f722f306464333939363964383063663939613633343333376332346132663565666438666431613439632f446174615f5072657061726174696f6e2e6970796e62&nwo=callahantiff%2FPheKnowLator&path=Data_Preparation.ipynb&repository_id=149909076&repository_type=Repository#genomic-so) sections of the [`Data_Preparation.ipynb`](https://github.com/callahantiff/PheKnowLator/blob/master/Data_Preparation.ipynb) Jupyter Notebook for examples of how to consruct this document. 

<br>

**Outputs:** There are no approach-specific output files generated.  

<br>  

_____

🛑 *<b>ASSUMPTIONS</b>* 🛑  
**The algorithm makes the following assumptions:**
- If using the `subclass-based` construction approach, make sure that you have created the non-ontology node data to ontoogy class mapping dictionary (described in the [](#) section above) to the `./resources/construction_approach/subclass/` directory.     
