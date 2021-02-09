***
## Knowledge Graph Construction Approaches    
***
***

**Wiki Page:** **[`KG-Construction`](https://github.com/callahantiff/PheKnowLator/wiki/KG-Construction)**  
**Jupyter Notebook:** [`Data_Preparation.ipynb`](https://github.com/callahantiff/PheKnowLator/blob/master/Data_Preparation.ipynb)  
**Required Input:** `resources/construction_approach/subclass_construction_map.pkl`

____

### Purpose
New data can be added to the knowledge graph using 2 different construction approaches: (1) `instance-based` or (2) `subclass-based`. Each of these approaches is described further below.

<br>

_____

### Instance-Based Construction    

In this approach, each new edge is added as an `instance` of an existing class (via `rdf:Type`) in the knowledge graph.  
  
EXAMPLE: Adding the edge: `Morphine` ➞ `isSubstanceThatTreats` ➞ `Migraine`
- `isSubstanceThatTreats`(`Morphine`, `x1`)
- `Type`(`x1`, `Migraine`)

In this example, `Morphine` is an ontology data node from [ChEBI](https://www.ebi.ac.uk/chebi) and `Migraine` is a [Human Phenotype Ontology](https://hpo.jax.org/) term. This would result in the following triples:  

```
UUID1 = MD5(Morphine + isSubstanceThatTreats + Migraine + "subject")
UUID2 = MD5(Morphine + isSubstanceThatTreats + Migraine + "object")

UUID1, rdf:type, Morphine
UUID1, rdf:type, owl:NamedIndividual

UUID2, rdf:type, Migraine
UUID2, rdf:type, owl:NamedIndividual

UUID1, isSubstanceThatTreats, UUID2
``` 

A table is provided below showing the different triples that are added as function of edge type (i.e. `class`-`class` vs. `class`-`instance` vs. `instance`-`instance`) and relation strategy (i.e. relations only or relations + inverse relations). 

Edge Type | Relations | Needed Triples  
:--: | :--: | :--  
Class-Class | Relations Only | **`GO_1234567`, `REL`, `DOID_1234567`**<br><br>`UUID1` = `pkt` + MD5(`GO_1234567` + `<<REL>>` + `DOID_1234567` + "subject")<br>`UUID2` = `pkt` + MD5(`GO_1234567` + `<<REL>>` + `DOID_1234567` + "object")<br><hr>`UUID1`, `rdf:type`, `GO_1234567`<br> `UUID1`, `rdf:type`, `owl:NamedIndividual`<br><br> `UUID2`, `rdf:type`, `DOID_1234567`<br> `UUID2`, `rdf:type`, `owl:NamedIndividual`<br><br> `UUID1`, `REL`, `UUID2` 
  Class-Class | Relations + Inverse Relations | **`GO_1234567`, `REL`, `DOID_1234567`<br>`DOID_1234567`, `INV_REL`, `GO_1234567`**<br><br>`UUID1` = `pkt` + MD5(`GO_1234567` + `<<REL>>` + `DOID_1234567` + "subject")<br>`UUID2` = `pkt` + MD5(`GO_1234567` + `<<REL>>` + `DOID_1234567` + "object")<br><hr>`UUID1`, `rdf:type`, `GO_1234567`<br>`UUID1`, `rdf:type`, `owl:NamedIndividual`<br><br>`UUID2`, `rdf:type`, `DOID_1234567`<br>`UUID2`, `rdf:type`, `owl:NamedIndividual`<br><br>`UUID1`, `REL`, `UUID2`<br>`UUID2`, `INV_REL`, `UUID1`  
  Class-Instance | Relations Only | **`GO_1234567`, `REL`, `HGNC_1234567`**<br><br>`UUID1` = `pkt` + MD5(`GO_1234567` + `<<REL>>` + `HGNC_1234567` + "subject")<br>`UUID2` = `pkt` + MD5(`GO_1234567` + `<<REL>>` + `HGNC_1234567` + "object")<br><hr>`UUID1`, `rdf:type`, `GO_1234567`<br>`UUID1`, `rdf:type`, `owl:NamedIndividual`<br><br>`HGNC_1234567`, `rdfs:subClassOf`, `subclass_dict[HGNC_1234567]`<br>`HGNC_1234567`, `rdf:type`, `owl:Class`<br>`UUID2`, `rdf:type`, `HGNC_1234567`<br>`UUID2`, `rdf:type`, `owl:NamedIndividual`<br><br>`UUID1`, `REL`, `UUID2`  
Class-Instance | Relations + Inverse Relations | **`GO_1234567`, `REL`, `HGNC_1234567`<br>`HGNC_1234567`, `INV_REL`, `GO_1234567`**<br><br>`UUID1` = `pkt` + MD5(`GO_1234567` + `<<REL>>` + `HGNC_1234567` + "subject")<br>`UUID2` = `pkt` + MD5(`GO_1234567` + `<<REL>>` + `HGNC_1234567` + "object")<br><hr>`UUID1`, `rdf:type`, `GO_1234567`<br>`UUID1`, `rdf:type`, `owl:NamedIndividual`<br><br>`HGNC_1234567`, `rdfs:subClassOf`, `subclass_dict[HGNC_1234567]`<br>`HGNC_1234567`, `rdf:type`, `owl:Class`<br>`UUID2`, `rdf:type`, `HGNC_1234567`<br>`UUID2`, `rdf:type`, `owl:NamedIndividual`<br><br>`UUID1`, `REL`, `UUID2`<br>`UUID2`, `INV_REL`, `UUID1`  
Instance-Instance | Relations Only | **`HGNC_1234567`, `REL`, `HGNC_7654321`**<br><br>`UUID1` = `pkt` + MD5(`HGNC_1234567` + `<<REL>>` + `HGNC_7654321` + "subject")<br>`UUID2` = `pkt` + MD5(`HGNC_1234567` + `<<REL>>` + `HGNC_7654321` + "object")<br><hr>`HGNC_1234567`, `rdfs:subClassOf`,`subclass_dict[HGNC_1234567]`<br>`HGNC_1234567`, `rdf:type`, `owl:Class`<br>`UUID1`, `rdf:type`, `HGNC_1234567`<br>`UUID1`, `rdf:type`, `owl:NamedIndividual`<br><br>`HGNC_7654321`, `rdf:type`,`subclass_dict[HGNC_7654321]`<br>`HGNC_7654321`, `rdfs:subClassOf`, `owl:Class`<br>`UUID2`, `rdf:type`, `HGNC_7654321`<br>`UUID2`, `rdf:type`, `owl:NamedIndividual`<br><br>`UUID1`, `REL`, `UUID2`  
Instance-Instance | Relations + Inverse Relations | **`HGNC_1234567`, `REL`, `HGNC_7654321`<br>`HGNC_7654321`, `INV_REL`, `HGNC_1234567`**<br><br>`UUID1` = `pkt` + MD5(`HGNC_1234567` + `<<REL>>` + `HGNC_7654321` + "subject")<br>`UUID2` = `pkt` + MD5(`HGNC_1234567` + `<<REL>>` + `HGNC_7654321` + "object")<br><hr>`HGNC_1234567`, `rdfs:subClassOf`,`subclass_dict[HGNC_1234567]`<br>`HGNC_1234567`, `rdf:type`, `owl:Class`<br>`UUID1`, `rdf:type`, `HGNC_1234567`<br>`UUID1`, `rdf:type`, `owl:NamedIndividual`<br><br>`HGNC_7654321`, `rdf:type`,`subclass_dict[HGNC_7654321]`<br>`HGNC_7654321`, `rdfs:subClassOf`, `owl:Class`<br>`UUID2`, `rdf:type`,`HGNC_7654321`<br>`UUID2`, `rdf:type`,`owl:NamedIndividual`<br><br>`UUID1`, `REL`, `UUID2`<br>`UUID2`, `INV_REL`, `UUID1`

<br>

*Note.* `UUID` is a `BNode` that is created from an md5 hash of concatenated URIs. The URIs used in the hash string 
includes the subject and object URIs (each appended with "subject" and "object", respectfully) in addition to a 
relation (`<<REL>>`). To account for future use cases, we have devised a heuristic to determine what is used for 
`<<REL>>`: (1) for a given relation, determine if it has an inverse (via `owl:InverseOf`); (2) sort the relations 
alphabetically; and (3) select the first relation. The selected relation is then used for creating both `UUID` 
`BNodes` (i.e. `UUID1` and `UUID2` in all examples in the table above). For example, if the relations were `causes` 
and `caused_by` both `UUID` `BNodes` would be created using `caused_by`. Please note that all `UUID` `BNodes` 
created during the construction process are explicitly defined within the `pkt` namespace (`https://github.
com/callahantiff/PheKnowLator/pkt/`).

<br><br>

_____

### Subclass-Based Construction   

In this approach, each new edge is added as a subclass of an existing ontology class (via `rdfs:subClassOf`) in the knowledge graph.

EXAMPLE: Adding the edge: `TGFB1` ➞ `participatesIn` ➞ `Influenza Virus Induced Apoptosis`
- `participatesIn`(`TGFB1`, `Influenza Virus Induced Apoptosis`)
- `subClassOf`(`Influenza Virus Induced Apoptosis`, `Influenza A Pathway`)   
- `Type`(`Influenza Virus Induced Apoptosis`, `owl:Class`)  

Where `TGFB1` is a [Protein Ontology](https://proconsortium.org/) term and `Influenza Virus Induced Apoptosis` is a non-ontology data node from [Reactome](https://reactome.org/). In this example, `Influenza A Pathway` is an existing [Pathway Ontology](http://rgd.mcw.edu/rgdweb/ontology/search.html) class. This would result in the following triples:  

```
UUID1 = MD5(TGFB1 + participatesIn + Influenza Virus Induced Apoptosis)
UUID2 = MD5(TGFB1 + participatesIn + Influenza Virus Induced Apoptosis + owl:Restriction)

Influenza Virus Induced Apoptosis, rdfs:subClassOf, Influenza A Pathway
Influenza Virus Induced Apoptosis, rdf:type, owl:Class

UUID1, rdfs:subClassOf, TGFB1
UUID1, rdfs:subClassOf, UUID2
UUID2, rdf:type, owl:Restriction
UUID2, owl:someValuesFrom, Influenza Virus Induced Apoptosis
UUID2, owl:onProperty, participatesIn
```  

A table is provided below showing the different triples that are added as function of edge type (i.e. `class`-`class` vs. `class`-`instance` vs. `instance`-`instance`) and relation strategy (i.e. relations only or relations + inverse relations).  

Edge Type | Relations | Needed Triples  
:--: | :--: | :--  
Class-Class | Relations Only | **`GO_1234567`, `REL`, `DOID_1234567`**<br><br>`UUID1` = `pkt` + MD5(`DOID_1234567` + `REL` + `GO_1234567`)<br>`UUID2` = MD5(`DOID_1234567` + `REL` + `GO_1234567` + `owl:Restriction`)<br><hr>`UUID1`, `rdfs:subClassOf`, `GO_1234567`<br>`UUID1`, `rdfs:subClassOf`, `UUID2`<br>`UUID2`, `rdf:type`, `owl:Restriction`<br>`UUID2`, `owl:someValuesFrom`, `DOID_1234567`<br>`UUID2`, `owl:onProperty`, `REL`  
Class-Class | Relations + Inverse Relations | **`GO_1234567`, `REL`, `DOID_1234567`<br>`DOID_1234567`, `INV_REL`, `GO_1234567`**<br><br>`UUID1` = `pkt` + MD5(`GO_1234567` + `REL` + `DOID_1234567`)<br>`UUID2` = MD5(`GO_1234567` + `REL` + `DOID_1234567` + `owl:Restriction`)<br>`UUID3` = `pkt` + MD5(`DOID_1234567` + `INV_REL` + `GO_1234567`)<br>`UUID4` = MD5(`DOID_1234567` + `INV_REL` + `GO_1234567` + `owl:Restriction`)<br><hr>`UUID1`,`rdfs:subClassOf`,`GO_1234567`<br>`UUID1`,`rdfs:subClassOf`,`UUID2`<br>`UUID2`, `rdf:type`, `owl:Restriction`<br>`UUID2`, `owl:someValuesFrom`,`DOID_1234567`<br>`UUID2`, `owl:onProperty`, `REL`<br><br>`UUID3`, `rdfs:subClassOf`,`DOID_1234567`<br>`UUID3`,`rdfs:subClassOf`,`UUID4`<br>`UUID4`, `rdf:type`, `owl:Restriction`<br>`UUID4`, `owl:someValuesFrom`, `GO_1234567`<br>`UUID4`, `owl:onProperty`, `INV_REL`  
Class-Instance | Relations Only | **`GO_1234567`, `REL`, `HGNC_1234567`**<br><br>`UUID1` = `pkt` + MD5(`GO_1234567` + `REL` + `HGNC_1234567`)<br> `UUID2` = MD5(`GO_1234567` + `REL` + `HGNC_1234567` + `owl:Restriction`)<br><hr>`HGNC_1234567`, `rdfs:subClassOf`, `subclass_dict[HGNC_1234567]`<br> `HGNC_1234567`, `rdf:type`, `owl:Class`<br><br> `UUID1`, `rdfs:subClassOf`, `GO_12334567`<br> `UUID1`, `rdfs:subClassOf`, `UUID2`<br> `UUID2`, `rdf:type`, `owl:Restriction`<br> `UUID2`, `owl:someValuesFrom`, `HGNC_1234567`<br> `UUID2`, `owl:onProperty`, `REL`  
  Class-Instance | Relations + Inverse Relations | **`GO_1234567`, `REL`, `HGNC_1234567`<br>`HGNC_1234567`, `INV_REL`, `GO_1234567`**<br><br>`UUID1` = `pkt` + MD5(`GO_1234567` + `REL` + `HGNC_1234567`)<br> `UUID2` = MD5(`GO_1234567` + `REL` + `HGNC_1234567` + `owl:Restriction`)<br>`UUID3` = `pkt` + MD5(`HGNC_1234567` + `INV_REL` + `GO_1234567`)<br> `UUID4` = MD5(`HGNC_1234567` + `INV_REL` + `GO_1234567` + `owl:Restriction`)<br><hr> `HGNC_1234567`, `rdfs:subClassOf`, `subclass_dict[HGNC_1234567]`<br> `HGNC_1234567`, `rdf:type`, `owl:Class`<br><br> `UUID1`, `rdfs:subClassOf`, `GO_12334567`<br> `UUID1`, `rdfs:subClassOf`, `UUID2`<br> `UUID2`, `rdf:type`, `owl:Restriction`<br> `UUID2`, `owl:someValuesFrom`, `HGNC_1234567`<br> `UUID2`, `owl:onProperty`, `REL`<br><br> `UUID3`, `rdfs:subClassOf`, `HGNC_1234567`<br> `UUID3`, `rdfs:subClassOf`, `UUID4`<br> `UUID4`, `rdf:type`, `owl:Restriction`<br> `UUID4`, `owl:someValuesFrom`, `GO_12334567`<br> `UUID4`, `owl:onProperty`, `INV_REL`  
Instance-Instance | Relations Only | **`HGNC_1234567`, `REL`, `HGNC_7654321`**<br><br>`UUID1` = `pkt` + MD5(`HGNC_1234567` + `REL` + `HGNC_7654321`)<br> `UUID2` = MD5(`HGNC_1234567` + `REL` + `HGNC_7654321` + `owl:Restriction`)<br><hr>`HGNC_1234567`, `rdfs:subClassOf`, `subclass_dict[HGNC_1234567]`<br> `HGNC_1234567`, `rdf:type`, `owl:Class`<br><br> `HGNC_7654321`, `rdfs:subClassOf`, `subclass_dict[HGNC_7654321]`<br> `HGNC_7654321`, `rdf:type`, `owl:Class`<br><br> `UUID1`, `rdfs:subClassOf`, `HGNC_1234567`<br> `UUID1`, `rdfs:subClassOf`, `UUID2`<br> `UUID2`, `rdf:type`, `owl:Restriction`<br> `UUID2`, `owl:someValuesFrom`, `HGNC_7654321`<br> `UUID2`, `owl:onProperty`, `REL`  
Instance-Instance | Relations + Inverse Relations | **`HGNC_1234567`, `REL`, `HGNC_7654321`<br>`HGNC_7654321`, `INV_REL`, `HGNC_1234567`**<br><br>`UUID1` = `pkt` + MD5(`HGNC_1234567` + `REL` + `HGNC_7654321`)<br> `UUID2` = MD5(`HGNC_1234567` + `REL` + `HGNC_7654321` + `owl:Restriction`)<br>`UUID3` = `pkt` + MD5(`HGNC_7654321` + `INV_REL` + `HGNC_1234567`)<br> `UUID4` = MD5(`HGNC_7654321` + `INV_REL` + `HGNC_1234567` + `owl:Restriction`)<br><hr> `HGNC_1234567`, `rdfs:subClassOf`, `subclass_dict[HGNC_1234567]`<br> `HGNC_1234567`, `rdf:type`, `owl:Class`<br><br> `HGNC_7654321`, `rdfs:subClassOf`, `subclass_dict[HGNC_7654321]`<br> `HGNC_7654321`, `rdf:type`, `owl:Class`<br><br> `UUID1`, `rdfs:subClassOf`, `HGNC_1234567`<br> `UUID1`, `rdfs:subClassOf`, `UUID2`<br>`UUID2`, `rdf:type`, `owl:Restriction`<br> `UUID2`, `owl:someValuesFrom`, `HGNC_7654321`<br> `UUID2`, `owl:onProperty`, `REL`<br><br>`UUID3`, `rdfs:subClassOf`, `HGNC_7654321`<br> `UUID3`, `rdfs:subClassOf`, `UUID4`<br> `UUID4`, `rdf:type`, `owl:Restriction`<br> `UUID4`, `owl:someValuesFrom`, `HGNC_1234567`<br> `UUID4`, `owl:onProperty`, `INV_REL`

*Note.* When `UUID` is used within the subclass-based construction approach it is a `BNode` that is created from an md5 hash of concatenated URIs. See each table row for a sample of what this looks like. Please note that the primary `UUID` `BNodes` created during the construction process are explicitly defined within the `pkt` namespace 
(`https://github.com/callahantiff/PheKnowLator/pkt/`).

<br>

***

🛑 *<b>ASSUMPTIONS</b>* 🛑  
**Data:** [`subclass_construction_map.pkl`](https://storage.googleapis.com/pheknowlator/current_build/dependencies/subclass_construction_map.pkl)  

**The algorithm makes the following assumptions:**
- Make sure that you have created the non-ontology node data to ontology class mapping dictionary (described below) to the `./resources/construction_approach/*.pkl` directory.    

**Input requirements for both approaches:** A `pickled` dictionary (keys contain node identifiers (non-ontology node data) and the values are lists of ontology class identifiers) added to the `./resources/construction_approach/` directory. An example of this dictionary is shown below:  

```python
{
  'R-HSA-168277'  : ['http://purl.obolibrary.org/obo/PW_0001054',
                     'http://purl.obolibrary.org/obo/GO_0046730'],
  'R-HSA-9026286' : ['http://purl.obolibrary.org/obo/PW_000000001',
                     'http://purl.obolibrary.org/obo/GO_0019372'],
  '100129357'     : ['SO_0000043'],
  '100129358'     : ['SO_0000336'],
}                  

```

Please see the [Reactome Pathways - Pathway Ontology](https://render.githubusercontent.com/view/ipynb?commit=0dd39969d80cf99a634337c24a2f5efd8fd1a49c&enc_url=68747470733a2f2f7261772e67697468756275736572636f6e74656e742e636f6d2f63616c6c6168616e746966662f5068654b6e6f774c61746f722f306464333939363964383063663939613633343333376332346132663565666438666431613439632f446174615f5072657061726174696f6e2e6970796e62&nwo=callahantiff%2FPheKnowLator&path=Data_Preparation.ipynb&repository_id=149909076&repository_type=Repository#reactome-pw) and [Genomic Identifiers - Sequence Ontology](https://render.githubusercontent.com/view/ipynb?commit=0dd39969d80cf99a634337c24a2f5efd8fd1a49c&enc_url=68747470733a2f2f7261772e67697468756275736572636f6e74656e742e636f6d2f63616c6c6168616e746966662f5068654b6e6f774c61746f722f306464333939363964383063663939613633343333376332346132663565666438666431613439632f446174615f5072657061726174696f6e2e6970796e62&nwo=callahantiff%2FPheKnowLator&path=Data_Preparation.ipynb&repository_id=149909076&repository_type=Repository#genomic-so) sections of the [`Data_Preparation.ipynb`](https://github.com/callahantiff/PheKnowLator/blob/master/Data_Preparation.ipynb) Jupyter Notebook for examples of how to construct this document. 
