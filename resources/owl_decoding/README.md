***
## Decoding OWL Semantics    
***
***

**Wiki Page:** **[`OWL-NETS-2.0`](https://github.com/callahantiff/PheKnowLator/wiki/OWL-NETS-2.0)**  
____

### Purpose
The knowledge graph can be built with or without the inclusion of edges that contain OWL Semantics. The method we use to decode OWL semantics is called [`OWL-NETS`](https://github.com/callahantiff/PheKnowLator/wiki/OWL-NETS-2.0). 

OWL-NETS (NEtwork Transformation for Statistical learning) is a computational method that reversibly abstracts Web Ontology Language (OWL)-encoded biomedical knowledge into a more biologically meaningful network representation. OWL-NETS generates semantically rich knowledge graphs that contain heterogeneous nodes and edges and can be used for tasks which do not require OWL semantics. For additional information on this method and how it works, please see the above Wiki.
 
> Callahan TJ, Baumgartner Jr WA, Bada M, Stefanski AL, Tripodi I, White EK, Hunter LE. OWL-NETS: Transforming OWL representations for improved network inference. [PMID:29218876](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5737627/).

<br>

_____


🛑 *<b>ASSUMPTIONS</b>* 🛑  
- The algorithm does not require any additional input in order to run the `OWL-NETS` algorithm.  
- There is functionality that when running the algorithm can force the confirmation of the original edges to be purely substance- or instance-based based on the [construction approach](https://github.com/callahantiff/PheKnowLator/tree/master/resources/construction_approach) used to build the knowledge graph.  
  - If Substance-based ➞ convert all triples containing `rdf:type` to `rdfs:subClassOf`  
    - Convert all triples containing `rdf:type` to `rdfs:subClassOf`: `x, rdf:type, y` ➞ `x, rdfs:subClassOf, y`
    - Make `x` `rdfs:subClassOf` all of the ancestors of `y`
  - If Instance-based ➞ convert all triples containing `rdfs:subClassOf` to `rdf:type`   
    - Convert all triples containing `rdfs:subClassOf` to `rdf:type`: `x, rdfs:subClassOf, y` ➞ `x, rdf:type, y`
    - Make `x` `rdf:type` all of the ancestors of `y`
