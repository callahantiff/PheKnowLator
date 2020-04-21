***
## Decoding OWL Semantics    
***
***

**Wiki Page:** **[`OWL-NETS-2.0`](https://github.com/callahantiff/PheKnowLator/wiki/OWL-NETS-2.0)**  
**Required Input:** `resources/owl_decoding/OWL_NETS_Property_Types.txt`
____

### Purpose
The knowledge graph can be built with or without the inclusion of edges that contain OWL Semantics. The method we use to decode OWL semantics is called OWL-NETS. 

OWL-NETS (NEtwork Transformation for Statistical learning) is a computational method that reversibly abstracts Web Ontology Language (OWL)-encoded biomedical knowledge into a more biologically meaningful network representation. OWL-NETS generates semantically rich knowledge graphs that contain heterogeneous nodes and edges and can be used for tasks which do not require OWL semantics. For additional information on this method and how it works, please see the [`OWL-NETS-2.0`](https://github.com/callahantiff/PheKnowLator/wiki/OWL-NETS) Wiki.
 
> Callahan TJ, Baumgartner Jr WA, Bada M, Stefanski AL, Tripodi I, White EK, Hunter LE. OWL-NETS: Transforming OWL representations for improved network inference. [PMID:29218876](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5737627/).

<br>

_____

### Input Requirements
In order to decode the OWL semantics in a knowledge graph, the algorithm requires a single `.txt` file that contains all `owl:Property` types that should be kept when running `OWL-NETS`. An example of the content included in this file is shown below:  
```txt
http://purl.obolibrary.org/obo/activates
http://purl.obolibrary.org/obo/BFO_0000050
http://purl.obolibrary.org/obo/BSPO_0015202
http://purl.obolibrary.org/obo/chebi#has_functional_parent
http://purl.obolibrary.org/obo/CLO_0054409
http://purl.obolibrary.org/obo/core#connected_to
http://purl.obolibrary.org/obo/RO_0000053
```

<br>

_NOTE._ This file should contain all relations included in the [`resource_info.txt`](https://github.com/callahantiff/PheKnowLator/blob/master/resources/resource_info.txt) file.

<br>

_____


🛑 *<b>ASSUMPTIONS</b>* 🛑  
**The algorithm makes the following assumptions:**
- A `.txt` file containing the `owl:Property` types has been placed in the `./resources/owl_decoding/` directory.   
