***
## Knowledge Graph Construction   
***
***

**Wiki Page:** **[`KG-Construction`](https://github.com/callahantiff/PheKnowLator/wiki/KG-Construction)**  

____

### Purpose
Describe the different parameters and arguments that can be passed when using PheKnowLator to build a knowledge graph. The different options include: [build type](#build-type), [construction approach](#construction-approach), [relation or edge directionality](#relationsedge-directionality), [node metadata](#node-metadata) use, and [decoding of owl semantics](#decoding-owl-semantics). Each of these parameters is explained below.  

<br>

_____


### Build Type   
The knowledge graph build algorithm has been designed to run from three different stages of development: `full`, `partial`, and `post-closure`.

Build Type | Description | Use Cases  
:--: | -- | --   
`full` | Runs all build steps in the algorithm | You want to build a knowledge graph and will not use a reasoner  
`partial` | Runs all of the build steps in the algorithm through adding the edges<br><br> Node metadata can always be added to a `partial` built knowledge graph by running the build as `post-closure` | You want to build a knowledge graph and plan to run a reasoner over it<br><br> You want to build a knowledge graph, but do not want to include node metadata, filter OWL semantics, or generate triple lists  
`post-closure` | Assumes that a reasoner was run over a knowledge graph and that the remaining build steps should be applied to a closed knowledge graph. The remaining build steps include determining whether OWL semantics should be filtered and creating and writing triple lists<br><br> If this option is chosen, place the closed knowledge graph `.owl` file in the `resources/knowledge_graphs/relations_only/` or `resources/knowledge_graphs/relations_only/` directory (depending on how the graph was built) | You have run the `partial` build, ran a reasoner over it, and now want to complete the algorithm<br><br> You want to use the algorithm to process metadata and owl semantics for an externally built knowledge graph

<br> 

_____


### Construction Approach   
New data can be added to the knowledge graph using 2 different construction approaches: (1) `instance-based` or (2) `subclass-based`. Please see this [`README.md`](https://github.com/callahantiff/PheKnowLator/blob/master/resources/construction_approach/README.md) for more information.   

<br> 

_____

### Relations/Edge Directionality   
PheKnowLator can be built using a single set of provided relations (i.e. the `owl:ObjectProperty` or edge which is used to connect the nodes in the graph) with or without the inclusion of each relation's inverse. Please see this [`README.md`](https://github.com/callahantiff/PheKnowLator/blob/master/resources/relations_data/README.md) for additional information.  

<br> 

_____


### Node Metadata
The knowledge graph can be built with or without the inclusion of instance node metadata (i.e. labels, descriptions or definitions, and, synonyms). Please see this [`README.md`](https://github.com/callahantiff/PheKnowLator/blob/master/resources/node_data/README.md) for additional information. 

<br> 

_____


### Decoding OWL Semantics  
The knowledge graph can be built with or without the inclusion of edges that contain OWL Semantics. Please see this [`README.md`](https://github.com/callahantiff/PheKnowLator/blob/master/resources/owl_decoding/README.md) for additional information. 

<br>

🛑 *<b>ASSUMPTIONS</b>* 🛑  
**The algorithm makes the following assumptions:**
- Edge list data has been created (see [here](https://github.com/callahantiff/PheKnowLator/blob/master/resources/edge_data) for additional information)  
- Ontologies have been preprocessed (see [here](https://github.com/callahantiff/PheKnowLator/blob/master/resources/ontologies/README.md) for additional information)  
- Decisions made and required input documentation provided for each of the parameters described above.     
