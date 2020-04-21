***
## Preparing Ontology Data 
***
***

**Wiki Page:** **[`Data Sources`](https://github.com/callahantiff/PheKnowLator/wiki/v2-Data-Sources#ontologies)**  
**Jupyter Notebook:** **[`Ontology_Cleaning.ipynb`](https://github.com/callahantiff/PheKnowLator/blob/master/Ontology_Cleaning.ipynb)**  

___

### Purpose
We recommend that users take stes to verify the content in their ontologies prior to building the knowledge graph. PheKnowLator does not currently provide any support for verifying the data quality of ontology data. That being said, we do recommend that users verify the following in each of their ontology sources:  

**Cleaning Ontologies**  
While most ontologies are released with none or minor errors, it is still good practice to verify that the ontology is error free.  
- WHAT: Specific things you might look for include:  
  - Invalid typing for literals  
  - Punning (i.e. illegal redeclarations of entity types)  
  - Errors or inconsistencies in ontology identifiers  
  - Remove obsolete ontology classes  
- HOW: To do this, we reccommend opening the downloaded ontology file using an application like [Protége](https://protege.stanford.edu/) and running the ontology debugger. If you prefer to use Python, we recommend using the [`owlready2`](https://pypi.org/project/Owlready2/) library. 

**Merge Ontologies**:  
Often times there are errors that only exist in the presence or other ontologies. The most common error which occurs as a result of merging ontology files is punning. To merge a directory of ontologies, you can run the following:  

  ```python   
  #import needed libraries
  from pkt_kg.utils import merges_ontologies
  
  # set-up inpiut variables
  write_location = './resources/knowledge_graphs'
  merged_ontology_file = '/PheKnowLator_MergedOntologies.owl
  ontology_repository = glob.glob('*/ontologies/*.owl')
  
  # merge the ontologies
  merges_ontologies(ontology_repository, write_location, merged_ontology_file)
  ```

**Normalize Classes**:  
It is important to verify that there is consistency between the ontology classes once the ontology files have been merged together. We recommend veroifying two types of class-class consistency:  
- _Connectivity Between Existing Classes_: Make sure that all classes that represent the same entity are connected to each other.   
  - EXAMPLE: The [Sequence Ontology](http://www.sequenceontology.org/), [Chemical Entities of Biological Interest (ChEBI)](https://www.ebi.ac.uk/chebi), and [PRotein Ontology](https://proconsortium.org/) all include terms for protein, but none of these classes are connected to each other.  
- _Consistency Between Ontology Classes and New Edge Data Nodes_: Make sure that any of the existing ontology classes can be aligned with any of the new data entities that you want to add to the knowledge graph.   
  - EXAMPLE: There are several gene classes in the [Human Phenotype Ontology](https://hpo.jax.org/) and [PRotein Ontology](https://proconsortium.org/) that use [HGNC](https://www.genenames.org/) identifiers. If you don't want to use HGNC identifiers, then all of these classes will have to be updated.  

<br>

Please see the [`Ontology_Cleaning.ipynb`](https://github.com/callahantiff/PheKnowLator/blob/master/Ontology_Cleaning.ipynb) Jupyter notebook for an example of how ontology was cleaned for the current PheKnowLator build. 

<br>

### When Should I Perform Ontology Preprocessing?  
We recommending performing ontology preprocessing and cleaning steps after downloading all ontology files, but before building the knowledge graph.       

<br>

🛑 *<b>ASSUMPTIONS</b>* 🛑  
**The algorithm makes the following assumptions:**
- All ontologies are listed in the [`ontology_source_metadata.txt`](https://github.com/callahantiff/PheKnowLator/blob/master/resources/ontologies/ontology_source_metadata.txt) document.   
- Any data preprocessing or cleaning has been completed prior to building the knowledge graph. A Jupyter notebook containing examples of different preprocessing steps can be found [`here`](https://github.com/callahantiff/PheKnowLator/blob/master/Ontology_Cleaning.ipynb).  
