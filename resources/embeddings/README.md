***
## Creating Node Embeddings  
***
***

**Wiki Page:** **[`KG Construction`](https://github.com/callahantiff/PheKnowLator/wiki/KG-Construction#create-mechanism-embeddings)**  
**Jupyter Notebook:** **[`main.ipynb`](https://github.com/callahantiff/PheKnowLator/blob/master/main.ipynb)**  
___

### Purpose
To create estimates of molecular mechanisms, we derive node embeddings from the knowledge graph.
 
**Methods:**  
[DeepWalk](https://github.com/phanein/deepwalk). This repository contains code to run two versions of the [original
 method](http://www.perozzi.net/publications/14_kdd_deepwalk.pdf) developed by [Bryan Perozzi](https://github.com/phanein).   

<br>

**[`DeepWalk algorithm-C`](https://github.com/xgfs/deepwalk-c):** an implementation of the original algorithm in C
 ++, with some improvements to speed up initialize the hierarchical softmax tree that was developed by [Anton Tsitsulin](https://github.com/xgfs).  

<br>

**[`DeepWalk-RDF`](https://github.com/bio-ontology-research-group/walking-rdf-and-owl):** an extension of the
  original algorithm that also embeds graph edges; developed by [the Bio-Ontology Research Group](https://github.com/bio-ontology-research-group/walking-rdf-and-owl).  

**🛑 NOTE 🛑:** This library depends on the [C++ Boost library](https://www.pyimagesearch.com/2015/04/27/installing-boost-and-boost-python-on-osx-with-homebrew/) and [Boost Threadpool Header Files](http://threadpool.sourceforge.net/). In order to compile and run `Deepwalk-RDF`, there are a few important changes that will need to be made to the Headers in the sub-directory called `Boost` that is located at the top-level of the `walking-rdf-and-owl-master` directory:    
  - Change `TIME_UTC` to `TIME_UTC_` in the `boost/threadpool/task_adaptors.hpp`.  
  - Change the `-lboost_thread` argument to `-lboost_thread-mt` in the `walking-rdf-and-owl-master/Makefile`   
  - To troubleshoot incompatibility issues between `Deepwalk` and `Gensim`, run the following in this order:  
    - `pip uninstall gensim`  
    - `pip uninstall deepwalk`  
    - `pip install gensim==0.10.2` 
    - `pip install deepwalk`   
