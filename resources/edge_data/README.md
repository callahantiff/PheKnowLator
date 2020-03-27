***
## Constructing Edge Lists  
***
***

**Wiki Page:** **[`Data Sources`](https://github.com/callahantiff/PheKnowLator/wiki/v2-Data-Sources)**  
**Jupyter Notebook:** **[`Data_Preparation.ipynb`](https://github.com/callahantiff/PheKnowLator/blob/master/Data_Preparation.ipynb)**  
**Script:** **[`edge_list.py`](https://github.com/callahantiff/PheKnowLator/blob/master/pkt_kg/edge_list.py)** 

___

### Purpose
The first step in constructing a knowledge graph is to build edge lists. In the current build of PheKnowLator (**[`v2.0.0`](https://github.com/callahantiff/PheKnowLator/wiki/v2.0.0)**), this requires downloading and using several sources of linked open data (see Wiki page referenced above for additional information on each source). 

<br>

🛑 *<b>ASSUMPTIONS</b>* 🛑  
**The algorithm makes the following assumptions:**
- All data sources are listed in the [`edge_source_metadata.txt`](https://github.com/callahantiff/PheKnowLator/blob/master/resources/edge_data/edge_source_metadata.txt) document.  
- Any data preprocessing, including the development of identifier mapping and evidence/filtering data, has been completed prior to building the edge lists. A Jupyter notebook containing examples of different preprocessing steps can be found [`here`](https://github.com/callahantiff/PheKnowLator/blob/master/Data_Preparation.ipynb).  
