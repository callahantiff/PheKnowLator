***
## Constructing Edge Lists  
***
***

**Wiki Page:** **[`Data Sources`](https://github.com/callahantiff/PheKnowLator/wiki/v2-Data-Sources#data-sources)**  
**Jupyter Notebook:** **[`Data_Preparation.ipynb`](https://github.com/callahantiff/PheKnowLator/blob/master/Data_Preparation.ipynb)**  

___

### Purpose
The first step in constructing a knowledge graph is to build edge lists. In the current build of PheKnowLator (**[`v2.0.0`](https://github.com/callahantiff/PheKnowLator/wiki/v2.0.0)**), this requires downloading and using several sources of linked open data (see Wiki page referenced above for additional information on each source). 

<br>

_OUTPUT:_ Running this step will output a `json` file to `resources/Master_Edge_List_Dict.json`. The structure of this file is shown below:

```python
master_edges = {'chemical-disease'  :
                {'source_labels'    : ';MESH_;',
                 'data_type'        : 'class-class',
                 'edge_relation'    : 'RO_0002606',
                 'uri'              : ('http://purl.obolibrary.org/obo/',
                                       'http://purl.obolibrary.org/obo/'),
                 'delimiter'        : '#',
                 'column_idx'       : '1;4',
                 'identifier_maps'  : '0:./MESH_CHEBI_MAP.txt;1:disease-dbxref-map',
                 'evidence_criteria': "5;!=;' ",
                 'filter_criteria'  : 'None',
                 'edge_list'        : ["CHEBI_81395", "DOID_12858"],
                                      ["CHEBI_81395", "DOID_0090103"], ...,
                                      ["CHEBI_81395", "DOID_0090104"]}
```

<br>

🛑 *<b>ASSUMPTIONS</b>* 🛑  
**The algorithm makes the following assumptions:**
- All downloaded data sources are listed, with metadata, in the [`edge_source_metadata.txt`](https://github.com/callahantiff/PheKnowLator/blob/master/resources/edge_data/edge_source_metadata.txt) document.  
- Any data preprocessing, including the development of identifier mapping and evidence/filtering data, has been completed prior to building the edge lists. A Jupyter notebook containing examples of different preprocessing steps can be found [`here`](https://github.com/callahantiff/PheKnowLator/blob/master/Data_Preparation.ipynb).  
