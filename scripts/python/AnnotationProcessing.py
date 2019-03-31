################################################################################################
# AnnotationProcessing.py
# Purpose: script processes the annotation results from PubAnnotation
# version 1.0.0
# date: 10.24.2017
################################################################################################


## import module/script dependencies
import scripts.python.QueryEndpoint as QueryEndpoint
import json



def main():

    # read in query
    file = './resources/endpoints/queries/preeclampsia_10.25.17.txt'
    Query = open(file).read()

    # run query against endpoint
    url = 'http://pubannotation.org/projects/Preeclampsia/search'
    output = './resources/endpoints/data/preeclampsia_10.25.17'
    annotations = QueryEndpoint.RunQuery(Query, output, url)
    len(annotations['results']['bindings'])

    # open data
    with open("./resources/endpoints/data/preeclampsia_10.25.17.json") as json_data:
        annotations = json.load(json_data)

    # process results
    outfile = open('./resources/endpoints/data/preeclampsia_PubAnnotation.txt', "w")
    for result in annotations['results']['bindings']:
        pmid = str(result['doc']['value'].split('/')[-1])
        gene = str(result['gene']['value'].split('/')[-1].replace(':', '_'))
        disease = str(result['disease']['value'].split('/')[-1].replace(':', '_'))
        sentence = str(result['span_text']['value'])

        if gene != 'owl#Thing' and disease != 'owl#Thing':
            outfile.write(str(pmid) + '\t' + str(gene)+ '\t' + str(disease)+ '\t' + str(sentence) + '\n')

    outfile.close()



if __name__ == "__main__":
    main()