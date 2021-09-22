from lib.octavo_api_client import OctavoAPIClient
import csv


this_client = OctavoAPIClient(limit=-1)
outfile = "../data/work/idpairs_ritch_ecco_eebo_estc.csv"

api_ecco = "https://hume:bacon@vm0824.kaj.pouta.csc.fi/octavo/ecco/search"
api_eebo = "https://hume:bacon@vm0824.kaj.pouta.csc.fi/octavo/eebo/search"
fields_to_get = ['documentID', 'ESTCID', 'collectionID',
                 'publication_year', 'title', 'author', 'documentLength']

fields_ecco1 = {
    'query': "<DOCUMENT§collectionID:ecco1§DOCUMENT>",
    'field': fields_to_get,
    'limit': -1,
    'timeout': 60}
fields_ecco2 = {
    'query': "<DOCUMENT§collectionID:ecco2§DOCUMENT>",
    'field': fields_to_get,
    'limit': -1,
    'timeout': 60}
fields_eebo1 = {
    'query': "<DOCUMENT§collectionID:eebo1§DOCUMENT>",
    'field': fields_to_get,
    'limit': -1,
    'timeout': 60}
fields_eebo2 = {
    'query': "<DOCUMENT§collectionID:eebo2§DOCUMENT>",
    'field': fields_to_get,
    'limit': -1,
    'timeout': 60}

results_ecco1 = this_client.post_api_response(
    api_ecco, fields_ecco1)['results']['docs']
results_ecco2 = this_client.post_api_response(
    api_ecco, fields_ecco2)['results']['docs']
results_eebo1 = this_client.post_api_response(
    api_eebo, fields_eebo1)['results']['docs']
results_eebo2 = this_client.post_api_response(
    api_eebo, fields_eebo2)['results']['docs']

all_res = []
all_res.extend(results_ecco1)
all_res.extend(results_ecco2)
all_res.extend(results_eebo1)
all_res.extend(results_eebo2)


with open(outfile, 'w') as csvfile:
    fieldnames = ['estc_id', 'document_id', 'collection', 'title', 'author',
                  'publication_year', 'text_length']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for item in all_res:
        this_document_id_raw = str(item['documentID'])
        if item['collectionID'] in ['eebo1', 'eebo2']:
            this_collection = 'eebo'
            if this_document_id_raw[:2] == "11":
                this_document_id = "B" + this_document_id_raw[2:]
            elif this_document_id_raw[:2] == "10":
                this_document_id = "A" + this_document_id_raw[2:]
        else:
            this_collection = 'ecco'
            this_document_id = this_document_id_raw
        writer.writerow({'estc_id': item['ESTCID'],
                         'document_id': this_document_id,
                         'collection': this_collection,
                         'publication_year': item['publication_year'],
                         'title': item['title'],
                         'author': item['author'],
                         'text_length': item['documentLength']})
