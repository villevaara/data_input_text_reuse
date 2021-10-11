from lib.utils_common import read_csv_to_dictlist
from lib.helpers import list_to_dict_by_key
from lib.tr_analysis_helpers import get_api_id, get_unified_id
# from lib.estc_data_helpers import get_long_estc_id
from lib.octavo_api_client import OctavoEccoClient, OctavoEeboClient
import os
import csv
import json
from collections import OrderedDict
import re
import sys


def get_datafiles(rootdir):
    dirfiles = os.listdir(rootdir)
    datafiles = []
    for file in dirfiles:
        if file.endswith('.json'):
            datafiles.append(rootdir + "/" + file)
    return datafiles


# def writetext(textstr, outfile):
#     with open(outfile, 'w') as outtxt:
#         outtxt.write(textstr)


def get_correct_index(tr_index, item_indexdata):
    higher_indices = []
    for index in item_indexdata.keys():
        if index <= tr_index:
            higher_indices.append(index)
    if len(higher_indices) == 0:
        return {
            'index': tr_index,
            'header': ""}
    closest_lower = max(higher_indices)
    return {
            'index': tr_index + item_indexdata[closest_lower]['offset'],
            'header': item_indexdata[closest_lower]['header']}


# galeitems = read_csv_to_dictlist(
#     "../data/work/idpairs_rich_ecco_eebo_estc.csv")
# eccoindex = read_csv_to_dictlist("../data/work/ecco_dict.csv")
# eeboindex = read_csv_to_dictlist("../data/work/eebo_dict.csv")
# eebo_by_id = list_to_dict_by_key(eeboindex, 'id')
# ecco_by_id = list_to_dict_by_key(eccoindex, 'id')

indexdataloc = '../data/work/tr_offset_index.json'
with open(indexdataloc, 'r', encoding='utf-8') as f:
    temp_indexdata = json.load(f)
    indexdata = {}
    for key, value in temp_indexdata.items():
        item_indices = {}
        for index, items in value.items():
            item_indices[int(index)] = items
        indexdata[key] = item_indices


data_paths = [
    "../data/work/projc/A88639",
    "../data/work/projc/0098301300",
    "../data/work/projc/0256800600",
    "../data/work/projc/0294000102",
    "../data/work/projc/0434800100",
    "../data/work/projc/0661600600",
    "../data/work/projc/1230800400",
    "../data/work/projc/1516302100",
    "../data/work/projc/0111900600",
    "../data/work/projc/0294000101",
    "../data/work/projc/0294000103",
    "../data/work/projc/0564000100",
    "../data/work/projc/1096200100",
    "../data/work/projc/1400100400",
    "../data/work/projc/1538800300",
    ]

galeitems = read_csv_to_dictlist(
    "../data/work/idpairs_rich_ecco_eebo_estc.csv")

for item in galeitems:
    if item['author'] == "":
        item['author'] = None
    item['publication_year'] = int(item['publication_year'])
    item['text_length'] = int(item['text_length'])

galeitems_by_id = list_to_dict_by_key(galeitems, 'document_id')


def filter_all_data_for_json(all_data):
    keep_fields = [
        "id_primary",
        "api_primary",
        "api_id_primary",
        "text_primary",
        "text_start_primary_octavo",
        "text_end_primary_octavo",
        "header_primary_start",
        "header_primary_end",
        "id_secondary",
        "api_id_secondary",
        "api_secondary",
        "text_secondary",
        "text_start_secondary_octavo",
        "text_end_secondary_octavo",
        "header_secondary_start",
        "header_secondary_end",
        "length",
        "positives_percent",
        "file",
    ]
    filtered_data = []
    for item in all_data:
        filtered_item = {}
        for key in keep_fields:
            filtered_item[key] = item[key]
        filtered_data.append(filtered_item)
    return filtered_data


for path in data_paths:
    all_data = []
    jsonfiles = get_datafiles(path)
    for jsonfileloc in jsonfiles:
        with open(jsonfileloc, 'r') as jsonfile:
            jsondata = json.load(jsonfile)
            all_data.extend(jsondata)
    #
    this_document_id = get_unified_id(all_data[0]['id_primary'])
    #
    for item in all_data:
        for key in ['text_start_primary', 'text_end_primary']:
            if item['id_primary'] not in indexdata.keys():
                item[key + '_octavo'] = -1
                item['header_primary' + "_" +
                     key.split('_')[1]] = "NOT FOUND"
                item['api_id_primary'] = ""
                item['api_primary'] = ""
                continue
            pri_id = get_unified_id(item['id_primary'])
            index_and_header = get_correct_index(
                item[key], indexdata[item['id_primary']])
            item[key + '_octavo'] = index_and_header['index']
            item['header_primary' + "_" +
                 key.split('_')[1]] = index_and_header['header']
            item['api_id_primary'] = get_api_id(item['id_primary'])['id']
            item['api_primary'] = get_api_id(item['id_primary'])['api']
            item['author_primary'] = galeitems_by_id[pri_id][0]['author']
            item['title_primary'] = galeitems_by_id[pri_id][0]['title']
            item['publication_year_primary'] = galeitems_by_id[pri_id][0]['publication_year']
        for key in ['text_start_secondary', 'text_end_secondary']:
            if item['id_secondary'] not in indexdata.keys():
                item[key + '_octavo'] = -1
                item['header_secondary' + "_" +
                     key.split('_')[1]] = "NOT FOUND"
                item['api_id_secondary'] = ""
                item['api_secondary'] = ""
                continue
            sec_id = get_unified_id(item['id_secondary'])
            index_and_header = get_correct_index(
                item[key], indexdata[item['id_secondary']])
            item[key + '_octavo'] = index_and_header['index']
            item['header_secondary' + "_" +
                 key.split('_')[1]] = index_and_header['header']
            item['api_id_secondary'] = get_api_id(
                item['id_secondary'])['id']
            item['api_secondary'] = get_api_id(
                item['id_secondary'])['api']
            item['author_secondary'] = galeitems_by_id[sec_id][0]['author']
            item['title_secondary'] = galeitems_by_id[sec_id][0]['title']
            item['publication_year_secondary'] = galeitems_by_id[sec_id][0]['publication_year']

    outjson = (
        '../data/work/projc/processed/' +
        path.split("/")[-1] + '.json')
    #
    outcsv =  (
        '../data/work/projc/processed/' +
        path.split("/")[-1] + '.csv')
    #
    with open(outjson, 'w', encoding='utf-8') as f:
        jsondata = filter_all_data_for_json(all_data)
        json.dump(jsondata, f, ensure_ascii=False, indent=4)
    print("Wrote:" + outjson)
    #
    with open(outcsv, 'w') as csvf:
        fieldnames = list(all_data[0].keys())
        fieldnames = [
            "id_primary",
            "api_id_primary",
            "api_primary",
            "text_start_primary",
            "text_end_primary",
            "text_primary",
            "text_start_primary_octavo",
            "text_end_primary_octavo",
            "header_primary_start",
            "header_primary_end",
            "author_primary",
            "title_primary",
            "publication_year_primary",
            "id_secondary",
            "api_id_secondary",
            "api_secondary",
            "text_start_secondary",
            "text_end_secondary",
            "text_secondary",
            "text_start_secondary_octavo",
            "text_end_secondary_octavo",
            "header_secondary_start",
            "header_secondary_end",
            "author_secondary",
            "title_secondary",
            "publication_year_secondary",
            "length",
            "positives_percent",
            "file",
        ]
        writer = csv.DictWriter(csvf, fieldnames)
        writer.writeheader()
        for row in all_data:
            writer.writerow(row)
    print("Wrote:" + outcsv)
