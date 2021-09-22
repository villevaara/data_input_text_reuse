from lib.utils_common import read_csv_to_dictlist
from lib.helpers import list_to_dict_by_key
from lib.tr_analysis_helpers import get_api_id
# from lib.estc_data_helpers import get_long_estc_id
from lib.octavo_api_client import OctavoEccoClient, OctavoEeboClient
import os
# import csv
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


bayle1734a_jsonpaths = [
    "../data/work/bd1734a_1/",
    "../data/work/bd1734a_2/",
    "../data/work/bd1734a_3/",
    "../data/work/bd1734a_4/",
    "../data/work/bd1734a_5/",
    "../data/work/bd1734a_6/",
    "../data/work/bd1734a_7/",
    "../data/work/bd1734a_8/",
    "../data/work/bd1734a_9/",
    "../data/work/bd1734a_10/",
    ]
bayle1734b_jsonpaths = [
    "../data/work/bd1734b_1/",
    "../data/work/bd1734b_2/",
    "../data/work/bd1734b_3/",
    "../data/work/bd1734b_4/",
    "../data/work/bd1734b_5/",
    ]

bayle1710_jsonpaths = [
    "../data/work/bd1710_1/",
    "../data/work/bd1710_2/",
    "../data/work/bd1710_3/",
    "../data/work/bd1710_4/",
    ]

galeitems = read_csv_to_dictlist(
    "../data/work/idpairs_rich_ecco_eebo_estc.csv")

for item in galeitems:
    if item['author'] == "":
        item['author'] = None
    item['publication_year'] = int(item['publication_year'])
    item['text_length'] = int(item['text_length'])

galeitems_by_id = list_to_dict_by_key(galeitems, 'document_id')

for dataset in [
        bayle1734a_jsonpaths, bayle1734b_jsonpaths, bayle1710_jsonpaths]:
    for path in dataset:
        all_data = []
        jsonfiles = get_datafiles(path)
        for jsonfileloc in jsonfiles:
            with open(jsonfileloc, 'r') as jsonfile:
                jsondata = json.load(jsonfile)
                all_data.extend(jsondata)
        #
        this_estcid = galeitems_by_id[all_data[0]['id_primary']][0]['estc_id']
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
                index_and_header = get_correct_index(
                    item[key], indexdata[item['id_primary']])
                item[key + '_octavo'] = index_and_header['index']
                item['header_primary' + "_" +
                     key.split('_')[1]] = index_and_header['header']
                item['api_id_primary'] = get_api_id(item['id_primary'])['id']
                item['api_primary'] = get_api_id(item['id_primary'])['api']
            for key in ['text_start_secondary', 'text_end_secondary']:
                if item['id_secondary'] not in indexdata.keys():
                    item[key + '_octavo'] = -1
                    item['header_secondary' + "_" +
                         key.split('_')[1]] = "NOT FOUND"
                    item['api_id_secondary'] = ""
                    item['api_secondary'] = ""
                    continue
                index_and_header = get_correct_index(
                    item[key], indexdata[item['id_secondary']])
                item[key + '_octavo'] = index_and_header['index']
                item['header_secondary' + "_" +
                     key.split('_')[1]] = index_and_header['header']
                item['api_id_secondary'] = get_api_id(
                    item['id_secondary'])['id']
                item['api_secondary'] = get_api_id(
                    item['id_secondary'])['api']
        outjson = (
            '../data/work/hackathon/' +
            path.split("/")[-2] + "_estcid_" +
            this_estcid + '.json')
        #
        with open(outjson, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=4)
        print("Wrote:" + outjson)
