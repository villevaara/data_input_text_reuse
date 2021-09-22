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

hume_T004004_paths = [
    "../data/work/hume/hume_T004004"]
hume_T004010_paths = [
    "../data/work/hume/hume_T004010"]
hume_T083618_paths = [
    "../data/work/hume/hume_T083618"]
hume_T142762_paths = [
    "../data/work/hume/hume_T142762"]
hume_T217867_paths = [
    "../data/work/hume/hume_T217867"]
hume_T004022_paths = [
    "../data/work/hume/hume_T004022"]
hume_T103179_paths = [
    "../data/work/hume/hume_T103179"]
hume_T167239_paths = [
    "../data/work/hume/hume_T167239"]
hume_T004007_paths = [
    "../data/work/hume/hume_T004007"]
hume_T167242_paths = [
    "../data/work/hume/hume_T167242"]
hume_T142264_paths = [
    "../data/work/hume/hume_T142264"]
hume_T051777_paths = [
    "../data/work/hume/hume_T051777"]
hume_T004986_paths = [
    "../data/work/hume/hume_T004986"]
hume_T004008_paths = [
    "../data/work/hume/hume_T004008"]
hume_N70236_paths = [
    "../data/work/hume/hume_N70236"]
hume_N008414_paths = [
    "../data/work/hume/hume_N008414"]
hume_N008416_paths = [
    "../data/work/hume/hume_N008416"]
hume_T004002_paths = [
    "../data/work/hume/hume_T004002_1",
    "../data/work/hume/hume_T004002_2",
    "../data/work/hume/hume_T004002_3"]
hume_T033497_paths = [
    "../data/work/hume/hume_T033497_1",
    "../data/work/hume/hume_T033497_2"]

datasets = [
    hume_T004004_paths,
    hume_T004010_paths,
    hume_T083618_paths,
    hume_T142762_paths,
    hume_T217867_paths,
    hume_T004022_paths,
    hume_T103179_paths,
    hume_T167239_paths,
    hume_T004007_paths,
    hume_T167242_paths,
    hume_T142264_paths,
    hume_T051777_paths,
    hume_T004986_paths,
    hume_T004008_paths,
    hume_N70236_paths,
    hume_N008414_paths,
    hume_N008416_paths,
    hume_T004002_paths,
    hume_T033497_paths]

galeitems = read_csv_to_dictlist(
    "../data/work/idpairs_rich_ecco_eebo_estc.csv")

for item in galeitems:
    if item['author'] == "":
        item['author'] = None
    item['publication_year'] = int(item['publication_year'])
    item['text_length'] = int(item['text_length'])

galeitems_by_id = list_to_dict_by_key(galeitems, 'document_id')

for dataset in datasets:
    for path in dataset:
        all_data = []
        jsonfiles = get_datafiles(path)
        for jsonfileloc in jsonfiles:
            with open(jsonfileloc, 'r') as jsonfile:
                jsondata = json.load(jsonfile)
                all_data.extend(jsondata)
        #
        this_estcid = galeitems_by_id[all_data[0]['id_primary']][0]['estc_id']
        this_galeid = all_data[0]['id_primary']
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
            '../data/work/hume_full/hume_galeid_' +
            this_galeid + "_estcid_" +
            this_estcid + '.json')

        #
        with open(outjson, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=4)
