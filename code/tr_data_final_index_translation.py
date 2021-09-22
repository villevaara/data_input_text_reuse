from lib.utils_common import read_csv_to_dictlist
from lib.helpers import list_to_dict_by_key
# from lib.tr_analysis_helpers import get_unified_id
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


def get_headers_from_document_text(
        document_text, raw_text, collection):
    header_index_and_text = []
    header_indices = []
    # markers: newlines and headers, start of string and headers
    header_markers = ['^###### ', '^##### ', '^#### ', '^### ', '^## ', '^# ',
                      '\n\n###### ', '\n\n##### ',
                      '\n\n#### ', '\n\n### ', '\n\n## ', '\n\n# ']
    for header_marker in header_markers:
        for match in re.finditer(header_marker, document_text):
            if header_marker[0:2] == '\n\n':
                header_indices.append({
                    'index': match.start()+2,
                    'marker_length': len(header_marker)-2})
            else:
                header_indices.append({
                    'index': match.start(),
                    'marker_length': len(header_marker)})
    for index in header_indices:
        full_header = document_text[index['index']:]
        newline_position = full_header.find('\n\n') + 2
        full_header_text = full_header[0:newline_position]
        if collection != "eebo":
            if ('\n\n' + full_header_text) in raw_text:
                continue
                # if header text is found on raw text too it is
                # a false positive
        header_index_and_text.append({
            'index': index['index'],
            'header_text': full_header_text
            })
    retlist = sorted(header_index_and_text, key=lambda k: k['index'])
    return retlist


def get_char_offset_index_from_headers(headers, raw_text):
    new_index = OrderedDict()
    cumulative_offset = 0
    # if whitespaces stripped from start, set cumulative offset accordingly
    if len(raw_text) > len(raw_text.lstrip()):
        cumulative_offset = len(raw_text.lstrip()) - len(raw_text)
    for item in headers:
        this_index = item['index'] - cumulative_offset
        cumulative_offset += (len(item['header_text']))
        # back to back headers:
        if this_index in new_index.keys():
            new_index[this_index] = {
                'offset': cumulative_offset,
                'header': new_index[
                    this_index]['header'] + item['header_text'],
                'header_api_index': item['index']}
        else:
            new_index[this_index] = {
                'offset': cumulative_offset,
                'header': item['header_text'],
                'header_api_index': item['index']}
    return new_index


def get_char_offset_index_from_eebo_headers(headers, eebotextdata):
    new_index = OrderedDict()
    new_index[0] = {
        'offset': eebotextdata['offset'],
        'header': "",
        'header_api_index': 0}
    for item in headers:
        this_index = item['index']
        if this_index > eebotextdata['offset'] + len(eebotextdata['text']):
            continue
        if this_index < eebotextdata['offset']:
            continue
        new_index[this_index - eebotextdata['offset']] = {
            'offset': eebotextdata['offset'],
            'header': item['header_text'],
            'header_api_index': item['index']}
    return new_index


def test_offsets(raw_text, api_text, offsets, text_id):
    # terminate = False
    for key, value in offsets.items():
        if raw_text[key:key+5] != api_text[
                 key + value['offset']:key + value['offset'] + 5]:
            print("Text mismatch at: " + text_id + " index: " + str(key) +
                  " offset: " + str(value['offset']))
    #         terminate = True
    # if terminate:
    #     sys.exit("EXIT: Text mismatch error.")


def writetext(textstr, outfile):
    with open(outfile, 'w') as outtxt:
        outtxt.write(textstr)


def get_raw_eebotext(text_id, eebo_by_id):
    if text_id.split('text')[-1] != '':
        return None
    general_id = text_id = text_id.split(".")[0]
    raw_text_path = eebo_by_id[general_id][0]['path'] + "/"
    dirfiles = os.listdir(raw_text_path)
    datafiles = []
    for file in dirfiles:
        if file.endswith('_text.txt'):
            this_id = file[:-4]
            this_order = int(this_id.split("_")[1])
            with open(raw_text_path + file, 'r') as txtfile:
                raw_text = txtfile.read()
            if this_id[0] == "A":
                api_id = "10" + this_id.split(".")[0][1:]
            else:
                api_id = "11" + this_id.split(".")[0][1:]
            datafiles.append({
                'text': raw_text,
                'offset': 0,
                'order': this_order,
                'raw_text_id': this_id,
                'api_text_id': api_id})
            # datafiles.append(raw_text_path + file)
    datafiles = sorted(datafiles, key=lambda k: k['order'])
    prev_offset = 0
    retdict = {}
    for item in datafiles:
        item['offset'] = prev_offset
        prev_offset += (len(item['text']) + 2)
        retdict[item['raw_text_id']] = item
    # eebo texts for api are joined by \n\n
    return retdict


galeitems = read_csv_to_dictlist(
    "../data/work/idpairs_rich_ecco_eebo_estc.csv")
# jsonfiles = get_datafiles("../data/work/bayle1734_2/")
eccoindex = read_csv_to_dictlist("../data/work/ecco_dict.csv")
eeboindex = read_csv_to_dictlist("../data/work/eebo_dict.csv")
eebo_by_id = list_to_dict_by_key(eeboindex, 'id')
ecco_by_id = list_to_dict_by_key(eccoindex, 'id')
outjson = '../data/work/tr_offset_index.json'

jsonpaths = [
    "../data/work/hume_full/",
    "../data/work/hackathon/"
    ]


all_data = []
for path in jsonpaths:
    jsonfiles = get_datafiles(path)
    for jsonfileloc in jsonfiles:
        with open(jsonfileloc, 'r') as jsonfile:
            jsondata = json.load(jsonfile)
            all_data.extend(jsondata)


# xxxx
ecco_api_client = OctavoEccoClient()
eebo_api_client = OctavoEeboClient()
char_offsets = {}

text_ids = []
for item in all_data:
    text_ids.append(item['id_primary'])
    text_ids.append(item['id_secondary'])

text_ids = list(set(text_ids))
# text_ids = text_ids[:50]
# text_ids = ["A56206.headed_2_text", "A56206.headed_1_text"]
# text_ids = ['0081400111']
# text_ids = ["A65112.headed_2_text"]
# text_ids = ["A56206.headed_2_text"]
# text_ids = ['0818700401']
# text_ids = ["A90295.headed_1_text"]
# '0187701400' -- whitespaces stripped from start in api text
# problems still:
# 'A65588'

for text_id in text_ids:
    print(text_id)
    if text_id[0] == "A" or text_id[0] == "B":
        if text_id.split("_")[-1] != 'text':
            continue
        else:
            raw_text_path = eebo_by_id[
                text_id.split(".")[0]][0]['path'] + "/" + (text_id + ".txt")
            eebotextdata = get_raw_eebotext(text_id, eebo_by_id)[text_id]
            raw_text = eebotextdata['text']
            api_text = eebo_api_client.get_text_for_document_id(
                eebotextdata['api_text_id'])['text']
            collection = "eebo"
    else:
        api_text = ecco_api_client.get_text_for_document_id(text_id)['text']
        raw_text_path = ecco_by_id[text_id][0]['path'] + "/" + text_id + ".txt"
        collection = "ecco"
        with open(raw_text_path, 'r') as txtfile:
            raw_text = txtfile.read()
    #
    headers = get_headers_from_document_text(api_text, raw_text, collection)
    if collection == "ecco":
        offset = get_char_offset_index_from_headers(headers, raw_text)
    else:
        offset = get_char_offset_index_from_eebo_headers(headers, eebotextdata)
    test_offsets(raw_text, api_text, offset, text_id)
    char_offsets[text_id] = offset

# indexdataloc = '../data/work/bayle1734_index.json'

# with open(outjson, 'r', encoding='utf-8') as f:
#     temp_indexdata = json.load(f)
#     indexdata = {}
#     for key, value in temp_indexdata.items():
#         item_indices = {}
#         for index, items in value.items():
#             item_indices[int(index)] = items
#         indexdata[key] = item_indices

# for key, value in char_offsets.items():
#     indexdata[key] = value

with open(outjson, 'w', encoding='utf-8') as f:
    json.dump(char_offsets, f, ensure_ascii=False, indent=4)
