from lib.utils_common import read_csv_to_dictlist
from lib.helpers import list_to_dict_by_key
from lib.tr_analysis_helpers import get_unified_id
from lib.estc_data_helpers import get_long_estc_id
from lib.octavo_api_client import OctavoEccoClient
import os
import csv
import json
from collections import OrderedDict
# import matplotlib.pyplot as plt
# import seaborn as sns


def save_outputdata_csv(outputdata, outputfname):
    with open(outputfname, 'w') as outcsv:
        fieldnames = outputdata[0].keys()
        writer = csv.DictWriter(outcsv, fieldnames=fieldnames)
        writer.writeheader()
        for row in outputdata:
            writer.writerow(row)


def get_datafiles(rootdir):
    dirfiles = os.listdir(rootdir)
    datafiles = []
    for file in dirfiles:
        if file.endswith('.json'):
            datafiles.append(rootdir + "/" + file)
    return datafiles


def increment_char_index_count(charindexcount, index_pos):
    if index_pos in charindexcount.keys():
        charindexcount[index_pos] += 1
    else:
        charindexcount[index_pos] = 1


def increment_char_index_first(charindexorig, index_pos, text_id, galedata):
    if index_pos in charindexorig.keys():
        this_pub_year = galedata[text_id][0]['publication_year']
        old_pub_year = galedata[
                charindexorig[index_pos]][0]['publication_year']
        # change id if new id has smaller publication year
        if this_pub_year < old_pub_year:
            charindexorig[index_pos] = text_id
    else:
        charindexorig[index_pos] = text_id


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


def get_text_not_in_fragments(fragments_other_author, item_text,
                              text_type='raw'):
    is_original = {}
    for index in range(0, len(item_text)):
        is_original[index] = True
    for fragment in fragments_other_author:
        if text_type == 'raw':
            for index in range(fragment['text_start_primary'],
                               fragment['text_end_primary']):
                is_original[index] = False
        elif text_type == 'api':
            for index in range(fragment['api_text_start_primary'],
                               fragment['api_text_end_primary']):
                is_original[index] = False
    return is_original


def get_raw_ecco_text(text_id, ecco_by_id):
    raw_text_path = ecco_by_id[text_id][0]['path'] + "/" + text_id + ".txt"
    with open(raw_text_path, 'r') as txtfile:
        raw_text = txtfile.read()
    return raw_text


def enrich_fragment_data(orig_id, fragments, index_offsets, galeitems_by_id):
    this_index_offsets = index_offsets[orig_id]
    for item in fragments:
        item_start = get_correct_index(
            item['text_start_primary'], this_index_offsets)
        item_end = get_correct_index(
            item['text_end_primary'], this_index_offsets)
        item['text'] = api_text[
            item_start['index']:item_end['index'] + 1]
        item['api_id_primary'] = get_unified_id(item['id_primary'])
        item['api_id_secondary'] = get_unified_id(item['id_secondary'])
        item['api_text_start_primary'] = item_start['index']
        item['api_text_end_primary'] = item_end['index']
        item['api_start_header_primary'] = item_start['header']
        item['api_end_header_primary'] = item_end['header']
        item['author_secondary'] = galeitems_by_id[
            item['api_id_secondary']][0]['author']
        item['publication_year_secondary'] = galeitems_by_id[
            item['api_id_secondary']][0]['publication_year']
        item['publication_year_primary'] = galeitems_by_id[
            item['api_id_primary']][0]['publication_year']
        item['title_secondary'] = galeitems_by_id[
            item['api_id_secondary']][0]['title']
        item['work_id_secondary'] = galeitems_by_id[
            item['api_id_secondary']][0]['work_id']
        # item['id_primary'] = orig_id
        item['author_primary'] = galeitems_by_id[orig_id][0]['author']
        item['work_id_primary'] = galeitems_by_id[orig_id][0]['work_id']


workdata = read_csv_to_dictlist("../data/raw/estc_works_roles.csv")

for item in workdata:
    item['estc_id'] = get_long_estc_id(
        item['system_control_number'].split(')')[-1])
    item['work_id'] = item['finalWorkField']
workdata_by_id = list_to_dict_by_key(workdata, 'estc_id')

galeitems = read_csv_to_dictlist(
    "../data/work/idpairs_rich_ecco_eebo_estc.csv")

for item in galeitems:
    if item['estc_id'] in workdata_by_id.keys():
        item['work_id'] = workdata_by_id[item['estc_id']][0]['work_id']
    else:
        item['work_id'] = None
    if item['author'] == "":
        item['author'] = None
    item['publication_year'] = int(item['publication_year'])
    item['text_length'] = int(item['text_length'])

galeitems_by_id = list_to_dict_by_key(galeitems, 'document_id')

ecco_api_client = OctavoEccoClient()
eccoindex = read_csv_to_dictlist("../data/work/ecco_dict.csv")
eeboindex = read_csv_to_dictlist("../data/work/eebo_dict.csv")
eebo_by_id = list_to_dict_by_key(eeboindex, 'id')
ecco_by_id = list_to_dict_by_key(eccoindex, 'id')


# main loop

jsonfiles = get_datafiles("../data/work/hackathon/")
outputfolder = "../data/work/bayle_results/"

# all_data = {}

for jsonfileloc in jsonfiles:
    with open(jsonfileloc, 'r') as jsonfile:
        jsondata = json.load(jsonfile)

    all_data = jsondata
    # all_data[jsondata[0]['id_primary']] = jsondata

    # orig_id = "0825500201"
    orig_id = jsondata[0]['id_primary']
    outfile_prefix = jsonfileloc.split("/")[-1].split(".")[0]

    api_text = ecco_api_client.get_text_for_document_id(orig_id)['text']
    raw_text = get_raw_ecco_text(orig_id, ecco_by_id)

    indexdataloc = '../data/work/tr_offset_index.json'
    with open(indexdataloc, 'r', encoding='utf-8') as f:
        temp_indexdata = json.load(f)
        indexdata = {}
        for key, value in temp_indexdata.items():
            item_indices = {}
            for index, items in value.items():
                item_indices[int(index)] = items
            indexdata[key] = item_indices

    enrich_fragment_data(orig_id, all_data, indexdata, galeitems_by_id)

    all_data_filtered_later = []
    all_data_filtered_earlier = []
    all_data_filtered_contemporary = []

    for item in all_data:
        if (item['publication_year_secondary'] > item['publication_year_primary']):
            all_data_filtered_later.append(item)
        if (item['publication_year_secondary'] < item['publication_year_primary']):
            all_data_filtered_earlier.append(item)
        if (item['publication_year_secondary'] ==
                item['publication_year_primary']):
            all_data_filtered_contemporary.append(item)

    save_outputdata_csv(
        all_data_filtered_later,
        outputfolder + outfile_prefix + "_later_all.csv")

    # earlier. looks for just first instances
    charindex_orig = OrderedDict()

    for item in all_data_filtered_earlier:
        other_id = get_unified_id(item['id_secondary'])
        start_i = int(item['text_start_primary'])
        end_i = int(item['text_end_primary'])
        for index_pos in range(start_i, end_i + 1):
            increment_char_index_first(
                charindex_orig, index_pos, other_id, galeitems_by_id)

    charindex_orig = OrderedDict(sorted(charindex_orig.items()))

    fragments_earlier = []
    this_index = None
    segment_start_index = None
    this_id = None
    write_segment = False

    for k, v in charindex_orig.items():
        if this_index is None:
            this_index = k
            segment_start_index = this_index
            this_id = v
            continue
        new_index = k
        new_id = v
        if new_index != (1 + this_index):
            write_segment = True
        if new_id != this_id:
            write_segment = True
        if write_segment:
            fragments_earlier.append({
                'text_start_primary': segment_start_index,
                'text_end_primary': this_index,
                'id_secondary': this_id,
                'id_primary': orig_id})
            this_id = new_id
            segment_start_index = new_index
            write_segment = False
        this_index = new_index

    fragments_earlier.append({
        'text_start_primary': segment_start_index,
        'text_end_primary': this_index,
        'id_secondary': this_id,
        'id_primary': orig_id})

    enrich_fragment_data(
        orig_id, fragments_earlier, indexdata, galeitems_by_id)

    filtered_fragments_other_author_earlier = []
    for fragment in fragments_earlier:
        if fragment['author_primary'] == fragment['author_secondary']:
            continue
        if fragment['work_id_primary'] == fragment['work_id_secondary']:
            continue
        filtered_fragments_other_author_earlier.append(fragment)

    original_parts = get_text_not_in_fragments(
        filtered_fragments_other_author_earlier, raw_text, text_type='raw')

    # fragments after:
    # * all parts originally of Bayle,
    # * in fragments not by Bayle

    originally_bayle_indices = set()
    originally_not_bayle_indices = set()
    for key, value in original_parts.items():
        if value is True:
            originally_bayle_indices.add(key)
        else:
            originally_not_bayle_indices.add(key)

    fragments_later_orig_bayle_now_not_bayle = []

    for fragment in all_data_filtered_later:
        has_original_bayle = False
        chars_not_orig_bayle_count = 0
        fragment_len = len(range(int(fragment['text_start_primary']),
                           int(fragment['text_end_primary'])))
        for index in range(int(fragment['text_start_primary']),
                           int(fragment['text_end_primary'])):
            if index in originally_bayle_indices:
                has_original_bayle = True
            if index in originally_not_bayle_indices:
                chars_not_orig_bayle_count += 1
        orig_bayle_percentage = round(
            (fragment_len - chars_not_orig_bayle_count) / fragment_len, 2)
        if orig_bayle_percentage < 0.1:
            continue
        if has_original_bayle:
            fragment['original_author_percentage'] = orig_bayle_percentage
            fragments_later_orig_bayle_now_not_bayle.append(
                fragment)

    results_original_author_share = len(
        originally_bayle_indices) / len(raw_text)

    filtered_fragments_work = []
    for fragment in fragments_earlier:
        if fragment['work_id_primary'] == fragment['work_id_secondary']:
            continue
        filtered_fragments_work.append(fragment)

    save_outputdata_csv(
        fragments_later_orig_bayle_now_not_bayle,
        outputfolder + outfile_prefix + "_later_first_instance_author.csv")
    save_outputdata_csv(
        fragments_earlier,
        outputfolder + outfile_prefix + "_all_earlier.csv")
    save_outputdata_csv(
        filtered_fragments_other_author_earlier,
        outputfolder + outfile_prefix + "_earlier_other_author.csv")
    save_outputdata_csv(
        filtered_fragments_work,
        outputfolder + outfile_prefix + "_earlier_other_work.csv")

    # later. count reuses
    # discard same author
    later_charindex_count = OrderedDict()
    for index in range(0, len(api_text)):
        later_charindex_count[index] = 0

    this_indexdata = indexdata[orig_id]

    for item in all_data_filtered_later:
        other_id = get_unified_id(item['id_secondary'])
        this_id = get_unified_id(item['id_primary'])
        this_author = galeitems_by_id[this_id][0]['author']
        other_author = galeitems_by_id[other_id][0]['author']
        if this_author == other_author:
            continue
        start_i = get_correct_index(
            int(item['text_start_primary']), this_indexdata)['index']
        end_i = get_correct_index(
            int(item['text_end_primary']), this_indexdata)['index']
        for index_pos in range(start_i, end_i + 1):
            increment_char_index_count(later_charindex_count, index_pos)

    later_reuse_volume = []
    for key, value in later_charindex_count.items():
        later_reuse_volume.append({
            'index': key,
            'reuse_count': value
            })

    save_outputdata_csv(
        later_reuse_volume,
        outputfolder + outfile_prefix + "_later_reuse_counts.csv")
    print("Results written for: " + outfile_prefix)
