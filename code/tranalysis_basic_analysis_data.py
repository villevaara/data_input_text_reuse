from lib.utils_common import read_csv_to_dictlist
from lib.helpers import list_to_dict_by_key
from lib.estc_data_helpers import get_long_estc_id
import os
import csv


def get_datafiles(rootdir="../data/work/tr_metadata/"):
    dirfiles = os.listdir(rootdir)
    datafiles = []
    for file in dirfiles:
        if file.endswith('.csv'):
            datafiles.append(rootdir + "/" + file)
    return datafiles


def save_outputdata_csv(outputdata, outputfname):
    with open(outputfname, 'w') as outcsv:
        fieldnames = outputdata[0].keys()
        writer = csv.DictWriter(outcsv, fieldnames=fieldnames)
        writer.writeheader()
        for row in outputdata:
            writer.writerow(row)


def set_item_connection(
        item, connection, filter_author, filter_work,
        item_id_field="text1_id"):
    this_pubyear = item['publication_year']
    this_author = item['author']
    this_work_id = item['work_id']
    other_item_year = galeitems_by_id[
        connection[item_id_field]][0]['publication_year']
    other_author = galeitems_by_id[
        connection[item_id_field]][0]['author']
    other_work_id = galeitems_by_id[
        connection[item_id_field]][0]['work_id']
    if this_author is not None:
        if filter_author == "discard_same":
            if this_author == other_author:
                return None
        if filter_author == "only_keep_same":
            if this_author != other_author:
                return None
    if this_work_id is not None:
        if filter_work == "discard_same":
            if this_work_id == other_work_id:
                return None
    if this_pubyear < other_item_year:
        item['earlier_frag'] += connection['fragments']
        item['earlier_conn'] += 1
        item['earlier_char'] += connection['total_length']
    if this_pubyear > other_item_year:
        item['later_frag'] += connection['fragments']
        item['later_conn'] += 1
        item['later_char'] += connection['total_length']
    if this_pubyear == other_item_year:
        item['parallel_frag'] += connection['fragments']
        item['parallel_conn'] += 1
        item['parallel_char'] += connection['total_length']


def get_galeitem_results(
        item, galeitems_by_id, all_conn_by_id1, all_conn_by_id2,
        filter_author="nothing", filter_work="nothing"):
    if item['document_id'] in all_conn_by_id1.keys():
        for connection in all_conn_by_id1[item['document_id']]:
            set_item_connection(
                item, connection, filter_author, filter_work,
                item_id_field="text2_id")
    if item['document_id'] in all_conn_by_id2.keys():
        for connection in all_conn_by_id2[item['document_id']]:
            set_item_connection(
                item, connection, filter_author, filter_work,
                item_id_field="text1_id")
    item['earlier_char_by_length'] = item['earlier_char'] / item['text_length']
    item['later_char_by_length'] = item['later_char'] / item['text_length']
    item['parallel_char_by_length'] = (
        item['parallel_char'] / item['text_length'])
    return item


galeitems = read_csv_to_dictlist(
    "../data/work/idpairs_rich_ecco_eebo_estc.csv")

workdata = read_csv_to_dictlist("../data/raw/estc_works_roles.csv")
for item in workdata:
    item['estc_id'] = get_long_estc_id(
        item['system_control_number'].split(')')[-1])
    item['work_id'] = item['finalWorkField']
workdata_by_id = list_to_dict_by_key(workdata, 'estc_id')

for item in galeitems:
    # set work_ids
    if item['estc_id'] in workdata_by_id.keys():
        item['work_id'] = workdata_by_id[item['estc_id']][0]['work_id']
    else:
        item['work_id'] = None
    # set author data
    if item['author'] == "":
        item['author'] = None
    # rest
    item['publication_year'] = int(item['publication_year'])
    item['text_length'] = int(item['text_length'])
    resitem = {
        'earlier_frag': 0,
        'earlier_conn': 0,
        'earlier_char': 0,
        'earlier_char_by_length': 0.0,
        'later_frag': 0,
        'later_conn': 0,
        'later_char': 0,
        'later_char_by_length': 0.0,
        'parallel_frag': 0,
        'parallel_conn': 0,
        'parallel_char': 0,
        'parallel_char_by_length': 0.0,
    }
    for key, value in resitem.items():
        item[key] = value


galeitems_by_id = list_to_dict_by_key(galeitems, 'document_id')
conn_datafiles = get_datafiles()
# conn_datafiles = conn_datafiles[0:3]


for conn_f in conn_datafiles:
    print("Processing: " + conn_f)
    this_conn = read_csv_to_dictlist(conn_f)
    for item in this_conn:
        item['total_length'] = int(item['total_length'])
        item['fragments'] = int(item['fragments'])
    all_conn_by_id1 = list_to_dict_by_key(this_conn, 'text1_id')
    all_conn_by_id2 = list_to_dict_by_key(this_conn, 'text2_id')
    for item in galeitems:
        item = get_galeitem_results(
            item, galeitems_by_id, all_conn_by_id1, all_conn_by_id2,
            filter_author="discard_same", filter_work="discard_same")

outcsv = "../data/work/trstats_same_author_filtered_same_work_filtered.csv"
save_outputdata_csv(galeitems, outcsv)
print("Wrote: " + outcsv)
