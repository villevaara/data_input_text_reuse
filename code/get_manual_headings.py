from lib.utils_common import read_csv_to_dictlist
from lib.helpers import list_to_dict_by_key
from lib.tr_analysis_helpers import get_unified_id
from lib.estc_data_helpers import get_long_estc_id
from lib.octavo_api_client import OctavoEccoClient
import os
import csv
import json
from collections import OrderedDict


# workdata = read_csv_to_dictlist("../data/raw/estc_works_roles.csv")

# for item in workdata:
#     item['estc_id'] = get_long_estc_id(
#         item['system_control_number'].split(')')[-1])
#     item['work_id'] = item['finalWorkField']
# workdata_by_id = list_to_dict_by_key(workdata, 'estc_id')

# galeitems = read_csv_to_dictlist(
#     "../data/work/idpairs_rich_ecco_eebo_estc.csv")

# for item in galeitems:
#     if item['estc_id'] in workdata_by_id.keys():
#         item['work_id'] = workdata_by_id[item['estc_id']][0]['work_id']
#     else:
#         item['work_id'] = None
#     if item['author'] == "":
#         item['author'] = None
#     item['publication_year'] = int(item['publication_year'])
#     item['text_length'] = int(item['text_length'])

# galeitems_by_id = list_to_dict_by_key(galeitems, 'document_id')

ecco_api_client = OctavoEccoClient()
# eccoindex = read_csv_to_dictlist("../data/work/ecco_dict.csv")
# eeboindex = read_csv_to_dictlist("../data/work/eebo_dict.csv")
# eebo_by_id = list_to_dict_by_key(eeboindex, 'id')
# ecco_by_id = list_to_dict_by_key(eccoindex, 'id')

orig_id = "0824900102"
# outfile_prefix = jsonfileloc.split("/")[-1].split(".")[0]

api_text = ecco_api_client.get_text_for_document_id(orig_id)['text']
# raw_text = get_raw_ecco_text(orig_id, ecco_by_id)

heading_csv = "../data/work/bayle_labels/bd1734a_2_headings.csv"
headings = read_csv_to_dictlist(heading_csv)

