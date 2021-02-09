from lib.blast_datareader import get_single_tar_json_contents
from lib.helpers import create_dir_if_not_exists
import json
import sys
import csv
import argparse


def get_output_row(tr_instance, pri_pos, fname):
    if pri_pos == "1":
        sec_pos = "2"
    else:
        sec_pos = "1"
    #
    return {
        'id_primary': tr_instance[('text' + pri_pos + "_id")],
        'text_start_primary': tr_instance[('text' + pri_pos + '_text_start')],
        'text_end_primary': tr_instance[('text' + pri_pos + '_text_end')],
        'text_primary': tr_instance[('text' + pri_pos + '_text')],
        'id_secondary': tr_instance[('text' + sec_pos + '_id')],
        'text_start_secondary': tr_instance[
            ('text' + sec_pos + '_text_start')],
        'text_end_secondary': tr_instance[('text' + sec_pos + '_text_end')],
        'text_secondary': tr_instance[('text' + sec_pos + '_text')],
        'length': tr_instance['align_length'],
        'positives_percent': tr_instance['positives_percent'],
        'file': fname
    }


def save_outputdata_csv(outputdata, outputfname):
    with open(outputfname, 'w') as outcsv:
        fieldnames = outputdata[0].keys()
        writer = csv.DictWriter(outcsv, fieldnames=fieldnames)
        writer.writeheader()
        for row in outputdata:
            writer.writerow(row)


def save_outputdata_json(outputdata, outputfname):
    with open(outputfname, 'w') as outjson:
        json.dump(outputdata, outjson, ensure_ascii=False,
                  indent=4)
    print("Wrote output at: " + outputfname)


def get_single_input_output(input_filename):
    outputdata = []
    tar_cont = get_single_tar_json_contents(filename)
    for item in tar_cont:
        data_found = False
        if item['text1_id'] == estc_id_to_get:
            item_position = "1"
            data_found = True
        elif item['text2_id'] == estc_id_to_get:
            item_position = "2"
            data_found = True
        if data_found:
            fname = input_filename.split("/")[-1]
            outputrow = get_output_row(item, item_position, fname)
            outputdata.append(outputrow)
    return outputdata


parser = argparse.ArgumentParser(
    description="Fetch single ESTC id text reuse data.")
parser.add_argument("--indexfile", help="Index file location",
                    default="../data/work/text_index.json")
parser.add_argument("--id", help="ESTC / EEBO id of interest", required=True)
parser.add_argument("--outputpath", help="Output files location",
                    default="../data/work/tr_output")
args = parser.parse_args()


index_datafile = args.indexfile
estc_id_to_get = args.id
outputpath = args.outputpath

create_dir_if_not_exists(outputpath)

with open(index_datafile, 'r') as jsondata:
    data_index = json.load(jsondata)


if estc_id_to_get not in data_index.keys():
    sys.exit("ID not found in text reuse data.")

files_of_interest = data_index[estc_id_to_get]

i = 0
max_i = len(files_of_interest)
for filename in files_of_interest:
    i += 1
    print("Reading " + str(i) + "/" + str(max_i) + " : " + filename)
    outputdata = get_single_input_output(filename)
    if len(outputdata) > 0:
        outputfname = (
            outputpath + "/" + filename.split("/")[-1].split(".")[0])
        save_outputdata_json(outputdata, outputfname)
