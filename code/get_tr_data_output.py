from lib.blast_datareader import get_single_tar_json_contents
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


parser = argparse.ArgumentParser(
    description="Fetch single ESTC id text reuse data.")
parser.add_argument("--indexfile", help="Index file location",
                    default="../data/work/text_index.json")
parser.add_argument("--id", help="ESTC / EEBO id of interest", required=True)
parser.add_argument("--outputfile", help="Output file location",
                    default="../data/work/single_tr_data.json")
args = parser.parse_args()


index_datafile = args.indexfile
estc_id_to_get = args.id
outputfname = args.outputfile


with open(index_datafile, 'r') as jsondata:
    data_index = json.load(jsondata)


if estc_id_to_get not in data_index.keys():
    sys.exit("ID not found in text reuse data.")

files_of_interest = data_index[estc_id_to_get]

data_of_interest = {}
for filename in files_of_interest:
    print("Reading: " + filename)
    tar_cont = get_single_tar_json_contents(filename)
    data_of_interest[filename] = (tar_cont[0])


outputdata = []
for key, value in data_of_interest.items():
    for item in value:
        data_found = False
        if item['text1_id'] == estc_id_to_get:
            item_position = "1"
            data_found = True
        elif item['text2_id'] == estc_id_to_get:
            item_position = "2"
            data_found = True
        if data_found:
            outputrow = get_output_row(item, item_position, key)
            outputdata.append(outputrow)

save_outputdata_json(outputdata, outputfname)
