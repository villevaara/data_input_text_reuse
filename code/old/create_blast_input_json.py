import json
from os import listdir
import argparse
import csv
import os
import ecco_index


def get_input_ids(input_txt):
    with open(input_txt, 'r') as inputfile:
        lines = []
        for line in inputfile.readlines():
            line = line.strip("\n")
            if line != "":
                lines.append(line)
        return lines


def read_txt(text_file_loc):
    with open(text_file_loc) as textfile:
        text = textfile.read()
        return text


def get_dirdata(dirpath):
    resdict_list = []
    thisdir_files = listdir(dirpath)
    for filename in thisdir_files:
        doc_id = filename.replace(
            ".headed", "").replace(".txt", "")
        text_file_loc = dirpath + "/" + filename
        text = read_txt(text_file_loc)
        resdict_list.append({
            'doc_id': doc_id,
            'text': text
            })
    return resdict_list


def get_doc_id_collection(doc_id):
    if doc_id[:1] == "A" or doc_id[:1] == "B":
        return "eebo"
    else:
        return "ecco"


def get_text_for_doc_id(doc_id, ecco_id_dict=None):
    if ecco_id_dict is None:
        ecco_id_dict = get_ecco_id_dict()
    if get_doc_id_collection(doc_id) == "eebo":
        docpath = (
            "../data/raw/eebotxt/eebo_phase1/" + doc_id[:2] + "/" + doc_id)
    elif get_doc_id_collection(doc_id) == "ecco":
        docpath = ecco_id_dict[doc_id]
    textdata_list = get_dirdata(docpath)
    return textdata_list


# write Aleksi's crazy custom json format
def write_json_results(outdata, jsonfile):
    jsonstring = ""
    for entry in outdata:
        entry_json = json.dumps(entry) + "\n"
        jsonstring += entry_json
    with open(jsonfile, 'w') as jsonfile:
        jsonfile.write(jsonstring)


def get_ecco_id_dict(re_create=False,
                     csv_location='../data/work/ecco_dict.csv'):
    if os.path.exists(csv_location) and not re_create:
        print("using existing ecco_dict.csv")
        ecco_dict = {}
        with open(csv_location, 'r') as eccocsv:
            reader = csv.DictReader(eccocsv)
            for row in reader:
                ecco_dict[row['id']] = row['path']
    else:
        print("getting ecco dict on the fly")
        ecco_dict = ecco_index.get_ecco_dict()
    return ecco_dict


def get_dataset_for_doc_id_list(doc_id_list, ecco_id_dict=get_ecco_id_dict()):
    outdata = []
    for doc_id in doc_id_list:
        texts = get_text_for_doc_id(doc_id, ecco_id_dict)
        outdata.extend(texts)
    return outdata


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', '-i', help="a list if document ids",
                        type=str, default="doc_ids.txt")
    parser.add_argument('--output', '-o', help="name of json file", type=str,
                        default="doc_texts.json")
    args = parser.parse_args()
    print("input: " + "../data/work/" + args.input)
    input_ids = get_input_ids("../data/work/" + args.input)

    outdata = get_dataset_for_doc_id_list(input_ids)
    print("output: " + "../output/" + args.output)
    write_json_results(outdata, ("../output/" + args.output))


if __name__ == "__main__":
    main()
