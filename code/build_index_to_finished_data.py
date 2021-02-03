from lib.blast_datareader import (
    get_single_tar_json_contents,
    get_tar_datafiles)
import json
import argparse


def add_to_text_index(text_index, item_id, tarfile_name):
    if item_id in text_index.keys():
        text_index[item_id].add(tarfile_name)
    else:
        text_index[item_id] = set([tarfile_name])


def process_tarfile(tarfile, text_index, logfile):
    print("Processing tarfile: " + tarfile)
    tar_cont = get_single_tar_json_contents(tarfile, logfile)
    if tar_cont is not None:
        for item in tar_cont:
            for sub_item in item:
                add_to_text_index(text_index, sub_item['text1_id'], tarfile)
                add_to_text_index(text_index, sub_item['text2_id'], tarfile)


def write_index(text_index, output_fname):
    print("Preparing text index for JSON.")
    for key, value in text_index.items():
        text_index[key] = list(value)
    print("Writing text index to: " + output_fname)
    with open(output_fname, 'w') as outf:
        json.dump(text_index, outf)


parser = argparse.ArgumentParser(
    description="Build text reuse index.")
parser.add_argument("--indexfile", help="Index file location",
                    default="../data/work/text_index.json")
parser.add_argument("--dataloc", help="Text reuse data location",
                    required=True)
parser.add_argument("--logfile", help="Logfile location",
                    default="../logs/text_index.log")
args = parser.parse_args()


# conf
tarfile_loc = args.dataloc
text_index_fname = args.indexfile
logfile = args.logfile

with open(logfile, 'w') as logtxt:
    logtxt.write("# Starting logfile for text index.\n")

# main
tarfiles = get_tar_datafiles(tarfile_loc)
text_index = {}

for tarfile in tarfiles:
    process_tarfile(tarfile, text_index, logfile)

write_index(text_index, text_index_fname)
