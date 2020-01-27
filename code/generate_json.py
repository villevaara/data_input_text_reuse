from text_encoder import TextEncoder
# from create_blast_input_json import get_text_for_doc_id, get_ecco_id_dict
from lib.helpers import create_dir_if_not_exists
import json
import lib.blast_datareader as blastdr
import sys
import csv
import os
import ecco_index
from os import listdir


def read_txt(text_file_loc):
    with open(text_file_loc) as textfile:
        text = textfile.read()
        return text


def get_ecco_id_dict(re_create=False,
                     csv_location='../data/work/ecco_dict.csv'):
    ecco_dict = {}
    if os.path.exists(csv_location) and not re_create:
        print("using existing ecco_dict.csv")
        with open(csv_location, 'r') as eccocsv:
            reader = csv.DictReader(eccocsv)
            for row in reader:
                ecco_dict[row['id']] = row['path']
    else:
        print("getting ecco dict on the fly")
        ecco_list = ecco_index.get_ecco_dict()
        for row in ecco_list:
            ecco_dict[row['id']] = row['path']
    return ecco_dict


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


class BlastPair():
    def __init__(self, blastdata, ecco_id_dict=None,
                 textenc=TextEncoder("eng")):
        self.source_id = blastdata['source_id']
        self.source_text_start = None
        self.source_text_end = None
        self.source_start_blast = blastdata['source_start_blast']
        self.source_end_blast = blastdata['source_end_blast']
        self.source_text = None
        self.target_id = blastdata['target_id']
        self.target_text_start = None
        self.target_text_end = None
        self.target_start_blast = blastdata['target_start_blast']
        self.target_end_blast = blastdata['target_end_blast']
        self.target_text = None
        self.align_length = blastdata['align_length']
        self.positives_percent = blastdata['positives_percent']
        self.ecco_id_dict = ecco_id_dict
        self.textenc = textenc

    def set_correct_indices_and_texts(
            self, source_text=None, target_text=None):
        if source_text is None:
            source_text = get_text_for_doc_id(
                self.source_id, self.ecco_id_dict)[0]['text']
        if target_text is None:
            target_text = get_text_for_doc_id(
                self.target_id, self.ecco_id_dict)[0]['text']
        # textenc = TextEncoder("eng")
        sourcedata = self.textenc.decode_text(
            source_text, self.source_start_blast, self.source_end_blast)
        targetdata = self.textenc.decode_text(
            target_text, self.target_start_blast, self.target_end_blast)
        self.source_text = sourcedata[0]
        self.source_text_start = sourcedata[1][0]
        self.source_text_end = sourcedata[1][1]
        self.target_text = targetdata[0]
        self.target_text_start = targetdata[1][0]
        self.target_text_end = targetdata[1][1]

    def get_outdict(self):
        return {
            'source_id': self.source_id,
            'source_text_start': self.source_text_start,
            'source_text_end': self.source_text_end,
            'source_text': self.source_text,
            'target_id': self.target_id,
            'target_text_start': self.target_text_start,
            'target_text_end': self.target_text_end,
            'target_text': self.target_text,
            'align_length': self.align_length,
            'positives_percent': self.positives_percent
        }

    def get_fragdata(self, id_distinct_part):
        return [
            {'documentID': self.source_id,
             'fragmentID': id_distinct_part,
             'text': self.source_text,
             'startIndex': self.source_text_start,
             'endIndex': self.source_text_end,
             'align_length': self.align_length,
             'positives_percent': self.positives_percent
             },
            {'documentID': self.target_id,
             'fragmentID': id_distinct_part,
             'text': self.target_text,
             'startIndex': self.target_text_start,
             'endIndex': self.target_text_end,
             'align_length': self.align_length,
             'positives_percent': self.positives_percent
             }
        ]


def get_out_json_fname(input_fname):
    out_fname = input_fname.split("/")[-1].split(".")[0] + ".json"
    return out_fname


def get_frag_json_fname(input_fname):
    out_fname = "frags_" + input_fname.split("/")[-1].split(".")[0] + ".json"
    return out_fname


def write_batch_json(input_fname, ecco_id_dict,
                     outputdir="../output/blast_batches/"):
    textenc = TextEncoder("eng")
    outjson = get_out_json_fname(input_fname)
    batchdata = blastdr.read_blast_cluster_csv(input_fname)
    i = 0
    max_i = len(batchdata)
    blastpairs = []
    fragdata = []
    for item in batchdata:
        i += 1
        if i % 100 == 0:
            print(outjson + " " + str(i) + "/" + str(max_i))
        blastpair = BlastPair(item, ecco_id_dict, textenc)
        blastpair.set_correct_indices_and_texts()
        frag_id = (str(input_fname.split("/")[-1].split(".")[0].split(
                   "_")[-1]) + "_" + str(i))
        fragdata.extend(blastpair.get_fragdata(frag_id))
        blastpairs.append(blastpair)

    outdata = []
    for blastpair in blastpairs:
        outdata.append(blastpair.get_outdict())
    outdata = sorted(outdata, key=lambda k: k['source_text_start'])
    outfile = outputdir + outjson
    with open(outfile, 'w') as jsonout:
        json.dump(outdata, jsonout, indent=2)

    fragoutfile = outputdir + get_frag_json_fname(input_fname)
    with open(fragoutfile, 'w') as jsonout:
        json.dump(fragdata, jsonout, indent=2)


def process_batch_files(batchdir, outputdir):
    ecco_id_dict = get_ecco_id_dict()
    batch_files = blastdr.get_blast_data_filenames(batchdir)
    for batch_file in batch_files:
        write_batch_json(batch_file, ecco_id_dict,
                         outputdir)


def get_datadir():
    if len(sys.argv) == 1:
        sys.exit("Provide datadir.")
    elif len(sys.argv) == 2:
        return sys.argv[1]
    else:
        sys.exit("Too many command line args.")


# def create_batch_fragdata()

datadir = get_datadir()
inputdir = "../data/work/" + datadir + "/"
outputdir = "../output/" + datadir + "/"
create_dir_if_not_exists(outputdir)

process_batch_files(inputdir, outputdir)
