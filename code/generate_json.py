from text_encoder import TextEncoder
from lib.helpers import create_dir_if_not_exists
import json
import lib.blast_datareader as blastdr
import csv
import os
import ecco_index
import shutil
from os import listdir
import argparse
import tarfile
from io import StringIO
from io import BytesIO
from datetime import datetime
from datetime import timedelta
from time import time
from collections import OrderedDict


def read_txt(text_file_loc):
    with open(text_file_loc, 'r', encoding='utf-8') as textfile:
        text = textfile.read()
        return text


def get_text_id_index(csv_location,
                      collection_type,
                      re_create=False):
    ecco_dict = {}
    if os.path.exists(csv_location) and not re_create:
        print("using existing .csv")
        with open(csv_location, 'r') as eccocsv:
            reader = csv.DictReader(eccocsv)
            for row in reader:
                ecco_dict[row['id']] = row['path']
    else:
        print("getting text-id index on the fly")
        if collection_type == "ecco":
            collection_list = ecco_index.get_ecco_dict()
        elif collection_type == "eebo":
            collection_list = ecco_index.get_eebo_dict()
        else:
            exit("Invalid collection type: " + collection_type)
        for row in collection_list:
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


def get_text_for_doc_id(doc_id, ecco_id_index, eebo_id_index):
    # if ecco_id_dict is None:
    #     ecco_id_dict = get_ecco_id_dict()
    if get_doc_id_collection(doc_id) == "eebo":
        # docpath = (
        #     "../data/raw/eebotxt/eebo_phase1/" + doc_id[:2] + "/" + doc_id)
        docpath = eebo_id_index[doc_id.split(".")[0]]
        return read_txt(docpath + "/" + doc_id + ".txt")
    elif get_doc_id_collection(doc_id) == "ecco":
        docpath = ecco_id_index[doc_id]
        return read_txt(docpath + "/" + doc_id + ".txt")
    # textdata_list = get_dirdata(docpath)
    # return textdata_list


class BlastPair():
    def __init__(self, blastdata, ecco_id_dict=None, eebo_id_dict=None,
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
        self.eebo_id_dict = eebo_id_dict
        self.textenc = textenc

    def set_correct_indices_and_texts(
            self, source_text=None, target_text=None):
        if source_text is None:
            source_text = get_text_for_doc_id(
                self.source_id,
                self.ecco_id_dict, self.eebo_id_dict)
        if target_text is None:
            target_text = get_text_for_doc_id(
                self.target_id,
                self.ecco_id_dict, self.eebo_id_dict)
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
            'text1_id': self.source_id,
            'text1_text_start': self.source_text_start,
            'text1_text_end': self.source_text_end,
            'text1_text': self.source_text,
            'text2_id': self.target_id,
            'text2_text_start': self.target_text_start,
            'text2_text_end': self.target_text_end,
            'text2_text': self.target_text,
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


class BlastPair2():
    def __init__(self, blastdata,
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
        self.textenc = textenc

    def set_correct_indices_and_texts(
            self, inmemtxtdata, source_text=None, target_text=None):
        if source_text is None:
            source_text = inmemtxtdata.get_txt_id_text(self.source_id)
        if target_text is None:
            target_text = inmemtxtdata.get_txt_id_text(self.target_id)
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
            'text1_id': self.source_id,
            'text1_text_start': self.source_text_start,
            'text1_text_end': self.source_text_end,
            'text1_text': self.source_text,
            'text2_id': self.target_id,
            'text2_text_start': self.target_text_start,
            'text2_text_end': self.target_text_end,
            'text2_text': self.target_text,
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


def get_out_json_fname(input_fname, fname_prefix):
    out_fname = (fname_prefix + "_" +
                 input_fname.split("/")[-1].split(".")[0] + ".json")
    return out_fname


def get_frag_json_fname(input_fname):
    out_fname = "frags_" + input_fname.split("/")[-1].split(".")[0] + ".json"
    return out_fname


def write_batch_json(input_fname, fname_prefix, ecco_id_dict, eebo_id_dict,
                     outputdir="../output/blast_batches/"):
    textenc = TextEncoder("eng")
    outjson = get_out_json_fname(input_fname, fname_prefix)
    batchdata = blastdr.read_blast_cluster_csv(input_fname)
    i = 0
    max_i = len(batchdata)
    outdata = []
    for item in batchdata:
        i += 1
        if i % 100 == 0:
            print(outjson + " " + str(i) + "/" + str(max_i))
        blastpair = BlastPair(item, ecco_id_dict, eebo_id_dict, textenc)
        blastpair.set_correct_indices_and_texts()
        outdata.append(blastpair.get_outdict())
    outfile = outputdir + outjson
    with open(outfile, 'w', encoding='utf-8') as jsonout:
        json.dump(outdata, jsonout, indent=2, ensure_ascii=False)
    return outfile


def get_batch_jsondata(input_data, ecco_id_dict, eebo_id_dict):
    textenc = TextEncoder("eng")
    batchdata = blastdr.read_blast_cluster_csv_inmem(input_data)
    i = 0
    max_i = len(batchdata)
    outdata = []
    for item in batchdata:
        i += 1
        if i % 100 == 0:
            print("   --- " + str(i) + "/" + str(max_i))
        blastpair = BlastPair(item, ecco_id_dict, eebo_id_dict, textenc)
        blastpair.set_correct_indices_and_texts()
        outdata.append(blastpair.get_outdict())
    return outdata


def get_batch_jsondata_inmem(input_data, inmemtxtdata):
    textenc = TextEncoder("eng")
    batchdata = blastdr.read_blast_cluster_csv_inmem(input_data)
    i = 0
    max_i = len(batchdata)
    outdata = []
    for item in batchdata:
        i += 1
        if i % 100 == 0:
            print("   --- " + str(i) + "/" + str(max_i))
        blastpair = BlastPair2(item, textenc)
        blastpair.set_correct_indices_and_texts(inmemtxtdata)
        outdata.append(blastpair.get_outdict())
    return outdata

# def process_batch_files(batchdir, outputdir, ecco_dict_csv, eebo_dict_csv):
#     ecco_id_dict = get_text_id_index(ecco_dict_csv, "ecco")
#     eebo_id_dict = get_text_id_index(eebo_dict_csv, "eebo")
#     blastdr.extract_tar_datafiles(batchdir)
#     batch_files = blastdr.get_blast_data_filenames(batchdir)
#     for batch_file in batch_files:
#         write_batch_json(batch_file, ecco_id_dict, eebo_id_dict,
#                          outputdir)


def process_batch_files2(batchdir, outputdir,
                         thisiter, ecco_dict_csv, eebo_dict_csv):
    ecco_id_dict = get_text_id_index(ecco_dict_csv, "ecco")
    eebo_id_dict = get_text_id_index(eebo_dict_csv, "eebo")
    # tarfiles = blastdr.get_tar_datafiles(batchdir)
    thistar = batchdir + "/iter_" + str(thisiter) + ".tar.gz"
    # for tarfilepath in tarfiles:
    temp_batchdir = blastdr.extract_single_tar_datafiles(thistar)
    batch_files = blastdr.get_blast_data_filenames(temp_batchdir)
    fname_prefix = thistar.split("/")[-1].split(".")[0]
    ready_files = []
    for batch_file in batch_files:
        ready_json = write_batch_json(batch_file, fname_prefix,
                                      ecco_id_dict, eebo_id_dict, outputdir)
        ready_files.append(ready_json)
    tarfile_out = tarfile.open(
        outputdir + "/iter_" + str(thisiter) + ".tar.gz", 'w:gz')
    for filename in ready_files:
        tararc = filename.split("/")[-1]
        tarfile_out.add(filename, arcname=tararc)
    tarfile_out.close()
    for filename in ready_files:
        os.remove(filename)
    shutil.rmtree(temp_batchdir)


class InMemTxtData:
    def __init__(self, ecco_dict_csv, eebo_dict_csv, max_size=100000,
                 load_all=False):
        self.max_size = max_size
        # self.prune_size = int(self.max_size) + 1
        self.ecco_id_index = get_text_id_index(ecco_dict_csv, "ecco")
        self.eebo_id_index = get_text_id_index(eebo_dict_csv, "eebo")
        self.txt_data = OrderedDict()
        if load_all:
            self.read_all()

    def read_all(self):
        self.txt_data = OrderedDict()
        for item in (self.ecco_id_index + self.eebo_id_index):
            self.add_txt_data(item['id'])

    def add_txt_data(self, doc_id):
        this_text = get_text_for_doc_id(
            doc_id, self.ecco_id_index, self.eebo_id_index)
        self.txt_data[doc_id] = this_text
        if not self.max_size == -1:
            if len(self.txt_data) > self.max_size:
                self.txt_data.popitem(next(reversed(self.txt_data)))

    def get_txt_id_text(self, doc_id):
        if doc_id in self.txt_data.keys():
            self.txt_data.move_to_end(doc_id, last=False)
        else:
            self.add_txt_data(doc_id)
        return self.txt_data[doc_id]

    def get_inmem_size(self):
        return len(self.txt_data)


def process_batch_files3(batchdir, outputdir,
                         thisiter, ecco_dict_csv, eebo_dict_csv):
    inmemtxtdata = InMemTxtData(ecco_dict_csv, eebo_dict_csv, max_size=100000)
    thistar = batchdir + "/iter_" + str(thisiter) + ".tar.gz"
    batch_data = blastdr.get_single_tar_contents(thistar)
    all_ready_data = []
    i = 0
    max_i = len(batch_data)
    for batch_item in batch_data:
        i += 1
        print("Processing: item " + str(i) + "/" + str(max_i))
        ready_data = get_batch_jsondata_inmem(batch_item, inmemtxtdata)
        all_ready_data.extend(ready_data)
        print("inmem: " + str(inmemtxtdata.get_inmem_size()))
    tarfile_out = tarfile.open(
        outputdir + "/iter_" + str(thisiter) + "_out.tar.gz", 'w:gz')
    s = StringIO()
    writethis = json.dumps(all_ready_data, indent=2, ensure_ascii=False)
    s.write(writethis)
    s.seek(0)
    tarinfo = tarfile.TarInfo(
        name="iter_" + str(thisiter) + ".json")
    tarinfo.size = len(s.getvalue().encode('utf-8'))
    tarfile_out.addfile(
        tarinfo=tarinfo, fileobj=BytesIO(s.getvalue().encode('utf-8')))
    tarfile_out.close()


parser = argparse.ArgumentParser(description="Decode indices back to text.")
parser.add_argument("--datadir", help="Input datadir", required=True)
parser.add_argument("--outdir", help="Output datadir", required=True)
parser.add_argument(
    "--iter", help="Iteration to process", type=int, required=True)
args = parser.parse_args()


thisiter = args.iter
inputdir = args.datadir + "/"
outputdir = args.outdir + "/"

create_dir_if_not_exists(outputdir)

ecco_dict_csv = "../data/work/ecco_dict.csv"
eebo_dict_csv = "../data/work/eebo_dict.csv"
# process_batch_files2(inputdir, outputdir,
#                      thisiter, ecco_dict_csv, eebo_dict_csv)
print("start:", datetime.now())
start_time = time()
process_batch_files3(inputdir, outputdir,
                     thisiter, ecco_dict_csv, eebo_dict_csv)
print("end:", datetime.now())
print("elapsed:", str(timedelta(seconds=(int(time()-start_time)))))
