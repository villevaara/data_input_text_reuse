from text_encoder import TextEncoder
from lib.helpers import create_dir_if_not_exists
import json
import lib.blast_datareader as blastdr
import csv
import os
import ecco_index
# import shutil
from os import listdir
import argparse
import tarfile
from io import StringIO
from io import BytesIO
from datetime import datetime
from datetime import timedelta
from time import time
from collections import OrderedDict
from multiprocessing import Pool


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
    if get_doc_id_collection(doc_id) == "eebo":
        docpath = eebo_id_index[doc_id.split(".")[0]]
        return read_txt(docpath + "/" + doc_id + ".txt")
    elif get_doc_id_collection(doc_id) == "ecco":
        docpath = ecco_id_index[doc_id]
        return read_txt(docpath + "/" + doc_id + ".txt")


class BlastPairMulti():
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
        blastpair = BlastPairMulti(item, textenc)
        blastpair.set_correct_indices_and_texts(inmemtxtdata)
        outdata.append(blastpair.get_outdict())
    return outdata


class InMemTxtData:
    def __init__(self, ecco_dict_csv, eebo_dict_csv, max_size=100000,
                 load_all=False):
        self.max_size = max_size
        self.ecco_id_index = get_text_id_index(ecco_dict_csv, "ecco")
        self.eebo_id_index = get_text_id_index(eebo_dict_csv, "eebo")
        self.txt_data = OrderedDict()
        self.load_all = load_all
        if load_all:
            self.read_all()

    def read_all(self):
        self.txt_data = OrderedDict()
        for item in (list(self.ecco_id_index.keys()) +
                     list(self.eebo_id_index.keys())):
            if self.max_size == -1:
                self.add_txt_data(item)
            else:
                self.add_txt_data(item)
                if self.get_inmem_size() == int(self.max_size * 0.5):
                    break

    def add_txt_data(self, doc_id):
        this_text = get_text_for_doc_id(
            doc_id, self.ecco_id_index, self.eebo_id_index)
        self.txt_data[doc_id] = this_text
        if not self.max_size == -1:
            if len(self.txt_data) > self.max_size:
                self.txt_data.popitem(next(reversed(self.txt_data)))

    def get_txt_id_text(self, doc_id):
        # If all text are already in memory, do not modify the dict.
        if not self.load_all or self.max_size != -1:
            if doc_id in self.txt_data.keys():
                self.txt_data.move_to_end(doc_id, last=False)
            else:
                self.add_txt_data(doc_id)
        return self.txt_data[doc_id]

    def get_inmem_size(self):
        return len(self.txt_data)


class DataProcessor:
    def __init__(self, inmemtxtdata):
        self.inmemtxtdata = inmemtxtdata

    def process_single_item(self, batch_item):
        ready_data = get_batch_jsondata_inmem(batch_item, self.inmemtxtdata)
        return ready_data


def process_batch_files_multi(batchdir, outputdir, inmemtxtdata,
                              this_iter, ecco_dict_csv, eebo_dict_csv):
    this_tar = batchdir + "/iter_" + str(this_iter) + ".tar.gz"
    batch_data = blastdr.get_single_tar_contents(this_tar)
    #
    pool = Pool()
    dataprocessor = DataProcessor(inmemtxtdata)
    ready_data = pool.map(dataprocessor.process_single_item, batch_data)
    #
    tarout_fname = outputdir + "/iter_" + str(this_iter) + "_out.tar.gz"
    tarfile_out = tarfile.open(tarout_fname, 'w:gz')
    s = StringIO()
    writethis = json.dumps(ready_data, indent=2, ensure_ascii=False)
    s.write(writethis)
    s.seek(0)
    tarinfo = tarfile.TarInfo(
        name="iter_" + str(this_iter) + ".json")
    tarinfo.size = len(s.getvalue().encode('utf-8'))
    tarfile_out.addfile(
        tarinfo=tarinfo, fileobj=BytesIO(s.getvalue().encode('utf-8')))
    tarfile_out.close()
    print("   --- wrote: " + tarout_fname)


parser = argparse.ArgumentParser(description="Decode indices back to text.")
parser.add_argument("--datadir", help="Input datadir", required=True)
parser.add_argument("--outdir", help="Output datadir", required=True)
parser.add_argument(
    "--iter", help="Iteration to process", type=int, required=True)
parser.add_argument(
    "--maxsize", help="Maximum number of source documents in memory.",
    type=int, default=100000)
parser.add_argument(
    "--loadall", help="Preload source texts into memory.",
    dest="load_all", action='store_true')
parser.add_argument(
    "--no-loadall", help="DO NOT preload source texts into memory.",
    dest="load_all", action='store_false')
parser.set_defaults(load_all=True)
args = parser.parse_args()

inputdir = args.datadir + "/"
outputdir = args.outdir + "/"
thisiter = args.iter
maxsize_inmem = args.maxsize
load_all = args.load_all

create_dir_if_not_exists(outputdir)

ecco_dict_csv = "../data/work/ecco_dict.csv"
eebo_dict_csv = "../data/work/eebo_dict.csv"

if thisiter == -1:
    all_tarfiles = blastdr.get_tar_datafiles(inputdir)
    all_iter = []
    for fname in all_tarfiles:
        fname_iter = int(fname.split("iter_")[-1].split(".")[0])
        all_iter.append(fname_iter)
else:
    all_iter = thisiter

print("start:", datetime.now())
start_time = time()

# ECCO = 61GB
# EEBO = 10GB

# setup progress logging.
processed_iters = []
processed_iters_txt = "../data/work/final_json_processed_iters.txt"
if os.path.isfile(processed_iters_txt):
    with open(processed_iters_txt, 'r') as pros_iters_f:
        items = pros_iters_f.readlines()
        for item in items:
            if item.isdigit():
                processed_iters.append(int(item))

inmemtxtdata = InMemTxtData(
    ecco_dict_csv,
    eebo_dict_csv,
    max_size=maxsize_inmem,
    load_all=load_all)

for current_iter in all_iter:
    if current_iter in processed_iters:
        continue
    else:
        print("   --- processing iter: " + str(current_iter))
        process_batch_files_multi(inputdir, outputdir, inmemtxtdata,
                                  current_iter, ecco_dict_csv, eebo_dict_csv)
        with open(processed_iters_txt, 'a') as logfile:
            logfile.write(str(current_iter) + "\n")

print("end:", datetime.now())
print("elapsed:", str(timedelta(seconds=(int(time()-start_time)))))
