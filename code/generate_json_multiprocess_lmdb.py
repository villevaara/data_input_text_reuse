from text_encoder import TextEncoder
from lib.helpers import create_dir_if_not_exists
from joblib import Parallel, delayed
import json
import lib.blast_datareader as blastdr
import csv
import os
import ecco_index
from os import listdir
import argparse
import tarfile
from io import StringIO
from io import BytesIO
from datetime import datetime
from datetime import timedelta
from time import time
import lmdb


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


class BlastPairDB():
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
            self, db, source_text=None, target_text=None):
        if source_text is None:
            source_text = db.get(
                self.source_id.encode("ascii")).decode("unicode_escape")
        if target_text is None:
            target_text = db.get(
                self.target_id.encode("ascii")).decode("unicode_escape")
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


class DataProcessorDB:
    def __init__(self, db_loc):
        self.db_loc = db_loc

    def get_batch_jsondata_db(self, input_data, verbose=False):
        all_data = []
        for item in input_data:
            batchdata = blastdr.read_blast_cluster_csv_inmem(item)
            if len(batchdata) > 0:
                all_data.extend(batchdata)
        if len(all_data) > 0:
            print("    -- Processing data - len: " + str(len(all_data)))
        outdata = Parallel(n_jobs=threads)(
            delayed(self.get_blastpair_data)(item) for item in all_data)
        return outdata

    def get_blastpair_data(self, item):
        orig_db = lmdb.open(self.db_loc, readonly=True)
        o_db = orig_db.begin()
        textenc = TextEncoder("eng")
        blastpair = BlastPairDB(item, textenc)
        blastpair.set_correct_indices_and_texts(o_db)
        return blastpair.get_outdict()


def process_batch_files_db(batchdir, outputdir, db_loc, threads, this_iter):
    this_tar = batchdir + "/iter_" + str(this_iter) + ".tar.gz"
    batch_data = blastdr.get_single_tar_contents(this_tar)
    dataprocessor = DataProcessorDB(db_loc)
    #
    ready_data = dataprocessor.get_batch_jsondata_db(batch_data)
    ready_data_final = []
    for item in ready_data:
        if len(item) != 0:
            ready_data_final.append(item)
    #
    tarout_fname = outputdir + "/iter_" + str(this_iter) + "_out.tar.gz"
    tarfile_out = tarfile.open(tarout_fname, 'w:gz')
    s = StringIO()
    writethis = json.dumps(
        ready_data_final, indent=2, ensure_ascii=False)
    s.write(writethis)
    s.seek(0)
    tarinfo = tarfile.TarInfo(
        name="iter_" + str(this_iter) + ".json")
    tarinfo.size = len(s.getvalue().encode('utf-8'))
    tarfile_out.addfile(
        tarinfo=tarinfo, fileobj=BytesIO(s.getvalue().encode('utf-8')))
    tarfile_out.close()
    print("  -- Wrote: " + tarout_fname)


def get_processed_iters_from_output_path(outputdir):
    files_in_outdir = os.listdir(outputdir)
    processed_iters = []
    for file in files_in_outdir:
        if file[-6:] == "tar.gz" and file[:4] == "iter":
            processed_iters.append(int(file.split("_")[1]))
    return processed_iters


parser = argparse.ArgumentParser(description="Decode indices back to text.")
parser.add_argument("--datadir", help="Input datadir", required=True)
parser.add_argument("--outdir", help="Output datadir", required=True)
parser.add_argument(
    "--iter", help="INT. Iteration to process. -1 (default) = process all.",
    type=int, default=-1)
parser.add_argument(
    "--threads", help="Number of parallel threads to use.",
    type=int, default=1)
parser.add_argument(
    "--db", help="Location of LMDB database for texts", required=True)
args = parser.parse_args()

inputdir = args.datadir + "/"
outputdir = args.outdir + "/"
thisiter = args.iter
threads = args.threads
db_loc = args.db

# db_loc = ('/media/vvaara/My Passport/worktemp/txt_reuse/txt_reuse/' +
#           'blast_work_from_puhti/blast_work/db/original_data_DB/')

create_dir_if_not_exists(outputdir)

if thisiter == -1:
    all_tarfiles = blastdr.get_tar_datafiles(inputdir)
    all_iter = []
    for fname in all_tarfiles:
        fname_iter = int(fname.split("iter_")[-1].split(".")[0])
        all_iter.append(fname_iter)
else:
    all_iter = [thisiter]

# Check outputdir for already processed iterations. Skip these.
processed_iters = get_processed_iters_from_output_path(outputdir)
iters_to_process = []
for current_iter in all_iter:
    if iter == -1:
        if current_iter in processed_iters:
            continue
    else:
        iters_to_process.append(current_iter)
iters_to_process.sort()

print("\nGenerating final JSON for text reuse data. Args:")
print("Input dir  : " + inputdir)
print("Output dir : " + outputdir)
print("Iteration  : " + str(thisiter))
print("Threads    : " + str(threads))
print("DB Location: " + db_loc)
print("Iters to pr: " + len(iters_to_process))
print("Iters all  : " + len(all_iter))

print("\nStart time:", datetime.now())
start_time = time()

# ECCO = 61GB
# EEBO = 10GB

for current_iter in iters_to_process:
    print("Processing iter: " + str(current_iter))
    process_batch_files_db(inputdir, outputdir, db_loc,
                           threads, current_iter)

print("\nEnd time:", datetime.now())
print("Elapsed:", str(timedelta(seconds=(int(time()-start_time)))))
