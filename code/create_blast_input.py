import json
import gzip
import csv
import sys
import os
from fnmatch import fnmatch
import datetime
import argparse


def get_txt_file_metadata(txt_file_path):
    entry_id = txt_file_path.split("/")[-1].split(".txt")[0]
    if "/eccotxt/" in txt_file_path:
        collection = txt_file_path.split("/eccotxt/")[-1].split("/")[0].lower()
    elif "/eebotxt/" in txt_file_path:
        collection = txt_file_path.split("/eebotxt/")[-1].split("/")[0].lower()
    else:
        sys.exit("No collection part found in path (/eccotxt/ or /eebootxt/).")
    return {
            'doc_id': entry_id,
            'collection': collection,
            'text_loc': txt_file_path}


def get_input_file_list(input_list_csv):
    input_file_list = []
    with open(input_list_csv, 'r') as eccocsv:
        reader = csv.DictReader(eccocsv)
        for row in reader:
            input_file_list.append(row)
    return input_file_list


def read_txt(text_file_loc):
    with open(text_file_loc) as textfile:
        text = textfile.read()
        return text


def get_entry_data(entry_metedata_dict):
    return {
        'doc_id': entry_metedata_dict['doc_id'],
        'text': read_txt(entry_metedata_dict['text_loc']),
        'collection': entry_metedata_dict['collection'],
        'text_loc': entry_metedata_dict['text_loc']}


def write_json_results(outdata, jsonfile, compress=True):
    jsonstring = ""
    for entry in outdata:
        entry_json = json.dumps(entry) + "\n"
        jsonstring += entry_json
    if compress:
        with gzip.open(jsonfile + ".gz", 'wb') as jsonfilegzip:
            json_bytes = jsonstring.encode('utf-8')
            jsonfilegzip.write(json_bytes)
    else:
        with open(jsonfile, 'w') as jsonfile:
            jsonfile.write(jsonstring)


def process_chunk(this_chunk, chunk_n, outdir, prefix=None):
    chunk_data = []
    for entry in this_chunk:
        entry_data = get_entry_data(entry)
        chunk_data.append(entry_data)
    if prefix is not None:
        chunk_fname = prefix + "_chunk_" + str(chunk_n) + ".json"
    else:
        chunk_fname = "chunk_" + str(chunk_n) + ".json"
    jsonfile = outdir + "/" + chunk_fname
    write_json_results(chunk_data, jsonfile, compress=True)


def process_input_id_list_to_chunks(doc_id_list, outdir):
    chunks = [doc_id_list[x:x+100] for x in range(0, len(doc_id_list), 100)]
    chunk_n = 0
    for chunk in chunks:
        process_chunk(chunk, chunk_n, outdir)
        print("Processed chunk " + str(chunk_n))
        chunk_n += 1


def filter_eebo_file(eebo_filepath):
    eebo_filename_lastpart = eebo_filepath.split("/")[-1]
    if "_note_at_" in eebo_filename_lastpart:
        return True
    elif eebo_filename_lastpart.count("_") == 2 and (
            eebo_filename_lastpart[-9:] == "_text.txt"):
        return True
    else:
        return False


def get_input_id_stream(sourcedir):
    # iterate over *.txt files in input dir,
    # pass files through metadata function
    # filter them
    # 'doc_id' and 'text' are required fields, rest optional metadata
    pattern = "*.txt"
    for path, subdirs, files in os.walk(sourcedir):
        for name in files:
            if fnmatch(name, pattern):
                thispath = os.path.join(path, name)
                abspath = os.path.abspath(thispath)
                txt_meta = get_txt_file_metadata(abspath)
                keep_this = False
                if txt_meta['collection'][:4] == "eebo":
                    # eebo tcp text is broken up, so only limited
                    # files taken as text reuse input data
                    keep_this = filter_eebo_file(abspath)
                elif txt_meta['collection'][:4] == "ecco":
                    # ecco txt expected to be just single file
                    keep_this = True
                else:
                    print("Unknown collection: " + txt_meta['collection'])
                if keep_this:
                    entry_data = get_entry_data(txt_meta)
                    yield entry_data


def log_writer(logline, logbuffer, logfile, force_write=False):
    logbuffer.append(logline)
    if len(logbuffer) == 1000 or force_write:
        with open(logfile, 'a') as logoutfile:
            print("Wrote to log at: " + logfile)
            writer = csv.writer(logoutfile)
            writer.writerows(logbuffer)
            logbuffer = []
    return logbuffer


def process_input_stream_to_chunks(
        input_generator, logfile, outdir, chunk_size=100, prefix=None):
    chunk_n = 0
    logbuffer = []
    this_chunk = []
    #
    for item in input_generator:
        this_chunk.append(item)
        if logfile is not None:
            logbuffer = log_writer(
                [item['text_loc']], logbuffer, logfile)
        if len(this_chunk) == chunk_size:
            process_chunk(this_chunk, chunk_n, outdir, prefix)
            this_chunk = []
            print("Processed chunk " + str(prefix) + "_" + str(chunk_n))
            chunk_n += 1
    #
    if logfile is not None:
        logbuffer = log_writer(
            [""], logbuffer, logfile, force_write=True)
    process_chunk(this_chunk, chunk_n, outdir, prefix)
    print("Generator empty.")


# get command line args
parser = argparse.ArgumentParser(
    description="Script for preparing gzipped JSON input data for BLAST.")
parser.add_argument(
    "--prefix",
    help="Prefix for output files. Typically 'ecco' or 'eebo'",
    required=True)
parser.add_argument("--input", help="Location of txt files for input data.",
                    required=True)
parser.add_argument("--output", help="Output location.", required=True)
parser.add_argument("--log",
                    help="Logfile location. If None, skip logging.",
                    default=None)
args = parser.parse_args()

# logging
log_ts = datetime.datetime.now().strftime("_%y%m%d-%H%M")
if args.log is not None:
    logfile = (args.log + "/blast_input_log_" +
               args.prefix + log_ts + ".csv")
else:
    logfile = None


input_stream = get_input_id_stream(args.input)
process_input_stream_to_chunks(
    input_generator=input_stream,
    logfile=logfile,
    outdir=args.output,
    chunk_size=1000,
    prefix=args.prefix)
