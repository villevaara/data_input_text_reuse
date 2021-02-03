import os
import tarfile
from lib.helpers import create_dir_if_not_exists
from collections import Counter
import json


def read_blast_cluster_csv_inmem(blastdata):
    outlist = []
    source_id = None
    for line in blastdata.decode("utf-8").split("\n"):
        # look for source id at its own line
        if line.startswith("# Query:"):
            source_id = line.split(" ")[-1].strip("\n")
        # Skip comments and empty lines
        elif line.startswith("#"):
            continue
        elif line == (""):
            continue
        else:
            linesplit = line.strip("\n").split("\t")
            if len(linesplit) != 7:
                continue
            else:
                outlist.append({
                    'source_id': str(source_id),
                    'source_start_blast': int(linesplit[1]),
                    'source_end_blast': int(linesplit[2]),
                    'target_id': str(linesplit[0]),
                    'target_start_blast': int(linesplit[3]),
                    'target_end_blast': int(linesplit[4]),
                    'align_length': int(linesplit[5]),
                    'positives_percent': float(linesplit[6])
                })
    return outlist


def validata_blast_batch(batchdata_bits):
    batchdata_utf8_list = batchdata_bits.decode("utf-8").split("\n")
    if batchdata_utf8_list[0][0:8] != "# BLASTP":
        return {'validates': False, 'reason': 'Invalid header.'}
    if batchdata_utf8_list[-2] != "# BLAST processed 1 queries":
        return {'validates': False, 'reason': 'Invalid footer.'}
    if len(batchdata_utf8_list) < 6:
        return {'validates': False, 'reason': "Length < 6 rows."}
    return {'validates': True, 'reason': ''}


def validate_blast_iter_data(tarfile_loc):
    iterdata = get_single_tar_contents(tarfile_loc)
    n_bad = 0
    reasons = []
    for item in iterdata:
        item_validates = validata_blast_batch(item)
        if item_validates['validates'] is False:
            n_bad += 1
            reasons.append(item_validates['reason'])
    counts = dict(Counter(reasons))
    if n_bad > 0:
        iter_validates = False
    else:
        iter_validates = True
    return {'validates': iter_validates, 'reasons': counts, 'n_bad': n_bad}


def read_blast_cluster_csv(blast_batch_file):
    outlist = []
    source_id = None
    with open(blast_batch_file, 'r') as blastdata:
        for line in blastdata.readlines():
            # look for source id at its own line
            if line.startswith("# Query:"):
                source_id = line.split(" ")[-1].strip("\n")
            elif line.startswith("#"):
                continue
            else:
                linesplit = line.strip("\n").split("\t")
                outlist.append({
                    'source_id': str(source_id),
                    'source_start_blast': int(linesplit[1]),
                    'source_end_blast': int(linesplit[2]),
                    'target_id': str(linesplit[0]),
                    'target_start_blast': int(linesplit[3]),
                    'target_end_blast': int(linesplit[4]),
                    'align_length': int(linesplit[5]),
                    'positives_percent': float(linesplit[6])
                })
    return outlist


def get_blast_data_filenames(rootdir="../data/work/blast_batches/"):
    dirfiles = os.listdir(rootdir)
    valid_files = []
    for file in dirfiles:
        if file.startswith('batch_') and file.endswith('.tsv'):
            valid_files.append(
                os.path.abspath(rootdir + file))
    return valid_files


def extract_tar_datafiles(rootdir="../data/work/blast_batches/"):
    dirfiles = os.listdir(rootdir)
    for file in dirfiles:
        if file.startswith('iter_') and file.endswith('.tar.gz'):
            tar = tarfile.open(rootdir + "/" + file, "r:gz")
            tar.extractall(path=rootdir)
            tar.close()
            print("Extracted: " + file)


def get_tar_datafiles(rootdir="../data/work/blast_batches/"):
    dirfiles = os.listdir(rootdir)
    tarfiles = []
    for file in dirfiles:
        if file.startswith('iter_') and file.endswith('.tar.gz'):
            tarfiles.append(rootdir + "/" + file)
    return tarfiles


def extract_single_tar_datafiles(tarpath):
    tar = tarfile.open(tarpath, "r:gz")
    itername = tarpath.split("/")[-1].split(".")[0]
    temp_path = os.path.dirname(tarpath) + "/tmp/" + itername + "/"
    create_dir_if_not_exists(temp_path)
    tar.extractall(path=temp_path)
    tar.close()
    print("Extracted: " + tarpath + " in: " + temp_path)
    return temp_path


def get_single_tar_contents(tarpath):
    tar = tarfile.open(tarpath, "r:gz")
    all_contents = []
    for member in tar.getmembers():
        f = tar.extractfile(member)
        if f is not None:
            contents = f.read()
            all_contents.append(contents)
    tar.close()
    print("  -- Extracted data in: " + tarpath)
    return all_contents


def get_single_tar_json_contents(tarpath, logfile):
    try:
        tar = tarfile.open(tarpath, "r:gz")
    except Exception:
        with open(logfile, 'a') as logtxt:
            logtxt.write(
                tarpath.split("/")[-1] + " - Error in opening file.\n")
        return None

    all_contents = []
    try:
        tar.getmembers()
    except Exception:
        with open(logfile, 'a') as logtxt:
            logtxt.write(
                tarpath.split("/")[-1] + " - Error in getting tar members.\n")
        return None

    for member in tar.getmembers():
        f = tar.extractfile(member)
        if f is not None:
            contents = json.load(f)
            all_contents.append(contents)
    tar.close()
    print("  -- Extracted data in: " + tarpath)
    return all_contents
