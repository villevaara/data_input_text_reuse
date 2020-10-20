import os
import tarfile
from lib.helpers import create_dir_if_not_exists


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
