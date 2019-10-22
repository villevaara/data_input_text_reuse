import os


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
