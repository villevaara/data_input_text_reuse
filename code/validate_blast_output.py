import argparse
from lib.blast_datareader import (
    validate_blast_iter_data,
    get_tar_datafiles)
import csv


parser = argparse.ArgumentParser(
    description="Validate BLAST output data in directory.")
parser.add_argument("--datadir", help="Input datadir", required=True)
parser.add_argument("--logdir", help="Log output location",
                    default="../logs/")
args = parser.parse_args()


tarfiles_in_path = get_tar_datafiles(args.datadir)

bad_results = []

for tarfile in tarfiles_in_path:
    validates = validate_blast_iter_data(tarfile)
    this_iter = int(tarfile.split("iter_")[-1].split(".")[0])
    if validates['validates'] is not True:
        bad_results.append({
            'iter': this_iter, 'n_bad': validates['n_bad']})

br_sorted = sorted(bad_results, key=lambda k: k['iter'])

with open((args.logdir + '/iters_invalid.csv'), 'w', newline='') as csvfile:
    fieldnames = ['iter', 'n_bad']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for item in br_sorted:
        writer.writerow(item)
