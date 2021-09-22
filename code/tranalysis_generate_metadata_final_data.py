from lib.blast_datareader import (
    get_single_tar_json_contents,
    get_tar_datafiles)
from lib.helpers import create_dir_if_not_exists
import json
import sys
import csv
import argparse


def get_unified_id(raw_id):
    if raw_id[0] == "A" or raw_id[0] == "B":
        return raw_id.split(".")[0]
    else:
        return raw_id


def get_tarfile_results(tarfile_name, results_so_far, mode="ecco"):
    tar_test_cont = get_single_tar_json_contents(tarfile_name)
    tar_cont = []
    if tar_test_cont is not None:
        for item in tar_test_cont:
            for sub_item in item:
                tar_cont.append(sub_item)
    else:
        return results_so_far
    results = results_so_far
    for item in tar_cont:
        id1 = get_unified_id(item['text1_id'])
        id2 = get_unified_id(item['text2_id'])
        has_eebo = False
        if id1[0] == "A" or id1[0] == "B":
            has_eebo = True
        if id2[0] == "A" or id2[0] == "B":
            has_eebo = True
        if mode == "ecco" and has_eebo:
            continue
        if mode == "eebo" and not has_eebo:
            continue
        connection_id = '#'.join(sorted([id1, id2]))
        if connection_id not in results.keys():
            results[connection_id] = {
                'text1_id': id1,
                'text2_id': id2,
                'total_length': int(item['align_length']),
                'fragments': 1
            }
        else:
            results[connection_id]['total_length'] += int(item['align_length'])
            results[connection_id]['fragments'] += 1
    return results


def save_outputdata_csv(outputdata, outputfname):
    with open(outputfname, 'w') as outcsv:
        fieldnames = outputdata[0].keys()
        writer = csv.DictWriter(outcsv, fieldnames=fieldnames)
        writer.writeheader()
        for row in outputdata:
            writer.writerow(row)


tarfile_name = "../data/raw/results_filled_test/iter_2_out.tar.gz"
tardir = "/media/vvaara/My Passport/worktemp/results_qpi100_filled"
tarfiles = get_tar_datafiles(tardir)
result_index = {}

outdir = "../data/work/tr_metadata/"
create_dir_if_not_exists(outdir)

# process ecco only
outfiles_n = 0
all_results = {}
files_in_buffer = 0
for tarfile in tarfiles:
    all_results = get_tarfile_results(tarfile, all_results, mode="ecco")
    files_in_buffer += 1
    if files_in_buffer == 1000:
        summary = []
        for key, value in all_results.items():
            resdict = value
            value['tr_id'] = key
            summary.append(resdict)
        outputfname = outdir + "tr_metadata_ecco_" + str(outfiles_n) + ".csv"
        save_outputdata_csv(summary, outputfname)
        print("Wrote:" + outputfname)
        outfiles_n += 1
        files_in_buffer = 0
        all_results = {}

summary = []
for key, value in all_results.items():
    resdict = value
    value['tr_id'] = key
    summary.append(resdict)
outputfname = outdir + "tr_metadata_ecco_" + str(outfiles_n) + ".csv"
save_outputdata_csv(summary, outputfname)
print("Wrote final:" + outputfname)


# process eebo
all_results = {}
for tarfile in tarfiles:
    all_results = get_tarfile_results(tarfile, all_results, mode="eebo")
summary = []
for key, value in all_results.items():
    resdict = value
    value['tr_id'] = key
    summary.append(resdict)
outputfname = outdir + "tr_metadata_eebo_all.csv"
save_outputdata_csv(summary, outputfname)
print("Wrote:" + outputfname)

