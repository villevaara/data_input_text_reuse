import os
import csv
from datetime import datetime


def create_dir_if_not_exists(directory_path, verbose=True):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
    if verbose:
        print("Created: " + directory_path)


def get_current_date_string():
    datestring = datetime.now().strftime('%y%m%d')
    return datestring


def get_current_timestamp_string():
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
    return timestamp


def read_csv_to_dictlist(csv_file):
    ret_list = []
    with open(csv_file, 'r') as csvfile:
        csvreader = csv.DictReader(csvfile)
        for row in csvreader:
            ret_list.append(row)
    return ret_list


def write_csv_from_dictlist(dictlist, csv_file):
    with open(csv_file, 'w') as outfile:
        fieldnames = dictlist[0].keys()
        csvwriter = csv.DictWriter(outfile, fieldnames=fieldnames)
        csvwriter.writeheader()
        for row in dictlist:
            csvwriter.writerow(row)


def path_is_empty(path):
    for dirpath, dirnames, files in os.walk(path + '/.'):
        if files:
            return False
        if not files:
            return True
