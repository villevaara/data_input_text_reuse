import os
from fnmatch import fnmatch
import csv


def get_ecco_dict(root='../data/raw/eccotxt'):
    pattern = "xml"
    fullpaths = []
    for path, subdirs, files in os.walk(root):
        for name in subdirs:
            if fnmatch(name, pattern):
                thispath = os.path.join(path, name)
                fullpaths.append(os.path.abspath(thispath))
    ecco_id_dict = {}
    for path in fullpaths:
        ecco_id = path.split("/xml")[0].split("/")[-1]
        ecco_id_dict[ecco_id] = path
    ecco_out = []
    for key, value in ecco_id_dict.items():
        ecco_out.append({'ecco_id': key, 'path': value})
    return ecco_out


def write_eccodict(fname, ecco_out_data):
    with open(fname, 'w') as outfile:
        fieldnames = ecco_out_data[0].keys()
        writer = csv.DictWriter(outfile, fieldnames)
        writer.writeheader()
        for row in ecco_out_data:
            writer.writerow(row)


def main():
    ecco_out_data = get_ecco_dict()
    write_eccodict("../data/work/ecco_dict.csv", ecco_out_data)


if __name__ == "__main__":
    main()
