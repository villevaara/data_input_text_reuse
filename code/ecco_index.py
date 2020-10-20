import os
from fnmatch import fnmatch
import csv
import argparse


def get_ecco_dict(root='../data/raw/eccotxt'):
    pattern = "*.txt"
    fullpaths = []
    for path, subdirs, files in os.walk(root):
        for name in files:
            if fnmatch(name, pattern):
                thispath = os.path.join(path, name)
                fullpaths.append(os.path.abspath(thispath))
    ecco_id_dict = {}
    for path in fullpaths:
        ecco_id = path.split("/")[-1].split(".txt")[0]
        ecco_id_dict[ecco_id] = "/".join(path.split("/")[0:-1])
    ecco_out = []
    for key, value in ecco_id_dict.items():
        # set pouta single pages location
        pouta_pages = (
            "cloud-user@vm0824.kaj.pouta.csc.fi:/scratch/" +
            value.split("/raw/")[-1])
        # set pouta xml location
        collection = value.split("/eccotxt/")[-1].split("/")[0].lower()
        if collection == "ecco_i":
            pouta_xml = (
                "cloud-user@vm0824.kaj.pouta.csc.fi:/ecco1pool/ecco1/" +
                value.split("/eccotxt/")[-1])
        else:
            pouta_xml = (
                "cloud-user@vm0824.kaj.pouta.csc.fi:/ecco2pool/ecco2/" +
                value.split("/eccotxt/")[-1])
        # create output
        ecco_out.append({
            'id': key,
            'collection': collection,
            'path': value,
            'pouta_pages': pouta_pages,
            'pouta_xml': pouta_xml})
    return ecco_out


def get_eebo_dict(root='../data/raw/eebotxt'):
    pattern = "[AB][0-9][0-9]*"
    fullpaths = []
    for path, subdirs, files in os.walk(root):
        for name in subdirs:
            if fnmatch(name, pattern):
                thispath = os.path.join(path, name)
                fullpaths.append(os.path.abspath(thispath))
    eebo_id_dict = {}
    for path in fullpaths:
        eebo_id = path.split("/")[-1]
        eebo_id_dict[eebo_id] = path
        eebo_out = []
        for key, value in eebo_id_dict.items():
            collection = value.split("/eebotxt/")[-1].split("/")[0].lower()
            eebo_out.append({
                'id': key,
                'collection': collection,
                'path': value,
                })
    return eebo_out


def get_ecco_txt_dict(datasource="../data/raw/eccotxt"):
    pattern = "*.txt"
    fullpaths = []
    for path, subdirs, files in os.walk(datasource):
        for name in files:
            if fnmatch(name, pattern):
                thispath = os.path.join(path, name)
                fullpaths.append(os.path.abspath(thispath))
    ecco_id_dict = {}
    for path in fullpaths:
        ecco_id = path.split("/")[-1].split(".txt")[0]
        if ecco_id in ecco_id_dict.keys():
            print("duplicated key!")
            print(ecco_id)
            print(path)
        ecco_id_dict[ecco_id] = path
    ecco_out = []
    for key, value in ecco_id_dict.items():
        collection = value.split("/eccotxt/")[-1].split("/")[0].lower()
        ecco_out.append({
            'id': key,
            'collection': collection,
            'content': value})
    return ecco_out


def get_eebo_txt_dict(datasource='../data/raw/eebotxt'):
    pattern = "*.txt"
    fullpaths = []
    for path, subdirs, files in os.walk(datasource):
        for name in files:
            if fnmatch(name, pattern):
                thispath = os.path.join(path, name)
                fullpaths.append(os.path.abspath(thispath))
    eebo_id_dict = {}
    for path in fullpaths:
        eebo_id = path.split("/")[-1].split(".txt")[0]
        if eebo_id in eebo_id_dict.keys():
            print("duplicated key!")
            print(eebo_id)
            print(path)
        eebo_id_dict[eebo_id] = path
        eebo_out = []
        for key, value in eebo_id_dict.items():
            collection = value.split("/eebotxt/")[-1].split("/")[0].lower()
            eebo_out.append({
                'id': key,
                'collection': collection,
                'content': value,
                })
    return eebo_out


def filter_eebo_dict(eebo_dict):
    filtered = []
    for entry in eebo_dict:
        if "_note_at_" in entry['content']:
            filtered.append(entry)
        elif entry['content'].count("_") == 2 and (
                entry['content'][-9:] == "_text.txt"):
            filtered.append(entry)
    return filtered


def write_eccodict(fname, ecco_out_data):
    with open(fname, 'w') as outfile:
        fieldnames = ecco_out_data[0].keys()
        writer = csv.DictWriter(outfile, fieldnames)
        writer.writeheader()
        for row in ecco_out_data:
            writer.writerow(row)


def read_eccosource_dict(fname):
    eccosource_dict = {}
    with open(fname, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            eccosource_dict[row['id']] = row
    return eccosource_dict


def read_eebosource_dict(fname):
    eebosource_dict = {}
    with open(fname, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            eebosource_dict[row['id']] = row
    return eebosource_dict


def get_all_doc_ids(ecco_source_csv, eebo_source_csv):
    all_ids = []
    for filename in [ecco_source_csv, eebo_source_csv]:
        with open(filename, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                all_ids.append(row['id'])
    return all_ids


def write_all_doc_ids(all_doc_ids, outfile):
    with open(outfile, 'w') as outtxt:
        for doc_id in all_doc_ids:
            outtxt.write(doc_id + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Create index files for ECCO and EEBO texts.")
    parser.add_argument("--eccodir", help="ECCO datadir", required=True)
    parser.add_argument("--eebodir", help="EEBO datadir", required=True)
    args = parser.parse_args()

    ecco_out_data = get_ecco_dict(root=args.eccodir)
    write_eccodict("../data/work/ecco_dict.csv", ecco_out_data)
    eebo_dict = get_eebo_dict(root=args.eebodir)
    write_eccodict("../data/work/eebo_dict.csv", eebo_dict)


if __name__ == "__main__":
    main()
