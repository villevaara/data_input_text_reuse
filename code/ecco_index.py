import os
from fnmatch import fnmatch
import csv


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
            'ecco_id': key,
            'collection': collection,
            'path': value,
            'pouta_pages': pouta_pages,
            'pouta_xml': pouta_xml})
    return ecco_out


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
            eccosource_dict[row['ecco_id']] = row
    return eccosource_dict


def main():
    ecco_out_data = get_ecco_dict()
    write_eccodict("../data/work/ecco_dict.csv", ecco_out_data)


if __name__ == "__main__":
    main()
