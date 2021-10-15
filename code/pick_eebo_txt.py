import glob
import json
from shutil import copyfile
import os


path = "../data/raw/eccotxt/"
# datafiles_found = []

# text_files = glob.glob(path + "/**/*.txt", recursive = True)

resfiles_path = "../data/work/projc/processed"
resfiles = glob.glob(resfiles_path + "/*.json")

ids_to_find = set()

for resfile in resfiles:
    with open(resfile, 'r') as jsonfile:
        reusedata = json.load(jsonfile)
        for item in reusedata:
            if item['api_primary'] == "ecco":
                ids_to_find.add(item['id_primary'])
            if item['api_secondary'] == "ecco":
                ids_to_find.add(item['id_secondary'])

ids_to_find = list(ids_to_find)

ids_found = os.listdir("../data/work/projc/eccodataset/")

print("searching...")
i = 0
max_i = len(ids_to_find)
for text_id in ids_to_find:
    if text_id + ".txt" in ids_found:
        continue
    files = glob.glob(path + "/**/" + text_id + ".txt", recursive = True)
    if len(files) < 1:
        print("\n!!! No files found, strage!\n")
    for file in files:
        dest = "../data/work/projc/eccodataset/" + file.split("/")[-1]
        copyfile(file, dest)
        print(text_id + " -- copied: " + file)
    print(str(i) + "/" + str(max_i))
    i += 1
