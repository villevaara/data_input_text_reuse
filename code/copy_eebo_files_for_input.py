# input:
# - all of ecco. one text file per item.
#   * no markdown
# - eebo-tcp. main text and notes
#   * make sure all possibilities covered
#   * make sure no markdown?

# first additional input:
# - ecco-tcp
#   * now or additional?
#   * hopefully no markdown

# second additional input dataset:
# - newspaper data X

# ids:
# - one id per ecco text
# - one id per eebo full text
# - one id per eebo full text note (with corresponding eebo full text id as substring)
# 

# 1. get dict with ecco ids and source files

# 2. get dict with eebo ids and fulltexts
# 2.1. get dict with eebo ids and fulltexts-notes

import os
from fnmatch import fnmatch
from shutil import copyfile
from lib.helpers import create_dir_if_not_exists

print("reading ecco data")


def test_file_len(file_location, treshold=50):
    with open(file_location, 'r') as infile:
        data = infile.read()
        if len(data) >= treshold:
            return True
        else:
            return False


def filter_eebo_file(eebo_filepath, include_notes=False, len_treshold=50):
    eebo_filename_lastpart = eebo_filepath.split("/")[-1]
    if include_notes and ("_note_at_" in eebo_filename_lastpart):
        return test_file_len(eebo_filepath, len_treshold)
        # return True
    elif eebo_filename_lastpart.count("_") == 2 and (
            eebo_filename_lastpart[-9:] == "_text.txt"):
        return True
    else:
        return False


def copy_eebo_files(
        datasource='../data/raw/eebotxt',
        copydestdir='../data/raw/eebotxt_filtered/',
        include_notes=True,
        len_treshold=50):
    pattern = "*.txt"
    # fullpaths = []
    for path, subdirs, files in os.walk(datasource):
        for name in files:
            if fnmatch(name, pattern):
                thispath = os.path.join(path, name)
                if filter_eebo_file(thispath, include_notes, len_treshold):
                    dst = copydestdir + "/" + thispath.split("/eebotxt/")[-1]
                    print(dst)
                    create_dir_if_not_exists("/".join(dst.split("/")[0:-1]))
                    copyfile(thispath, dst)


print("copying eebofiles")
copy_eebo_files()
