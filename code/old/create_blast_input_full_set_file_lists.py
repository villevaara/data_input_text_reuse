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

# from create_blast_input_json import get_ecco_id_dict
from ecco_index import (
    get_ecco_txt_dict,
    # get_eebo_txt_dict,
    # filter_eebo_dict,
    write_eccodict)
# import json
# import gzip


print("reading ecco data")
ecco_id_dict = get_ecco_txt_dict(datasource="../data/raw/eccotxt")

print("writing csvs")
write_eccodict('../data/work/input_file_lists/ecco_dict.csv', ecco_id_dict)
