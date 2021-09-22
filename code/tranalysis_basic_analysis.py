from lib.utils_common import read_csv_to_dictlist
from lib.helpers import list_to_dict_by_key
import os
import csv


def save_outputdata_csv(outputdata, outputfname):
    with open(outputfname, 'w') as outcsv:
        fieldnames = outputdata[0].keys()
        writer = csv.DictWriter(outcsv, fieldnames=fieldnames)
        writer.writeheader()
        for row in outputdata:
            writer.writerow(row)


datacsv = "../data/work/trstats_same_author_filtered.csv"

thisdata = read_csv_to_dictlist(datacsv)

# thisdata

for item in thisdata:
    for thiskey in ['publication_year', 'text_length', 'earlier_frag',
                    'earlier_conn', 'earlier_char', 'later_frag', 'later_conn',
                    'later_char', 'parallel_frag', 'parallel_conn',
                    'parallel_char']:
        item[thiskey] = int(item[thiskey])
    for thiskey in [
            'parallel_char_by_length', 'earlier_char_by_length',
            'later_char_by_length']:
        item[thiskey] = float(item[thiskey])


thissorted = sorted(
    thisdata, key=lambda i: i['later_char'], reverse=True)


top1000 = thissorted[:10001]

save_outputdata_csv(thissorted, "../data/work/top_all_tr_after.csv")
