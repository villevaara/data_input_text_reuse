import csv
import operator


def list_to_dict_by_key(list_to_group, key_field):
    results_dict = {}
    for entry in list_to_group:
        if entry[key_field] in results_dict.keys():
            results_dict[entry[key_field]].append(entry)
        else:
            results_dict[entry[key_field]] = [entry]
    return results_dict


def read_csv_to_dictlist(csv_file):
    ret_list = []
    with open(csv_file, 'r', encoding="utf-8") as csvfile:
        csvreader = csv.DictReader(csvfile)
        for row in csvreader:
            ret_list.append(row)
    return ret_list


def save_outputdata_csv(outputdata, outputfname):
    with open(outputfname, 'w') as outcsv:
        fieldnames = outputdata[0].keys()
        writer = csv.DictWriter(outcsv, fieldnames=fieldnames)
        writer.writeheader()
        for row in outputdata:
            writer.writerow(row)


dataloc = "/home/vvaara/projects/comhis/dhh21bayle_data/bayle_processed/"
datafiles = [
    "bd1710_1_estcid_T143095_later_first_instance_author.csv",
    "bd1710_2_estcid_T143095_later_first_instance_author.csv",
    "bd1710_3_estcid_T143095_later_first_instance_author.csv",
    "bd1710_4_estcid_T143095_later_first_instance_author.csv",
    "bd1734b_1_estcid_T143097_later_first_instance_author.csv",
    "bd1734b_2_estcid_T143097_later_first_instance_author.csv",
    "bd1734b_3_estcid_T143097_later_first_instance_author.csv",
    "bd1734b_4_estcid_T143097_later_first_instance_author.csv",
    "bd1734b_5_estcid_T143097_later_first_instance_author.csv",
    "bd1734a_1_estcid_T143096_later_first_instance_author.csv",
    "bd1734a_2_estcid_T143096_later_first_instance_author.csv",
    "bd1734a_3_estcid_T143096_later_first_instance_author.csv",
    "bd1734a_4_estcid_T143096_later_first_instance_author.csv",
    "bd1734a_5_estcid_T143096_later_first_instance_author.csv",
    "bd1734a_6_estcid_T143096_later_first_instance_author.csv",
    "bd1734a_7_estcid_T143096_later_first_instance_author.csv",
    "bd1734a_8_estcid_T143096_later_first_instance_author.csv",
    "bd1734a_9_estcid_T143096_later_first_instance_author.csv",
    "bd1734a_10_estcid_T143096_later_first_instance_author.csv"]

all_totals = {}

for datafile in datafiles:
    thisdata = read_csv_to_dictlist(dataloc + datafile)
    thisdata_by_secondary_id = list_to_dict_by_key(
        thisdata, 'api_id_secondary')
    #
    totals = {}
    for key, value in thisdata_by_secondary_id.items():
        totals[key] = {
            'title_secondary': value[0]['title_secondary'],
            'author_secondary': value[0]['author_secondary'],
            'publication_year_secondary': value[0][
                'publication_year_secondary']
        }
        chars_inds = set()
        for item in value:
            for char_ind in range(int(item['text_start_secondary']),
                                  int(item['text_end_secondary'])):
                chars_inds.add(char_ind)
        totals[key]['chars'] = len(chars_inds)
    all_totals[datafile] = totals


output = []

for bayle_item, totals in all_totals.items():
    for other_id, value in totals.items():
        outputline = value
        outputline['bayle_item'] = bayle_item
        outputline['id_secondary'] = other_id
        output.append(outputline)

output = sorted(output, key=operator.itemgetter("bayle_item", "chars"),
                reverse=True)
outputpath = "/home/local/vvaara/projects/comhis/bayle-textreuse/data/work/"

save_outputdata_csv(
    outputdata=output,
    outputfname=outputpath + "bayle_later_statistics.csv")
