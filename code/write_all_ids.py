from ecco_index import get_all_doc_ids, write_all_doc_ids


eebocsv = "../data/work/eebo_dict.csv"
eccocsv = "../data/work/ecco_dict.csv"

all_doc_ids = get_all_doc_ids(eebocsv, eccocsv)
write_all_doc_ids(all_doc_ids, "../data/work/all_doc_ids_eebotcp_ecco.txt")
