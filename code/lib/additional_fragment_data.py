import json
import glob


def get_additional_cluster_data(datadir):
    datafiles = glob.glob(datadir + "*")
    combined_cludata = []
    for datafile in datafiles:
        with open(datafile, 'r') as jsonfile:
            cludata = json.load(jsonfile)
            for item in cludata:
                combined_cludata.append(item)
    return combined_cludata


def unify_ecco_id(ecco_id_string):
    if "/" in ecco_id_string:
        unified_id = ecco_id_string.split("/")[-1].split(".")[0]
    else:
        unified_id = ecco_id_string
    return unified_id


def get_flat_clusterdata(additional_cluster_data):
    flat_clusterdata = []
    for cluster_id in additional_cluster_data.keys():
        clusterdata = additional_cluster_data.get(cluster_id)
        for item in clusterdata.get('hits'):
            frag_id = cluster_id
            ecco_id = item.get('doc_id')
            # new additional cluster data had full path. fix that:
            ecco_id = unify_ecco_id(ecco_id)
            text = item.get('text')
            start_index = item.get('original_indices')[0]
            end_index = item.get('original_indices')[1]
            flat_clusterdata.append({
                'documentID': ecco_id,
                'fragmentID': frag_id,
                'text': text,
                'startIndex': start_index,
                'endIndex': end_index
            })
    return flat_clusterdata


def read_datadir_add_clusterdata(datadir):
    additional_cluster_data = get_additional_cluster_data(datadir)
    # flat_clusterdata = get_flat_clusterdata(additional_cluster_data)
    return additional_cluster_data

# add_cludatadir = "data/tr_data_additional/14_volumes/"
# add_cludata = read_datadir_add_clusterdata(add_cludatadir)
