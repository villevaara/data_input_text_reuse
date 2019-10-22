import json
from operator import itemgetter


def load_good_metadata(jsonfile):
    with open(jsonfile) as json_datafile:
        good_metadata = json.load(json_datafile)
        return(good_metadata)


def write_results_txt(hit_clusters, prefix):
    for key, value in hit_clusters.items():
        directory = "output/" + prefix + "/"
        filename = directory + prefix + "_" + key + ".txt"
        with open(filename, 'w') as txtfile:
            txtfile.write("clust: " + key + "\n")
            txtfile.write("count: " + str(value.get('Count')) + "\n")
            txtfile.write("avgle: " + str(value.get('Avglength')) + "\n\n")
            hits = value.get('Hits')

            for hit in hits:
                txtfile.write(str(hit.get('year')) + " -- " +
                              hit.get('author') + " -- " +
                              hit.get('title') +
                              " -- eccoid: " +
                              str(hit.get('eccoid')) +
                              " -- estcid: " +
                              hit.get('estcid') + "\n")

            txtfile.write("\n-----------\n-----------\n\n")

            for hit in hits:
                eccoid = hit.get('eccoid')
                year = hit.get('year')
                author = hit.get('author')
                title = hit.get('title')
                estcid = hit.get('estcid')
                text = hit.get('text')
                txtfile.write(str(year) + " -- " + author + " -- " + title +
                              " -- eccoid: " + str(eccoid) + " -- estcid: " +
                              estcid + "\n\n")
                txtfile.write(text)
                txtfile.write("\n\n-----\n")


def get_author_from_estc(ecco_id, good_metadata):
    estc_metadata = good_metadata.get(ecco_id)
    author = estc_metadata.get('estc_author')
    if not author:
        author = "NO AUTHOR IN ESTC DATA"
    return author


def get_author_bd_from_estc(ecco_id, good_metadata):
    estc_metadata = good_metadata.get(ecco_id)
    author_birth = estc_metadata.get('estc_author_birth')
    if author_birth == "NA" or author_birth == "ESTC DATA MISSING":
        author_birth = None
    author_death = estc_metadata.get('estc_author_death')
    if author_death == "NA" or author_death == "ESTC DATA MISSING":
        author_death = None
    return {'birth': author_birth, 'death': author_death}


def get_estcid_from_estc(ecco_id, good_metadata):
    estc_metadata = good_metadata.get(ecco_id)
    estcid = estc_metadata.get('estc_id')
    if not estcid:
        estcid = "NO DATA FOR FIELD IN ESTC"
    return estcid


def get_country_from_estc(ecco_id, good_metadata):
    estc_metadata = good_metadata.get(ecco_id)
    country = estc_metadata.get('estc_country')
    if not country:
        country = "NO DATA FOR FIELD IN ESTC"
    return country


def get_location_from_estc(ecco_id, good_metadata):
    estc_metadata = good_metadata.get(ecco_id)
    location = estc_metadata.get('estc_publication_place')
    if not location:
        location = "NO DATA FOR FIELD IN ESTC"
    return location


def get_first_ed_year_guess_from_estc(ecco_id, good_metadata):
    estc_metadata = good_metadata.get(ecco_id)
    first_ed_year_guess = estc_metadata.get('first_ed_year')
    if first_ed_year_guess is not None:
        first_ed_year_guess = int(first_ed_year_guess)
    return first_ed_year_guess


def get_year_from_estc(ecco_id, good_metadata):
    estc_metadata = good_metadata.get(ecco_id)
    year = estc_metadata.get('estc_publication_year')
    if year == 'NA' or year == 'ESTC DATA MISSING':
        year = 9999
    else:
        year = int(year)
    return year


def get_title_from_estc(ecco_id, good_metadata):
    estc_metadata = good_metadata.get(ecco_id)
    title = estc_metadata.get('estc_title')
    if not title:
        title = "NO DATA FOR FIELD IN ESTC"
    return title


def enrich_cluster(cluster_value, good_metadata):
    hits = cluster_value.get('Hits')
    hits_sorted = []

    for hit in hits:
        eccoid = hit.get('book_id')
        estc_metadata = good_metadata.get(eccoid)
        year = estc_metadata.get('estc_publication_year')
        if year == 'NA' or year == 'ESTC DATA MISSING':
            year = 9999
        else:
            year = int(year)
        author = estc_metadata.get('estc_author')
        if not author:
            author = "NO AUTHOR IN ESTC DATA"
        hit_dict = {'eccoid': eccoid,
                    'year': year,
                    'author': author,
                    'title': estc_metadata.get('estc_title'),
                    'estcid': estc_metadata.get('estc_id'),
                    'text': hit.get('text')}
        hits_sorted.append(hit_dict)

    hits_sorted = sorted(hits_sorted, key=itemgetter('year'))
    ret_dict = {'Hits': hits_sorted,
                'Avglength': cluster_value.get('Avglength'),
                'Count': cluster_value.get('Count')}
    return ret_dict


def process_cluster(cluster_data, good_metadata,
                    search_author='NONE',
                    search_title='NONE',
                    search_estcid='NONE',
                    search_text_file='NONE',
                    need_others=True,
                    min_authors=1,
                    min_count=2,
                    primus='any'):

    search_text_list = []

    if search_text_file != 'NONE':
        with open(search_text_file) as file:
            search_text_list = file.readlines()

    # print(search_text_list)

    hit_clusters = dict()
    totalHits = 0

    for key, value in cluster_data.items():
        enriched_value = enrich_cluster(value, good_metadata)
        hits = enriched_value.get('Hits')
        count = int(enriched_value.get('Count'))
        primus_status = False

        if count >= min_count:
            hitsFound = False
            if need_others:
                othersFound = False
            else:
                othersFound = True
            authorlist = []

            for hit in hits:
                author_found = False
                title_found = False
                estcid_found = False
                text_found = False
                multi_author = False
                if search_author == "NONE":
                    author_found = True
                elif search_author.lower() in hit.get('author').lower():
                    author_found = True

                if search_title == "NONE":
                    title_found = True
                elif search_title.lower() in hit.get('title').lower():
                    title_found = True

                if search_estcid == "NONE":
                    estcid_found = True
                elif search_estcid == hit.get('estcid'):
                    estcid_found = True

                if len(search_text_list) == 0:
                    text_found = True
                else:
                    for term in search_text_list:
                        if term.strip() in hit.get('text').lower():
                            text_found = True

                if (author_found and title_found and estcid_found and
                        text_found):
                    hitsFound = True
                else:
                    othersFound = True

                author = hit.get('author')
                authorlist.append(author)
                authorset = set(authorlist)
                if len(authorset) >= min_authors:
                    multi_author = True

            if (primus == 'first' and
                    search_author.lower() in authorlist[0].lower()):
                primus_status = True
            elif (primus == 'notfirst' and
                    search_author.lower() not in authorlist[0].lower()):
                primus_status = True
            elif (primus == 'any'):
                primus_status = True

            if (hitsFound and othersFound and
                    multi_author and primus_status):
                print("hit: " + key)
                hit_clusters[key] = enriched_value
                totalHits = totalHits + 1

    return hit_clusters, totalHits


def write_single_cluster_txt(cluster_data_dict, write_filename):
    cluster_index = cluster_data_dict.get('cluster_index')
    gz_filename = cluster_data_dict.get('gz_filename')
    cluster_data = cluster_data_dict.get('cluster_data')

    filename = write_filename
    with open(filename, 'w') as txtfile:
        txtfile.write("gz_filename: " + gz_filename + "\n")
        txtfile.write("clust:       " + cluster_index + "\n")
        txtfile.write("count:       " + str(cluster_data.get('Count')) + "\n")
        txtfile.write("avgle:       " +
                      str(cluster_data.get('Avglength')) + "\n\n")
        hits = cluster_data.get('Hits')

        for hit in hits:
            eccoid = hit.get('book_id')
            year = hit.get('year')
            author = hit.get('author')
            title = hit.get('title')
            text = hit.get('text')
            txtfile.write(str(year) + " -- " + author + " -- " + title +
                          " -- eccoid: " + str(eccoid) +
                          "\n\n")
            txtfile.write(text)
            txtfile.write("\n\n-----\n")


def update_summary_dict(hit_clusters, summary_dict):
    for key, value in hit_clusters.items():

        cluster_index = key
        cluster_data = value

        hits = cluster_data.get('Hits')

        for hit in hits:
            estcid = hit.get('estcid')
            year = hit.get('year')
            author = hit.get('author')
            title = hit.get('title')

            if estcid in summary_dict.keys():
                # print(summary_dict)
                summary_dict_datarow = summary_dict.get(estcid)
                # print(summary_dict_datarow)
                references = summary_dict_datarow.get("references") + 1
                summary_dict_datarow["references"] = references
                clusters = summary_dict_datarow.get("clusters")
                clusters.append(cluster_index)
                summary_dict_datarow["clusters"] = clusters
                summary_dict[estcid] = summary_dict_datarow
            else:
                clusters = [cluster_index]
                references = 1
                summary_dict_datarow = {"estcid": estcid,
                                        "title": title,
                                        "author": author,
                                        "year": year,
                                        "references": references,
                                        "clusters": clusters}
                summary_dict[estcid] = summary_dict_datarow

    return summary_dict
