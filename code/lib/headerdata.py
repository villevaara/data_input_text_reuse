import re

from lib.octavo_api_client import (
    OctavoEccoClient)


def get_headers_from_document_text(document_text):
    header_index_and_text = []
    header_indices = []
    for match in re.finditer('\n\n# ', document_text):
        header_indices.append(match.start())
    for index in header_indices:
        # from_header = document_text[index + 4:]  # 3:]
        full_header = document_text[index:]
        newline_position = full_header[4:].find('\n') + 4
        full_header_text = full_header[0:newline_position]
        header_text = full_header_text[4:].strip()
        # leave the \n\n# plus whitespace from the header text
        header_index_and_text.append({'index': index,
                                      'header_text': header_text,
                                      'full_header_text': full_header_text})
    return header_index_and_text


def get_headers_for_document_id(document_id, document_text=None):
    if document_text is None:
        ecco_api_client = OctavoEccoClient()
        document_data = ecco_api_client.get_text_for_document_id(document_id)
        document_text = document_data.get('text')
    headerdata = get_headers_from_document_text(document_text)
    return headerdata


def get_headerdata_as_dict(headerdata):
    headerdata_dict = {}
    for headerpair in headerdata:
        headerdata_dict[headerpair.get('index')] = (
            headerpair.get('header_text'))
    return headerdata_dict


def get_header_for_textindex(start_index, headerdata):

    header_text = "** NO HEADER FOUND **"

    for entry in headerdata:
        if (entry.get('index') - start_index) < 0:
            header_text = entry.get('header_text')
        else:
            break

    return header_text


def get_header_and_index_for_textindex(start_index, headerdata):
    header_text = "** NO HEADER FOUND **"
    header_index = -1
    for entry in headerdata:
        if (entry.get('index') - start_index) < 0:
            header_index = entry.get('index')
            header_text = entry.get('header_text')
        else:
            break
    return {'text': header_text, 'index': header_index}


def get_header_summary_data(cluster_list, document_id):
    # list of index-headertext -pairs:
    headerdata = get_headers_for_document_id(document_id)
    headerdata_dict = get_headerdata_as_dict(headerdata)

    summary_results = []

    for key in headerdata_dict.keys():

        total_fragments = 0
        authors = set()
        titles = set()

        for cluster in cluster_list:
            if cluster.group_id == key:
                total_fragments = total_fragments + cluster.get_length()
                authors.update(list(cluster.get_authors()))
                titles.update(list(cluster.get_titles()))

        unique_authors = len(authors)
        unique_titles = len(titles)

        if len(authors) > 0:
            authors_text = str(authors)
        else:
            authors_text = ""

        if len(titles) > 0:
            titles_text = str(titles)
        else:
            titles_text = ""

        index_results_dict = {'header_index': key,
                              'header_text': headerdata_dict.get(key),
                              'total_fragments': total_fragments,
                              'unique_authors': unique_authors,
                              'authors': authors_text,
                              'unique_titles': unique_titles,
                              'titles': titles_text}
        summary_results.append(index_results_dict)

    return summary_results
