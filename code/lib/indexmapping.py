from lib.headerdata import get_headers_from_document_text


def get_document_text_without_headers(this_ecco_text, this_ecco_headers):
    this_ecco_text_stripped = this_ecco_text
    for header in this_ecco_headers:
        this_ecco_text_stripped = "".join(
            this_ecco_text_stripped.split(header['full_header_text']))
    return this_ecco_text_stripped


def get_stripped_text_index(this_ecco_headers):
    stripped_text_index = []
    chars_removed = 0
    for header in this_ecco_headers:
        stripped_header_index = header['index'] - chars_removed
        chars_removed += len(header['full_header_text'])
        stripped_text_index.append(
            {'index': stripped_header_index,
             'chars_removed': chars_removed,
             'header_text': header['full_header_text']})
    # index is the character index by which
    # chars_removed number of characters in headers have been removed
    return stripped_text_index


def verify_header_embedding_index(
        ecco_octavo_text, fragment_text, missing_index):
    full_text_subset = ecco_octavo_text[
        missing_index['start_index']:
        missing_index['end_index']]
    full_text_subset_minus_headers = full_text_subset
    for header in missing_index['headers_within']:
        full_text_subset_minus_headers = (
            "".join(full_text_subset_minus_headers.split(header)))
    verifies = full_text_subset_minus_headers == fragment_text
    return {
        'full_text_subset': full_text_subset,
        'full_text_subset_minus_headers': full_text_subset_minus_headers,
        'fragment_text': fragment_text,
        'verifies': verifies
    }


def get_missing_fragment_index(
        fragment_text, stripped_text, stripped_text_index):
    stripped_frag_index_start = stripped_text.find(fragment_text)
    # if fragment text is not found in the stripped text, return -1
    if stripped_frag_index_start == -1:
        return {
            'start_index': -1,
            'end_index': -1,
            'headers_within': []}
    stripped_frag_index_end = stripped_frag_index_start + len(fragment_text)
    start_add = 0
    end_add = 0
    headers_within = []
    for item in stripped_text_index:
        if item['index'] <= stripped_frag_index_start:
            start_add += len(item['header_text'])
        if item['index'] <= stripped_frag_index_end:
            end_add += len(item['header_text'])
            if item['index'] > stripped_frag_index_start:
                headers_within.append(item['header_text'])
        else:
            break
    return {
        'start_index': stripped_frag_index_start + start_add,
        'end_index': stripped_frag_index_end + end_add,
        'headers_within': headers_within}


# doc: 0957000300 frag: 2601263 not found with string search.

def get_index_for_header_enclosing_fragment(this_ecco_text, fragment_text):
    this_ecco_headers = get_headers_from_document_text(this_ecco_text)
    stripped_text_index = get_stripped_text_index(this_ecco_headers)
    stripped_text = get_document_text_without_headers(
        this_ecco_text, this_ecco_headers)
    missing_index = get_missing_fragment_index(
        fragment_text, stripped_text, stripped_text_index)
    index_verifies = verify_header_embedding_index(
        this_ecco_text, fragment_text, missing_index)['verifies']
    if index_verifies:
        return {
            'start_index': missing_index['start_index'],
            'end_index': missing_index['end_index'],
            'validates': index_verifies
        }
    else:
        return {
            'start_index': None,
            'end_index': None,
            'validates': index_verifies
        }
