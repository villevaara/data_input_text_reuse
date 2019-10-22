import re
# import sys

from lib.tr_fragment import TextReuseFragment
from lib.text_reuse_common import load_good_metadata
from lib.octavo_api_client import (
    OctavoEccoClient,
    OctavoEeboClient
    # OctavoEccoClusterClient
    )


def test_fragment_text(fragment, document_text, target_text_type="octavo"):
    fragment_text = fragment.text
    # fragment_text = fragment_text.strip().lower()
    # octavo_text = document_text[fragment.start_index:
    #                             fragment.end_index].lower()
    if target_text_type == "octavo":
        target_text = document_text[fragment.start_index:
                                    fragment.end_index]
    elif target_text_type == "plain":
        target_text = document_text[fragment.start_index_fragment_data:
                                    fragment.end_index_fragment_data]
    else:
        print("invalid target text type")
        target_text = ""
    # legacy, delete
    # if fragment.is_ascii:
    #     octavo_text = re.sub(r'[^\x00-\x7f]', r'', octavo_text)
    # octavo_text = ' '.join(octavo_text.split())
    if fragment_text == target_text:
        return True
    else:
        print("  !!  Validation error. fragtext, octavotext:")
        print(fragment_text)
        print("\n")
        print(target_text)
        return False


def get_document_text_splitjoined(
        document_text, ascii_search, splitjoin=False):
    if ascii_search:
        document_text = re.sub(r'[^\x00-\x7f]', r'', document_text)
    if splitjoin:
        document_text_stripped = ' '.join(document_text.split())
    else:
        document_text_stripped = document_text
    return document_text_stripped


def get_fragmentlist(cluster_data,
                     document_text_data,
                     ):
    print("> Getting fragment list ...")
    cluster_data_length = len(cluster_data) - 1
    fragment_list = []
    i = 0
    print("items in list: " + str(cluster_data_length))
    for item in cluster_data:
        if i % 50 == 0:
            print("Processing item: " + str(i) +
                  " / " + str(cluster_data_length))
            print("documentID: " + item.get('documentID'))
            print("fragmentID: " + str(item.get('fragmentID')))
        i = i + 1
        if 'positives_percent' in item.keys():
            pos_per = item['positives_percent']
        else:
            pos_per = None
        fragment = TextReuseFragment(ecco_id=item.get('documentID'),
                                     fragment_id=item.get('fragmentID'),
                                     text=item.get('text'),
                                     start_index=item.get('startIndex'),
                                     end_index=item.get('endIndex'),
                                     positives_percent=pos_per)
        # set octavo indices
        fragment.set_octavo_index(document_text_data.get('text'))
        if not fragment.octavo_indices_validate:
            print(fragment.error_id + " does not validate. Not added to list.")
        else:
            fragment_list.append(fragment)
    print("  >> Done!")
    return fragment_list


def get_validated_fragmentlist(fragmentlist, doc_plaintext, docid,
                               target_text_type="plain"):
    validated_fraglist = []
    print("validating fragdata")
    for fragment in fragmentlist:
        frag_validation = (
            test_fragment_text(
                fragment, doc_plaintext, target_text_type=target_text_type))
        if frag_validation:
            validated_fraglist.append(fragment)
        else:
            print("a fragment did not validate. ?!")
            print("fragment_id: " + str(fragment.fragment_id))
            print("ecco_id:     " + str(fragment.ecco_id))
    return validated_fraglist


def get_doctext_indexmap(orig_text, method_ascii):
    whitespace_chars = ['\t', '\n', '\x0b', '\x0c', '\r', '\x1c', '\x1d',
                        '\x1e', '\x1f', ' ', '\x85', '\xa0', '\u1680',
                        '\u2000', '\u2001', '\u2002', '\u2003', '\u2004',
                        '\u2005', '\u2006', '\u2007', '\u2008', '\u2009',
                        '\u200a', '\u2028', '\u2029', '\u202f', '\u205f',
                        '\u3000']
    orig_text_length = len(orig_text)
    # results to return. array (list) of fragmenttext_index,chars_lost
    fragment_indices = []
    chars_lost_list = []
    # cumlative index of characters lost
    chars_lost = 0
    # set flags
    # start with prev_char_hit -flag set to account for leading zeros
    prev_char_wshit = True
    this_char_wshit = False
    save_pending = False
    # iterate every char in text.
    # add entry to index if previous char was lost, and current isn't
    for char_index in range(0, orig_text_length):
        if method_ascii and ord(orig_text[char_index]) > 127:
            # getting non-ascii: if ord(char) is > 127
            chars_lost += 1
            save_pending = True
            continue
        if orig_text[char_index] in whitespace_chars:
            this_char_wshit = True
        else:
            this_char_wshit = False
        if prev_char_wshit and this_char_wshit:
            chars_lost += 1
            save_pending = True
        prev_char_wshit = this_char_wshit
        # save also at last char
        if (save_pending and not this_char_wshit) or \
           (save_pending and char_index == orig_text_length - 1):
            fragment_index = char_index - chars_lost
            fragment_indices.append(fragment_index)
            chars_lost_list.append(chars_lost)
            save_pending = False
    if method_ascii:
        method_description = "ascii"
    else:
        method_description = "unicode"
    results = {'method': method_description,
               'fragment_index': fragment_indices,
               'chars_lost': chars_lost_list}
    return results


class FragmentList(object):

    def __init__(self, cluster_data, seed_document_id=None):
        self.seed_document_id = seed_document_id  # not used ...
        # self.seed_fragment = None
        self.fragment_list = (
            self.get_initial_fragment_list(cluster_data))
        self.cluster_id_index = dict()
        self.document_id_index = dict()
        self.fragments_by_ecco_id = dict()
        self.set_doc_id_dict()

    def get_initial_fragment_list(self, cluster_data):
        print("  > Initializing fragment list ...")
        cluster_data_length = len(cluster_data)
        fragment_list = []
        print("items in list: " + str(cluster_data_length))
        for item in cluster_data:
            fragment = TextReuseFragment(ecco_id=item.get('documentID'),
                                         fragment_id=item.get('fragmentID'),
                                         text=item.get('text'),
                                         start_index=item.get('startIndex'),
                                         end_index=item.get('endIndex'))
            fragment_list.append(fragment)
            # AUG refractor-delete
            # if fragment.ecco_id == self.seed_docid:
            #     self.seed_fragment = fragment
        return fragment_list

    def set_doc_id_dict(self):
        for fragment in self.fragment_list:
            if fragment.ecco_id in self.fragments_by_ecco_id.keys():
                self.fragments_by_ecco_id[
                    fragment.ecco_id].append(fragment)
            else:
                self.fragments_by_ecco_id[fragment.ecco_id] = [
                    fragment]

    def remove_fragments_missing_correct_indices(self):
        correct_fragments = list()
        for fragment in self.fragment_list:
            if fragment.octavo_indices_validate:
                correct_fragments.append(fragment)
        self.fragment_list = correct_fragments
        self.fragments_by_ecco_id = dict()
        self.set_doc_id_dict()

    def set_octavo_indices(self):
        ecco_api_client = OctavoEccoClient()
        eebo_api_client = OctavoEeboClient()
        for document_id, fragment_list in self.fragments_by_ecco_id.items():
            # TODO: handle the notes somehow!
            if "_note_" in str(document_id):
                textdata = None
            elif len(document_id) < 10:
                textdata = eebo_api_client.get_text_for_document_id(
                    document_id).get('text')
            else:
                textdata = ecco_api_client.get_text_for_document_id(
                    document_id).get('text')
            for fragment in fragment_list:
                fragment.set_octavo_index(textdata)

    def set_cluster_id_index(self):
        i = 0
        for fragment in self.fragment_list:
            cluid = fragment.fragment_id
            fragment_index = i
            if cluid in self.cluster_id_index.keys():
                self.cluster_id_index[cluid].append(fragment_index)
            else:
                self.cluster_id_index[cluid] = [fragment_index]
            i += 1

    def __verify_cluster_index_built(self):
        if len(self.cluster_id_index) == 0:
            print("Building cluster id index.")
            self.set_cluster_id_index()

    def set_document_id_index(self):
        i = 0
        for fragment in self.fragment_list:
            docid = fragment.document_id
            fragment_index = i
            if docid in self.document_id_index.keys():
                self.document_id_index[docid].append(fragment_index)
            else:
                self.document_id_index[docid] = [fragment_index]
            i += 1

    def __verify_document_index_built(self):
        if len(self.document_id_index) == 0:
            print("Building document id index.")
            self.set_document_id_index()

    def add_metadata(self, author_metadata, good_metadata=None):
        if good_metadata is None:
            good_metadata_jsonfile = "data/metadata/good_metadata.json"
            good_metadata = load_good_metadata(good_metadata_jsonfile)
        print("  > Adding metadata to fragments.")
        for fragment in self.fragment_list:
            fragment.add_metadata(good_metadata, author_metadata)

    def add_headerdata(self, headerdata, document_id):
        print("  > Adding headerdata to matching fragments.")
        for fragment in self.fragment_list:
            if fragment.ecco_id == document_id:
                fragment.add_headerdata(headerdata)

    def get_unique_authors(self):
        unique_authors = set()
        for fragment in self.fragment_list:
            unique_authors.add(fragment.author)
        return unique_authors

    def get_unique_cluster_ids(self):
        self.__verify_cluster_index_built()
        unique_cluster_ids = self.cluster_id_index.keys()
        return unique_cluster_ids

    def get_fragments_of_cluster_id(self, cluster_id):
        self.__verify_cluster_index_built()
        filtered_list = []
        for fragment_index in self.cluster_id_index[cluster_id]:
            filtered_list.append(self.fragment_list[fragment_index])
        return filtered_list

    def get_unique_document_ids(self):
        self.__verify_document_index_built()
        unique_document_ids = self.document_id_index.keys()
        return unique_document_ids

    def get_fragments_of_document_id(self, document_id):
        self.__verify_document_index_built()
        filtered_list = []
        for fragment_index in self.document_id_index[document_id]:
            filtered_list.append(self.fragment_list[fragment_index])
        return filtered_list

    def get_length(self):
        return len(self.fragment_list)
