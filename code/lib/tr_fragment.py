import sys

from bisect import bisect_left

from lib.headerdata import (
    get_header_and_index_for_textindex,
    )

from lib.indexmapping import get_index_for_header_enclosing_fragment

from lib.text_reuse_common import (
    get_location_from_estc,
    get_author_from_estc,
    get_year_from_estc,
    get_estcid_from_estc,
    get_title_from_estc,
    get_author_bd_from_estc,
    get_country_from_estc,
    get_first_ed_year_guess_from_estc
    )


class TextReuseFragment(object):

    def __init__(self, ecco_id, fragment_id,
                 text, start_index, end_index, positives_percent=None):
        self.estc_id = None
        self.collection = self.__get_collection(ecco_id)
        self.ecco_id = self.__get_proper_ecco_id(ecco_id)
        self.fragment_id = str(fragment_id)
        self.error_id = "doc: " + self.ecco_id + " frag: " + self.fragment_id
        self.title = None
        self.author = None
        self.author_birth = None
        self.author_death = None
        self.author_politics = None
        self.year = None
        self.text = text
        self.location = None
        self.country = None
        self.start_index_fragment_data = start_index
        self.end_index_fragment_data = end_index
        self.start_index = None
        self.end_index = None
        self.octavo_indices_validate = None
        self.text_before = None
        self.text_after = None
        self.document_length = None
        self.preceding_header = None
        self.preceding_header_index = -1
        self.document_collection = None
        # self.is_ascii = None
        self.seed_document_id = None
        self.seed_header_id = None
        self.seed_header_text = None
        self.seed_header_start_index = None
        self.seed_header_end_index = None
        self.seed_ref_length = None
        self.seed_uid = None
        self.seed_uid_simple_author = None
        self.first_ed_year_guess = None
        self.positives_percent = positives_percent

    def __get_collection(self, document_id):
        if document_id[0] == 'A':
            return "eebo"
        elif document_id[0] == 'B':
            return "eebo"
        else:
            return "ecco"

    def __get_proper_ecco_id(self, ecco_id):
        if ecco_id[0] == 'A':
            ecco_id = "10" + ecco_id[1:len(ecco_id)]
        elif ecco_id[0] == 'B':
            ecco_id = "11" + ecco_id[1:len(ecco_id)]
        return ecco_id

    def set_octavo_index(self, octavo_text):
        if octavo_text is None:
            self.octavo_indices_validate = False
        else:
            self.set_simple_find_index(octavo_text)
            if not self.octavo_indices_validate:
                header_rem_indices = get_index_for_header_enclosing_fragment(
                    octavo_text, self.text)
                if header_rem_indices['validates']:
                    print("found with header search.")
                    self.start_index = header_rem_indices['start_index']
                    self.end_index = header_rem_indices['end_index']
                    self.octavo_indices_validate = True
                else:
                    print(self.error_id +
                          " not found with header strip search either.")

    def set_simple_find_index(self, octavo_text):
        if octavo_text is None:
            self.octavo_indices_validate = False
        else:
            start_index = octavo_text.find(self.text)
            if start_index == -1:
                print(self.error_id +
                      " not found with string search.")
                self.octavo_indices_validate = False
            else:
                self.start_index = start_index
                self.end_index = (self.start_index + len(self.text))
                validates = (octavo_text[self.start_index:self.end_index] ==
                             self.text)
                if not validates:
                    print(self.error_id + " texts do not match. weird.")
                    self.octavo_indices_validate = False
                else:
                    self.octavo_indices_validate = True

    def set_seed_data(self, seed_data):
        self.seed_document_id = seed_data.get('seed_document_id')
        self.seed_header_id = seed_data.get('seed_header_id')
        self.seed_header_text = seed_data.get('seed_header_text')
        self.seed_header_start_index = seed_data.get('seed_header_start_index')
        self.seed_header_end_index = seed_data.get('seed_header_end_index')
        self.seed_ref_length = (
            int(self.seed_header_end_index) -
            int(self.seed_header_start_index))
        self.seed_uid = (
            self.ecco_id + "_" +
            str(round(self.seed_header_start_index / 100)) + "_" +
            str(round(self.seed_header_start_index / 100)))
        self.seed_uid_simple_author = (
            self.author + "_" +
            str(round(self.seed_header_start_index / 100)))

    def add_metadata(self, good_metadata, author_metadata):
        if self.ecco_id in good_metadata.keys():
            self.location = get_location_from_estc(
                self.ecco_id, good_metadata)
            self.country = get_country_from_estc(
                self.ecco_id, good_metadata)
            self.author = get_author_from_estc(
                self.ecco_id, good_metadata)
            self.estc_id = get_estcid_from_estc(
                self.ecco_id, good_metadata)
            self.year = get_year_from_estc(
                self.ecco_id, good_metadata)
            self.title = get_title_from_estc(
                self.ecco_id, good_metadata)
            bd = get_author_bd_from_estc(self.ecco_id, good_metadata)
            self.author_birth = bd.get('birth')
            self.author_death = bd.get('death')
            self.first_ed_year_guess = get_first_ed_year_guess_from_estc(
                self.ecco_id, good_metadata)
            if self.first_ed_year_guess is None:
                self.first_ed_year_guess = (
                    self.__get_guessed_first_ed_year())
            self.author_politics = (
                self.get_author_political_affiliation(author_metadata))
        else:
            print(str(self.ecco_id) + " not in good metadata.")

    def get_author_political_affiliation(self, author_metadata):
        authordata = author_metadata.get(self.author)
        if authordata is None:
            return "no_record"
        affiliation = authordata.get('political_views')
        if affiliation == "":
            return "no_record"
        else:
            return affiliation

    def __get_guessed_first_ed_year(self):
        if self.author_birth is not None:
            first_ed_year_guess = int(self.author_birth) + 45
        elif self.author_death is not None:
            first_ed_year_guess = int(self.author_death) - 10
        else:
            first_ed_year_guess = 9999
        if first_ed_year_guess > self.year:
            return self.year
        else:
            return first_ed_year_guess

    def add_headerdata(self, headerdata):
        if self.start_index is not None:
            header_search_index = self.start_index
            header_ti = get_header_and_index_for_textindex(
                header_search_index, headerdata)
            self.preceding_header = header_ti.get('text')
            self.preceding_header_index = header_ti.get('index')
        else:
            self.preceding_header = None
            self.preceding_header_index = None

    # # old, refractor and delete
    # def __context_search(self, search_text, document_text_data, ascii_search):
    #     if ascii_search:
    #         document_text = document_text_data.get('splitjoined_ascii')
    #     else:
    #         document_text = document_text_data.get('splitjoined_unicode')
    #     # document_text_stripped = ' '.join(document_text.split())
    #     fragment_start_index = document_text.find(search_text)
    #     if fragment_start_index == -1:
    #         print(" !! testing lowercase")
    #         fragment_start_index = document_text.lower().find(search_text)
    #         if fragment_start_index != -1:
    #             print(" >>>>> lowercase found")
    #             print(self.fragment_id)
    #     return fragment_start_index

    # # old, refractor and delete
    # def set_fragment_encoding(self, document_text_data):

    #     document_collection = document_text_data.get('collection')

    #     if document_collection == "ecco1":
    #         ascii_search = True
    #     else:
    #         ascii_search = False

    #     retried = False

    #     while True:
    #         fragment_start_index = self.__context_search(self.text,
    #                                                      document_text_data,
    #                                                      ascii_search)

    #         if fragment_start_index != -1:
    #             self.find_start_index = fragment_start_index
    #             self.find_end_index = (fragment_start_index +
    #                                    len(self.text))
    #             if ascii_search:
    #                 self.is_ascii = True
    #             else:
    #                 self.is_ascii = False
    #             return self.is_ascii
    #         elif (fragment_start_index == -1) and (retried is True):
    #             print(" > !! Fragment still not found." +
    #                   " No encoding set.")
    #             self.is_ascii = None
    #             # return None
    #             break
    #         elif (fragment_start_index == -1) and (retried is False):
    #             print(" > docid: " + str(self.ecco_id) +
    #                   " cluid: " + str(self.fragment_id) +
    #                   " Fragment not found." +
    #                   " Trying the other method (ascii/unicode)")
    #             ascii_search = not ascii_search
    #             retried = True

    # # old, refractor
    # def find_octavo_index(self, index, octavo_indexmap):
    #     fragment_indices = octavo_indexmap.get('fragment_index')
    #     chars_lost = octavo_indexmap.get('chars_lost')

    #     index_find_pos = bisect_left(fragment_indices, index)

    #     # if fragment index is the last index
    #     if index_find_pos == len(fragment_indices):
    #         return index + chars_lost[index_find_pos - 1]
    #     # if index found is before first addition
    #     if index_find_pos == 0 and fragment_indices[index_find_pos] > index:
    #         return index
    #     if fragment_indices[index_find_pos] == index:
    #         return index + chars_lost[index_find_pos]
    #     if fragment_indices[index_find_pos] > index:
    #         return index + chars_lost[index_find_pos - 1]

    #     print("Index not found! WTF.")
    #     return -1

    # # old, refractor
    # def set_octavo_indices(self, octavo_indexmap, orig_index=True):
    #     if orig_index:
    #         start_i = self.start_index
    #         end_i = self.end_index
    #     else:
    #         start_i = self.find_start_index
    #         end_i = self.find_end_index
    #     self.octavo_start_index = (
    #         self.find_octavo_index(start_i, octavo_indexmap))
    #     self.octavo_end_index = (
    #         self.find_octavo_index(end_i, octavo_indexmap))

    def get_context_text(self,
                         context_position,
                         document_text,
                         window_size):
        if context_position == 'before':
            context_start = self.start_index - window_size
            context_end = self.start_index
        elif context_position == 'after':
            context_start = self.end_index
            context_end = self.end_index + window_size
        else:
            sys.exit("Invalid context position!")
        context_text = document_text[context_start:context_end]
        return context_text
