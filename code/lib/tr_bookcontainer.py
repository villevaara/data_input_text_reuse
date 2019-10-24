import time
import urllib
import sys
import os
import xml.etree.ElementTree as ET
import glob
from collections import OrderedDict
from bisect import bisect_left
# from lib.octavo_api_client import get_cluster_data_for_document_id
from lib.fragmentlists import get_fragmentlist
from lib.utils_common import path_is_empty, create_dir_if_not_exists
from random import random
from PIL import Image
from PIL import ImageDraw
from PIL import ImageFont
from fpdf import FPDF
import csv
from shutil import copy2


def gather_eccodata(doc_id, target_path, ecco_source_dict,
                    force_fetch=False):
    txt_target_path = target_path + "/" + str(doc_id) + "/fulltext"
    xml_target_path = target_path + "/" + str(doc_id) + "/xml"
    pagetxt_target_path = target_path + "/" + str(doc_id) + "/pagetexts"
    img_target_path = target_path + "/" + str(doc_id) + "/img"
    # create dirs if missing
    for path in [txt_target_path, xml_target_path,
                 pagetxt_target_path, img_target_path]:
        create_dir_if_not_exists(path)
    # get fulltxt file
    if path_is_empty(txt_target_path) or force_fetch:
        source_path = ecco_source_dict[doc_id]['path']
        sourcefiles = glob.glob(source_path + "/*.txt")
        for sourcefile in sourcefiles:
            copy2(sourcefile, (txt_target_path + "/"))
    # get pages from pouta with scp
    if path_is_empty(pagetxt_target_path) or force_fetch:
        source_path = ecco_source_dict[doc_id]['pouta_pages']
        os.system("scp -i ../../comhis.pem " +
                  source_path + "/* " +
                  pagetxt_target_path + "/.")
    # get xml from pouta with scp
    if path_is_empty(xml_target_path) or force_fetch:
        source_path = ecco_source_dict[doc_id]['pouta_xml']
        os.system("scp -i ../../comhis.pem " +
                  source_path + "/* " +
                  xml_target_path + "/.")


def write_plaintext_mismatch(text_plaintext, text_pages_combined, outputdir):
    outfile1 = outputdir + "text_plaintext.txt"
    outfile2 = outputdir + "text_pages_combined.txt"
    with open(outfile1, 'w') as outputtxt_file:
        outputtxt_file.write(text_plaintext)
    with open(outfile2, 'w') as outputtxt_file:
        outputtxt_file.write(text_pages_combined)


def filter_additional_cluster_data_with_docid(additional_cluster_data, docid):
    filtered_list = []
    for datapoint in additional_cluster_data:
        if datapoint.get('documentID') == docid:
            filtered_list.append(datapoint)
    return filtered_list


def get_local_ecco_fulltext_data(eccoid, local_eccodata_dir):
    fulltext_fname = (local_eccodata_dir + str(eccoid) + "/fulltext/" +
                      str(eccoid) + ".txt")
    with open(fulltext_fname, 'r') as fulltextfile:
        fulltext = fulltextfile.read()
    fulltext_data = {}
    fulltext_data['text'] = fulltext
    fulltext_data['collection'] = "local"
    return fulltext_data


def get_docid_fragments(docid_to_process,
                        cluster_api_client,
                        ecco_api_client,
                        field_eccocluster,
                        local_eccodata_dir=None,
                        additional_cluster_data=None):
    if cluster_api_client is not None:
        docid_clusterdata = (
            cluster_api_client.get_cluster_data_for_document_id(
                docid_to_process, fields=field_eccocluster))
    else:
        docid_clusterdata = list()
    #
    if additional_cluster_data is not None:
        relevant_add_cludata = filter_additional_cluster_data_with_docid(
            additional_cluster_data, docid_to_process)
        docid_clusterdata.extend(relevant_add_cludata)
    if local_eccodata_dir is not None:
        docid_fulltext_data = (
            get_local_ecco_fulltext_data(docid_to_process, local_eccodata_dir))
        docid_fulltext_data['collection'] = "ecco2"
    else:
        docid_fulltext_data = ecco_api_client.get_text_for_document_id(
            docid_to_process)
    #
    docid_fragments = get_fragmentlist(docid_clusterdata,
                                       docid_fulltext_data)
    return docid_fragments


class BookContainer(object):

    def __init__(self, octavo_id, ecco_api_client,
                 xml_img_page_datadir, fetch_images=False,
                 use_local_eccodata=False):
        self.octavo_id = str(octavo_id)
        self.ecco_api_client = ecco_api_client
        self.xml_img_page_datadir = xml_img_page_datadir
        self.plaintext = None
        self.plaintext_first_char_page_index = None
        self.page_header_lengths = {}
        self.octavo_collection = None
        self.fragment_list = None
        self.pagedata_dict = None
        self.section_headers = OrderedDict()
        self.section_highlight_stats = None
        self.document_highlight_stats = None
        self.pages_highlight_stats = None
        self.use_local_eccodata = use_local_eccodata
        self.snippet_data = None
        self.set_pagedata()
        self.set_plaintext()
        # sets plaintext_first_char_page_index and page_header_lengths
        self.set_fulltext_page_char_index()
        self.set_char_index_by_page_number()
        if fetch_images:
            self.fetch_document_images()

    def set_char_index_by_page_number(self):
        firstpage = min(self.plaintext_first_char_page_index.values())
        lastpage = max(self.plaintext_first_char_page_index.values())
        allpages = range(firstpage, lastpage)
        page_char_index_by_page = {}
        flipped_temp_dict = {}
        for key, value in self.plaintext_first_char_page_index.items():
            flipped_temp_dict[value] = key
        prev_char_index = flipped_temp_dict[1]
        for page_number in allpages:
            if page_number in flipped_temp_dict.keys():
                page_char_index_by_page[page_number] = (
                    flipped_temp_dict[page_number])
                prev_char_index = (
                    flipped_temp_dict[page_number])
            else:
                page_char_index_by_page[page_number] = prev_char_index
        self.first_char_page_index_by_page = page_char_index_by_page


    def set_pagedata(self):
        # [datadir]/[octavo_id]/xml/[octavo_id].xml
        xml_file_path = (self.xml_img_page_datadir + self.octavo_id + "/xml/" +
                         self.octavo_id + ".xml")
        if os.path.exists(xml_file_path):
            xml_data = ET.parse(xml_file_path)
        else:
            sys.exit(">>> ERROR! XML data does not exist for docid " +
                     self.octavo_id)
        pages = xml_data.find('text').findall('page')
        pagedict = OrderedDict()
        page_number = 1
        prev_section_header = "_FIRST_PAGE_"
        self.section_headers[page_number] = prev_section_header
        for page in pages:
            page_id = page.find('pageInfo').find('pageID').text
            section_headers = page.find('pageContent').findall('sectionHeader')
            # if page has multiple section headers, use last one
            if len(section_headers) > 0:
                prev_section_header = section_headers[-1].text
                self.section_headers[page_number] = prev_section_header
            page_img_id = page.find(
                'pageInfo').find('imageLink').text.split('.TIF')[0]
            page_img_file_path = (self.xml_img_page_datadir + self.octavo_id +
                                  "/img/" + page_img_id + ".png")
            page_paras = page.find('pageContent').findall('p')
            paralist = []
            pagewords_list = []
            pageword_order = 0
            page_tokenlist = []
            for paragraph in page_paras:
                paralist.append(paragraph)
                for word in paragraph.findall('wd'):
                    word_text = word.text
                    page_tokenlist.append(word_text)
                    word_pos_string_split = word.attrib.get('pos').split(',')
                    top_left_x = word_pos_string_split[0]
                    top_left_y = word_pos_string_split[1]
                    bottom_right_x = word_pos_string_split[2]
                    bottom_right_y = word_pos_string_split[3]
                    word_entry = {'text': word_text,
                                  'top_left_x': top_left_x,
                                  'top_left_y': top_left_y,
                                  'bottom_right_x': bottom_right_x,
                                  'bottom_right_y': bottom_right_y,
                                  'order': pageword_order,
                                  'highlight': False,
                                  'fragment_member': [],
                                  'fragment_start': [],
                                  'fragment_end': [],
                                  'positives_percent': None}
                    pageword_order += 1
                    pagewords_list.append(word_entry)
            page_entry = {
                'page_id': page_id,
                'page_img_id': page_img_id,
                'page_img_file_path': page_img_file_path,
                'page_words': pagewords_list,
                'page_number': page_number,
                'page_tokens': page_tokenlist,
                'page_prev_section_header': prev_section_header
            }
            pagedict[page_number] = page_entry
            page_number += 1
        self.pagedata_dict = pagedict

    def set_highlight_statistics(self):
        # total_hl_statistics = {}
        page_hl_statistics = OrderedDict()
        total_chars = 0
        total_hl_tokens = 0
        total_tokens = 0
        total_hl_chars = 0
        #
        section_hl_statistics = OrderedDict()
        for key, value in self.section_headers.items():
            section_hl_statistics[value] = {
                'page_number': key,
                'header': value,
                'tokens_total': 0,
                'tokens_highlighted': 0,
                'tokens_ratio': 0,
                'chars_total': 0,
                'chars_highlighted': 0,
                'chars_ratio': 0
            }
        #
        for page_i in self.pagedata_dict.keys():
            page = self.pagedata_dict[page_i]
            page_number = page['page_number']
            page_header = page['page_prev_section_header']
            page_total_tokens = len(page['page_tokens'])
            page_hl_tokens = 0
            page_total_chars = 0
            page_hl_chars = 0
            for word in page['page_words']:
                page_total_chars += len(word['text'])
                if word['highlight']:
                    page_hl_tokens += 1
                    page_hl_chars += len(word['text'])
            if page_total_chars == 0:
                page_char_hl_ratio = 0
            else:
                page_char_hl_ratio = page_hl_chars / page_total_chars
            if page_total_tokens == 0:
                page_token_hl_ratio = 0
            else:
                page_token_hl_ratio = page_hl_tokens / page_total_tokens
            page_hl_statistics[page_number] = {
                'header': page_header,
                'page_number': page_number,
                'tokens_total': page_total_tokens,
                'tokens_highlighted': page_hl_tokens,
                'tokens_ratio': page_token_hl_ratio,
                'chars_total': page_total_chars,
                'chars_highlighted': page_hl_chars,
                'chars_ratio': page_char_hl_ratio
            }
            # add section totals
            section_hl_statistics[page_header]['tokens_total'] += (
                page_total_tokens)
            section_hl_statistics[page_header]['tokens_highlighted'] += (
                page_hl_tokens)
            section_hl_statistics[page_header]['chars_total'] += (
                page_total_chars)
            section_hl_statistics[page_header]['chars_highlighted'] += (
                page_hl_chars)
            if section_hl_statistics[page_header]['tokens_total'] == 0:
                section_hl_statistics[page_header]['tokens_ratio'] = 0
            else:
                section_hl_statistics[page_header]['tokens_ratio'] = (
                    section_hl_statistics[page_header]['tokens_highlighted'] /
                    section_hl_statistics[page_header]['tokens_total'])
            if section_hl_statistics[page_header]['chars_total'] == 0:
                section_hl_statistics[page_header]['chars_ratio'] = 0
            else:
                section_hl_statistics[page_header]['chars_ratio'] = (
                    section_hl_statistics[page_header]['chars_highlighted'] /
                    section_hl_statistics[page_header]['chars_total'])
            # add to volume totals
            total_chars += page_total_chars
            total_hl_chars += page_hl_chars
            total_tokens += page_total_tokens
            total_hl_tokens += page_hl_tokens
        doc_char_ratio = total_hl_chars / total_chars
        doc_token_ratio = total_hl_tokens / total_tokens
        total_hl_statistics = {
            'chars_total': total_chars,
            'chars_highlighted': total_hl_chars,
            'chars_ratio': doc_char_ratio,
            'tokens_total': total_tokens,
            'tokens_highlighted': total_hl_tokens,
            'tokens_ratio': doc_token_ratio
        }
        self.document_highlight_stats = total_hl_statistics
        self.pages_highlight_stats = page_hl_statistics
        self.section_highlight_stats = section_hl_statistics

    def write_highlight_stats(self, outpath, fname_prefix=""):
        # csv_fname_doc_hl = (outpath + fname_prefix + self.octavo_id +
        #                     "_highlighted_document.csv")
        # csv_fname_sec_hl = (outpath + fname_prefix + self.octavo_id +
        #                     "_highlighted_sections.csv")
        csv_fname_pag_hl = (outpath + fname_prefix + self.octavo_id +
                            "_highlighted_pages.csv")
        with open(csv_fname_pag_hl, 'w') as csv_outfile:
            fieldnames = list(self.pages_highlight_stats[1].keys())
            writer = csv.DictWriter(csv_outfile, fieldnames=fieldnames)
            writer.writeheader()
            for value in self.pages_highlight_stats.values():
                writer.writerow(value)

    def fetch_document_images(self):
        for page_number in self.pagedata_dict.keys():
            page_img_file_path = (
                self.pagedata_dict[page_number]['page_img_file_path'])
            gale_img_id = self.pagedata_dict[page_number]['page_img_id']
            if not os.path.isfile(page_img_file_path):
                time.sleep(random() * 2)
                gale_url = ("http://callisto.ggsrv.com/imgsrv/FastFetch/UBER2/"
                            + gale_img_id + "?legacy=no&format=web")
                print("fetching img_file from Gale URL: " + gale_url)
                urllib.request.urlretrieve(gale_url, page_img_file_path)

    def set_plaintext(self):
        if self.use_local_eccodata:
            plaintext_data = get_local_ecco_fulltext_data(
                self.octavo_id, self.xml_img_page_datadir)
        else:
            plaintext_data = self.ecco_api_client.get_text_for_document_id(
                self.octavo_id)
        self.plaintext = plaintext_data.get('text')
        self.octavo_collection = plaintext_data.get('collection')

    def set_fragment_list(self, fragment_list):
        self.fragment_list = fragment_list

    def find_page_token_indices(self, search_string, page_number,
                                strip_ends=True):
        page_to_search = self.pagedata_dict[page_number]
        tokens_to_search_for = search_string.strip().split()
        if strip_ends:
            tokens_to_search_for = tokens_to_search_for[1:-1]
        tokens_to_search_in = page_to_search.get('page_tokens')
        start_i = -1
        end_i = -1
        for i in range(0, len(tokens_to_search_in)):
            if (tokens_to_search_in[i:i + len(tokens_to_search_for)]
                    == tokens_to_search_for):
                start_i = i
                end_i = i + len(tokens_to_search_for)
                break
        return {"start": start_i, "end": end_i}

    def get_page_number_for_char_i(self, char_i):
        indices = list(self.plaintext_first_char_page_index.keys())
        index_number = bisect_left(indices, char_i) - 1
        page_number = self.plaintext_first_char_page_index[
            indices[index_number]]
        return page_number

    def get_page_first_char_index(self, pagenumber):
        # add condition for pages containing header data.
        # return page first char index minus length of header data
        return self.first_char_page_index_by_page[pagenumber]
        # return list(
        #     self.plaintext_first_char_page_index.keys())[pagenumber - 1]

    def get_page_last_char_index(self, pagenumber):
        fulltext_length = len(self.plaintext)
        if pagenumber < len(self.first_char_page_index_by_page):
            return self.first_char_page_index_by_page[pagenumber] - 1
        else:
            return (fulltext_length - 1)

    def get_snippet_data(self, snip_start, snip_end):
        snip_first_page = self.get_page_number_for_char_i(snip_start)
        snip_last_page = self.get_page_number_for_char_i(snip_end)
        snip_page_range = list(range(snip_first_page, snip_last_page + 1))
        snip_page_dict = OrderedDict()
        #
        for page_number in snip_page_range:
            page_header_length = self.page_header_lengths[page_number]
            page_first_char_index = self.get_page_first_char_index(page_number)
            page_first_char_index += page_header_length
            page_last_char_index = self.get_page_last_char_index(page_number)
            if snip_end < page_last_char_index:
                page_snip_end = snip_end - 1
            else:
                page_snip_end = page_last_char_index
            if snip_start >= page_first_char_index:
                page_snip_start = snip_start
            else:
                page_snip_start = page_first_char_index
            page_snip = self.plaintext[page_snip_start:(page_snip_end + 1)]
            snip_page_dict[page_number] = {
                'snip_start': page_snip_start,
                'snip_end': page_snip_end,
                'snip_text': page_snip
            }
        return snip_page_dict

    def get_snippet_first_and_last_page(self, snippet_data):
        first_page_num = min(list(snippet_data.keys()))
        last_page_num = max(list(snippet_data.keys()))
        return {'first_page': first_page_num, 'last_page': last_page_num}

    def set_token_fragment_data(self):
        for fragment in self.fragment_list:
            # use "original", not octavo indices here
            fragment_snip_page_dict = self.get_snippet_data(
                fragment.start_index_fragment_data,
                fragment.end_index_fragment_data)
            snip_page_lims = self.get_snippet_first_and_last_page(
                fragment_snip_page_dict)
            #
            for page_number in fragment_snip_page_dict.keys():
                page_snip_data = fragment_snip_page_dict[page_number]
                page_snip_text = page_snip_data['snip_text']
                page_snip_token_indices = (
                    self.find_page_token_indices(page_snip_text, page_number))
                if (page_snip_token_indices['start'] != -1 and
                        page_snip_token_indices['end'] != -1):
                    for page_token_i in range(
                            (page_snip_token_indices['start'] - 1),
                            (page_snip_token_indices['end'] + 1)):
                        self.pagedata_dict[page_number]['page_words'][
                            page_token_i]['fragment_member'].append(
                                fragment.fragment_id)
                        # positives percent gets overwritten by last value
                        self.pagedata_dict[page_number]['page_words'][
                            page_token_i]['positives_percent'] = (
                                fragment.positives_percent)
                        if page_number == snip_page_lims['first_page']:
                            if page_token_i == (
                                    page_snip_token_indices['start'] - 1):
                                self.pagedata_dict[page_number]['page_words'][
                                    page_token_i]['fragment_start'].append(
                                        fragment.fragment_id)
                        if page_number == snip_page_lims['last_page']:
                            if page_token_i == (
                                    page_snip_token_indices['end']):
                                self.pagedata_dict[page_number]['page_words'][
                                    page_token_i]['fragment_end'].append(
                                        fragment.fragment_id)
                else:
                    print("-----------------")
                    print("Skipped fragment not found in XML:")
                    print(fragment.text)
                    print("Page: " + str(page_number))
        # fragment_strength
        # fragment_other_book_ids

    def set_token_highlights(self, invert=False):
        #
        for fragment in self.fragment_list:
            # use "original", not octavo indices here
            fragment_snip_page_dict = self.get_snippet_data(
                fragment.start_index_fragment_data,
                fragment.end_index_fragment_data)
            #
            for page_number in fragment_snip_page_dict.keys():
                page_snip_data = fragment_snip_page_dict[page_number]
                page_snip_text = page_snip_data['snip_text']
                page_snip_token_indices = (
                    self.find_page_token_indices(page_snip_text, page_number))
                if (page_snip_token_indices['start'] != -1 and
                        page_snip_token_indices['end'] != -1):
                    for page_token_i in range(
                            (page_snip_token_indices['start'] - 1),
                            (page_snip_token_indices['end'] + 1)):
                        self.pagedata_dict[page_number]['page_words'][
                            page_token_i]['highlight'] = True
                else:
                    print("-----------------")
                    print("Skipped fragment not found in XML:")
                    print(fragment.text)
                    print("Page: " + str(page_number))
        if invert:
            for page_number in self.pagedata_dict.keys():
                for page_word in self.pagedata_dict[
                        page_number]['page_words']:
                    page_word['highlight'] = not page_word['highlight']

    def get_page_header_length(self, pagetext_list):
        header_length = 0
        # if first char of first line is # start counting header length
        if len(pagetext_list) == 0:
            return header_length
        if pagetext_list[0][0] == "#":
            for line in pagetext_list:
                if line[0] == "#" or line[0] == "\n":
                    header_length += len(line)
                else:
                    break
        return header_length

    def filter_pagetext_list_headers(self, pagetext_list):
        filtered_list = list()
        potential_skipline = ""

        for line in pagetext_list:

            if line[0:2] == "# " or line[0:3] == "## ":
                potential_skipline = line

            elif len(potential_skipline) != 0:
                if line != "\n":
                    filtered_list.append(potential_skipline)
                    filtered_list.append(line)
                potential_skipline = ""

            else:
                filtered_list.append(line)

        return filtered_list

    def set_fulltext_page_char_index(self):
        docid_text_pages_dir = (self.xml_img_page_datadir + self.octavo_id +
                                "/pagetexts/")
        pagefiles = glob.glob(docid_text_pages_dir + "*_page*.txt")
        pagefiledict = {}
        lastpage = 0
        #
        for pagefile in pagefiles:
            pagenumber = int(
                pagefile.split('/')[-1].split('page')[-1].split('.txt')[0])
            if pagenumber > lastpage:
                lastpage = pagenumber
            pagefiledict[pagenumber] = pagefile
        combined_text_list = []
        first_char_page_index = OrderedDict()
        chars_processed = 0
        #
        for pagenumber in list(range(1, (lastpage + 1))):
            with open(pagefiledict.get(pagenumber), 'r') as pagefile:
                pagetext_list = list(pagefile)
                pagetext_list_filtered = self.filter_pagetext_list_headers(
                    pagetext_list)
                pagetext = ''.join(pagetext_list_filtered)
                self.page_header_lengths[pagenumber] = (
                    # OBS! Depends on the full text having, or not having
                    # embedded headers.
                    # self.get_page_header_length(pagetext_list))
                    self.get_page_header_length(pagetext_list_filtered))
            if len(pagetext) == 0:
                continue
            first_char_page_index[chars_processed] = pagenumber
            chars_processed += len(pagetext)
            combined_text_list.append(pagetext)
        #
        # combined_text = "".join(combined_text_list).strip()
        combined_text = "".join(combined_text_list)
        if self.plaintext == combined_text:
            self.plaintext_first_char_page_index = first_char_page_index
        else:
            write_plaintext_mismatch(
                self.plaintext, combined_text,
                "/home/vvaara/projects/comhis/text-reuse/temp/")
            sys.exit(">>> ERROR! Page indices do not match for docid " +
                     self.octavo_id)

    def get_page_highlight_img(self, page_number, img):
        word_data = self.pagedata_dict[page_number]['page_words']
        tmp_img = Image.new('RGBA', img.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(tmp_img)  # , mode="RGBA"
        #
        for word in word_data:
            if word['highlight']:
                x1 = int(word['top_left_x'])
                y1 = int(word['top_left_y'])
                x2 = int(word['bottom_right_x'])
                y2 = int(word['bottom_right_y'])
                colour = (170, 210, 131, 180)
                draw.rectangle(((x1, y1), (x2, y2)), fill=colour)
        #
        im_out = Image.alpha_composite(img, tmp_img)
        # white to transparent
        img_blacktext = img.convert("RGBA")
        datas = img_blacktext.getdata()
        #
        newData = []
        for item in datas:
            if item[0] == 255 and item[1] == 255 and item[2] == 255:
                newData.append((255, 255, 255, 0))
            else:
                newData.append(item)
        #
        img_blacktext.putdata(newData)
        im_final_composite = Image.alpha_composite(im_out, img_blacktext)
        im_final_out = im_final_composite.convert("RGB")
        return im_final_out

    def get_page_highlight_img2(self, page_number, img):
        word_data = self.pagedata_dict[page_number]['page_words']
        tmp_img = Image.new('RGBA', img.size, (0, 0, 0, 0))
        text_img = Image.new('RGBA', img.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(tmp_img)  # , mode="RGBA"
        textdraw = ImageDraw.Draw(text_img)  # , mode="RGBA"
        font = ImageFont.truetype(
            "/usr/share/fonts/truetype/freefont/FreeMonoBold.ttf",
            44, encoding="unic")
        #
        for word in word_data:
            x1 = int(word['top_left_x'])
            y1 = int(word['top_left_y'])
            x2 = int(word['bottom_right_x'])
            y2 = int(word['bottom_right_y'])
            if len(word['fragment_member']) > 0:
                # make alpha depend on positives_percent
                alpha_val = int(4 * (word['positives_percent'] - 40))
                colour = (170, 210, 131, alpha_val)
                draw.rectangle(((x1, y1), (x2, y2)), fill=colour)
            else:
                colour = (184, 94, 59, 120)
                draw.rectangle(((x1, y1), (x2, y2)), fill=colour)
            # draw line at start if first word of fragment
            if len(word['fragment_end']) > 0:
                # alpha (last) value 0-255 with 255 no transparent
                colour = (74, 92, 56, 255)
                draw.line([(x2 - 10, y1),
                           (x2, y1),
                           (x2, y2),
                           (x2 - 10, y2)], fill=colour, width=4)
            if len(word['fragment_start']) > 0:
                # alpha (last) value 0-255 with 255 no transparent
                colour = (74, 92, 56, 255)
                draw.line([(x1 + 10, y1),
                           (x1, y1),
                           (x1, y2),
                           (x1 + 10, y2)], fill=colour, width=4)
                title_text = ("f_id: " + word['fragment_start'][0] + " -- " +
                              "pp: " + str(word['positives_percent']) + "%")
                text_x = x1 + 13
                text_y = max([0, (y1 - 20)])
                text_outline_col = (0, 0, 0)
                out_off = 3
                text_col = (255, 255, 255)
                # draw outline
                textdraw.text((text_x + out_off, text_y + out_off), title_text,
                              align="right", fill=text_outline_col, font=font)
                textdraw.text((text_x + out_off, text_y - out_off), title_text,
                              align="right", fill=text_outline_col, font=font)
                textdraw.text((text_x - out_off, text_y + out_off), title_text,
                              align="right", fill=text_outline_col, font=font)
                textdraw.text((text_x - out_off, text_y - out_off), title_text,
                              align="right", fill=text_outline_col, font=font)
                #
                textdraw.text((text_x, text_y - out_off), title_text,
                              align="right", fill=text_outline_col, font=font)
                textdraw.text((text_x, text_y + out_off), title_text,
                              align="right", fill=text_outline_col, font=font)
                textdraw.text((text_x + out_off, text_y), title_text,
                              align="right", fill=text_outline_col, font=font)
                textdraw.text((text_x - out_off, text_y), title_text,
                              align="right", fill=text_outline_col, font=font)
                # draw text
                textdraw.text((text_x, text_y), title_text,
                              align="right", fill=text_col, font=font)
                # TODO write fragment id here
                # TODO write positives percent
            # draw line at end if last word of fragment
        #
        im_out = Image.alpha_composite(img, tmp_img)
        # white to transparent
        img_blacktext = img.convert("RGBA")
        datas = img_blacktext.getdata()
        #
        newData = []
        for item in datas:
            if item[0] == 255 and item[1] == 255 and item[2] == 255:
                newData.append((255, 255, 255, 0))
            else:
                newData.append(item)
        #
        img_blacktext.putdata(newData)
        im_final_comp = Image.alpha_composite(im_out, img_blacktext)
        im_final_comp = Image.alpha_composite(im_final_comp, text_img)
        im_final_out = im_final_comp.convert("RGB")
        return im_final_out

    def write_highlight_pdf(self, pdf_fname, temp_workdir="./work/img/"):
        print("Creating PDF: " + pdf_fname)
        pdf = FPDF()
        max_page = len(self.pagedata_dict.keys())
        for page_number in self.pagedata_dict.keys():
            print("Page: " + str(page_number) + " / " + str(max_page),
                  end="\r", flush=True)
            img_file_path = (
                self.pagedata_dict[page_number]['page_img_file_path'])
            img = Image.open(img_file_path).convert("RGBA")
            page_highligh_img = self.get_page_highlight_img2(page_number, img)
            img_savename = temp_workdir + str(page_number) + ".png"
            page_highligh_img.save(img_savename, "PNG")
            pdf.add_page()
            # test image width and height, and scale for A4 accordingly
            if img.height / img.width > 1.414:
                pdf.image(img_savename, 0, 0, h=297)
            else:
                pdf.image(img_savename, 0, 0, w=210)
        print("")
        pdf.output(pdf_fname, 'F')
