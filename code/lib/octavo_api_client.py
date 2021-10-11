import sys
from requests import get, post
from time import sleep


class OctavoAPIClient(object):

    def __init__(self,
                 # username,
                 # password,
                 api_base_address="https://hume:bacon@vm0824.kaj.pouta.csc.fi/octavo",
                 limit=100,
                 timeout=60):
        # self.username = username
        # self.password = password
        self.api_base_address = api_base_address
        self.limit = limit
        self.timeout = timeout
        self.limit_timeout_part = (
            "&limit=" +
            str(limit) +
            "&timeout=" +
            str(timeout))

    def get_fields_request_part(self, fields):
        fields_string = ""
        if len(fields) > 0:
            for field in fields:
                fields_string = (fields_string + "&field=" + field)
        return fields_string

    def announce_query(self, api_request):
        print("Querying API with: " + api_request)

    def call_api_with_get_and_forget(self, api_request):
        get(api_request, timeout=0.01)

    def get_api_response(self, api_request, max_retries=20):
        retries = 0
        while retries < max_retries:
            try:
                response = get(api_request)
                # response = post(api_request)
                data = response.json()
                # data = response.json().get('result').get('docs')
            except ValueError as error:
                # print(error)
                print("  !> Request probably timed out or something." +
                      " Retrying in 8 secs. Retries: " + str(retries) + "/" +
                      str(max_retries))
                sleep(8)
                retries += 1
                if retries == max_retries:
                    sys.exit("  !!> Max retries reached for request.")
                continue
            else:
                break
        return data

    def post_api_response(self, api_post_request,
                          api_post_request_data,
                          max_retries=20):
        retries = 0
        while retries < max_retries:
            try:
                response = post(api_post_request, api_post_request_data)
                data = response.json()
            except ValueError as error:
                print("  !> Request probably timed out or something." +
                      " Retrying in 8 secs. Retries: " + str(retries) + "/" +
                      str(max_retries))
                sleep(8)
                retries += 1
                if retries == max_retries:
                    sys.exit("  !!> Max retries reached for request.")
                continue
            else:
                break
        return data


class OctavoEccoClusterClient(OctavoAPIClient):

    def __init__(self, limit=-1, timeout=60):
        super().__init__(limit=limit, timeout=timeout)
        self.api_request_start = (
            self.api_base_address +
            "/reuse" +
            "/search?query=")
        self.api_post_request_uri = (
            self.api_base_address +
            "/reuse" +
            "/search")

    def get_cluster_data_for_document_id_url(self,
                                             document_id,
                                             fields):

        fields_part = self.get_fields_request_part(fields)

        api_request = (
            self.api_request_start +
            "documentID:" +
            str(document_id) +
            fields_part +
            self.limit_timeout_part)

        return api_request

    def get_cluster_data_for_document_id(self,
                                         document_id,
                                         fields=["documentID",
                                                 # "title",
                                                 "fragmentID",
                                                 "startIndex",
                                                 "endIndex",
                                                 # "avgLength",
                                                 "text"]):
        cluster_ids = self.get_cluster_ids_list_for_document_id(
            document_id)
        cluster_data = self.get_cluster_data_for_cluster_id_list(
            cluster_id_list=cluster_ids,
            document_id=document_id,
            fields=fields)
        return cluster_data

    def get_fragment_ids_for_document_id_list(self, document_id_list):
        fragment_ids = set()
        for document_id in document_id_list:
            api_request = (
                self.api_request_start +
                "<REUSE§documentID:" +
                str(document_id) +
                "§REUSE>" +
                "&field=fragmentID" +
                self.limit_timeout_part)
            self.announce_query(api_request)
            response_json = self.get_api_response(api_request)
            data = response_json.get('result').get('docs')
            for entry in data:
                fragment_ids.add(entry['fragmentID'])
        fragment_ids = list(fragment_ids)
        return fragment_ids

    def get_cluster_ids_list_for_document_id(self,
                                             document_id,
                                             fields=["fragmentID"]):
        fields_part = self.get_fields_request_part(fields)
        api_request = (
            self.api_request_start +
            "<REUSE§documentID:" +
            str(document_id) +
            "§REUSE>" +
            fields_part +
            self.limit_timeout_part)
        self.announce_query(api_request)
        response_json = self.get_api_response(api_request)
        data = response_json.get('result').get('docs')
        cluster_ids = []
        for entry in data:
            cluster_ids.append(str(entry.get('fragmentID')))
        return cluster_ids

    def get_cluster_data_for_cluster_id_list(self,
                                             cluster_id_list,
                                             document_id=None,
                                             fields=["documentID",
                                                     "fragmentID",
                                                     "startIndex",
                                                     "endIndex",
                                                     "text"]):
        print(" > " + str(len(cluster_id_list)) + " fragmentIDs in total")
        print(" > slicing list into 200 item parts")
        cluster_id_groups = []
        for i in range(0, len(cluster_id_list), 200):
            slice_start = i
            if i + 200 <= len(cluster_id_list):
                slice_end = i + 200
            else:
                slice_end = len(cluster_id_list)
            cluster_id_groups.append(cluster_id_list[slice_start:slice_end])

        all_data = []
        for cluster_id_group in cluster_id_groups:
            cluster_id_string = (
                ' OR '.join(cluster_id_group))
            cluster_id_part = "fragmentID:(" + cluster_id_string + ")"
            if document_id is not None:
                cluster_id_part += " AND documentID:" + str(document_id)
            api_post_request_data = {'query': cluster_id_part,
                                     'field': fields,
                                     'limit': self.limit,
                                     'timeout': self.timeout}
            self.announce_query(self.api_post_request_uri)
            response_json = self.post_api_response(self.api_post_request_uri,
                                                   api_post_request_data,
                                                   max_retries=40)
            response_data = response_json.get('result').get('docs')
            all_data.extend(response_data)
        return all_data

    def get_wide_cluster_data_for_document_id(self,
                                              document_id,
                                              fields=["documentID",
                                                      # "title",
                                                      "fragmentID",
                                                      "startIndex",
                                                      "endIndex",
                                                      "text"]):

        fields_part = self.get_fields_request_part(fields)

        api_request = (
            self.api_request_start +
            "<REUSE<REUSE§documentID:" +
            str(document_id) +
            "§fragmentID>§REUSE>" +
            fields_part +
            self.limit_timeout_part)

        self.announce_query(api_request)
        # response = get(api_request)
        # data = response.json().get('result').get('docs')
        data = self.get_api_response(api_request).get('result').get('docs')
        return data


class OctavoEeboClient(OctavoAPIClient):

    def __init__(self, limit=-1, timeout=60):
        super().__init__(limit=limit, timeout=timeout)
        self.api_request_start = (
            self.api_base_address +
            "/eebo" +
            "/search?query=")

    def get_text_for_document_id(self, document_id):
        api_request = (self.api_request_start +
                       "<DOCUMENT§documentID:" +
                       str(document_id) +
                       "§DOCUMENT>&field=content&field=collectionID" +
                       self.limit_timeout_part)

        self.announce_query(api_request)
        response_json = self.get_api_response(api_request)
        text = response_json.get('result').get('docs')[0].get('content')
        collection = (
            response_json.get('result').get('docs')[0].get('collectionID'))
        retdict = {'text': text, 'collection': collection}
        return retdict


class OctavoEccoClient(OctavoAPIClient):

    def __init__(self, limit=-1, timeout=60):
        super().__init__(limit=limit, timeout=timeout)
        self.api_request_start = (
            self.api_base_address +
            "/ecco" +
            "/search?query=")

    def get_doc_ids_for_author(self, author):
        repl_chars = [" ", "(", ")"]
        author_str = author
        for repl_char in repl_chars:
            author_str = author_str.replace(repl_char, "?")
        api_request = (self.api_request_start +
                       "<DOCUMENT§author:" +
                       author_str +
                       "§DOCUMENT>&field=documentID" +
                       self.limit_timeout_part)
        self.announce_query(api_request)
        data = self.get_api_response(api_request).get('result').get('docs')
        doc_ids = []
        for entry in data:
            doc_ids.append(entry['documentID'])
        return doc_ids

    def get_text_for_document_id(self, document_id):
        api_request = (self.api_request_start +
                       "<DOCUMENT§documentID:" +
                       str(document_id) +
                       "§DOCUMENT>&field=content&field=collectionID" +
                       self.limit_timeout_part)

        self.announce_query(api_request)
        # response = get(api_request)
        response_json = self.get_api_response(api_request)

        text = response_json.get('result').get('docs')[0].get('content')
        collection = (
            response_json.get('result').get('docs')[0].get('collectionID'))

        retdict = {'text': text, 'collection': collection}
        return retdict

    def get_estc_id_metadata(self, estc_id):
        fields = [
            "author",
            "totalPages",
            "module",
            "documentLength",
            "dateStart",
            "ESTCID",
            "collectionID",
            "system_control_number",
            "fullTitle",
            "documentID"
        ]
        fields_part = self.get_fields_request_part(fields)
        api_request = (self.api_request_start +
                       "<DOCUMENT§ESTCID:" +
                       str(estc_id) +
                       "§DOCUMENT>" +
                       fields_part +
                       self.limit_timeout_part)
        self.announce_query(api_request)
        data = self.get_api_response(api_request).get('result').get('docs')
        return data

    def get_document_id_metadata(self, document_id):
        fields = [
            "author",
            "totalPages",
            "module",
            "documentLength",
            "dateStart",
            "ESTCID",
            "collectionID",
            "system_control_number",
            "fullTitle",
            "documentID"
        ]
        fields_part = self.get_fields_request_part(fields)
        api_request = (self.api_request_start +
                       "<DOCUMENT§documentID:" +
                       str(document_id) +
                       "§DOCUMENT>" +
                       fields_part +
                       self.limit_timeout_part)
        self.announce_query(api_request)
        data = self.get_api_response(api_request).get('result').get('docs')
        return data

    def get_documents_by_length(self, length, operator, fields):
        if operator == "<":
            length_part = "documentLength:" + "[0 TO " + str(length - 1) + "]"
        elif operator == ">=":
            length_part = (
                "documentLength:[" + str(length) + " TO " + "99999999]")
        else:
            sys.exit("invalid operator!")
        fields_part = self.get_fields_request_part(fields)
        api_request = (self.api_request_start +
                       "<DOCUMENT§" +
                       length_part +
                       "§DOCUMENT>" +
                       fields_part +
                       self.limit_timeout_part)

        self.announce_query(api_request)
        # response = get(api_request)
        # data = response.json().get('result').get('docs')
        data = self.get_api_response(api_request).get('result').get('docs')
        return data
