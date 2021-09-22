def get_unified_id(raw_id):
    if raw_id[0] == "A" or raw_id[0] == "B":
        return raw_id.split(".")[0]
    else:
        return raw_id


def get_api_id(raw_id):
    if raw_id[0] == "A" or raw_id[0] == "B":
        retid = raw_id.split(".")[0]
        retid = retid[1:]
        if raw_id[0] == "A":
            retid = "10" + retid
        else:
            retid = "11" + retid
        return {'api': 'eebo', 'id': retid}
    else:
        return {'api': 'ecco', 'id': raw_id}
