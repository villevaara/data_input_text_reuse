import os


def list_to_dict_by_key(list_to_group, key_field):
    results_dict = {}
    for entry in list_to_group:
        if entry[key_field] in results_dict.keys():
            results_dict[entry[key_field]].append(entry)
        else:
            results_dict[entry[key_field]] = [entry]
    return results_dict


def string_has_lower(str_to_test):
    all_lower = ''.join(ch for ch in str_to_test if ch.islower())
    if len(all_lower) > 0:
        return True
    else:
        return False


def create_dir_if_not_exists(directory_path, verbose=True):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
    if verbose:
        print(directory_path)


def filter_rows_with_field_value(dictlist, field, value):
    results = []
    for row in dictlist:
        if row[field] == value:
            continue
        else:
            results.append(row)
    return results


def filter_rows_with_field_values(dictlist, field, values):
    results = []
    for row in dictlist:
        if row[field] in values:
            continue
        else:
            results.append(row)
    return results


def keep_rows_with_field_value(dictlist, field, value):
    results = []
    for row in dictlist:
        if row[field] == value:
            results.append(row)
        else:
            continue
    return results


def keep_rows_with_field_values(dictlist, field, values):
    results = []
    for row in dictlist:
        if row[field] in values:
            results.append(row)
        else:
            continue
    return results


def dictlist_keep_only_fields(dictlist, fieldnames):
    results = []
    for row in dictlist:
        resrow = {}
        for key, value in row.items():
            if key in fieldnames:
                resrow[key] = value
        results.append(resrow)
    return results
