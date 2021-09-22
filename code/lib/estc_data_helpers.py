def get_long_estc_id(estc_id):
    if len(estc_id) < 7:
        return (estc_id[0] + ((7 - len(estc_id)) * '0') +
                estc_id[1:])
    else:
        return (estc_id)
