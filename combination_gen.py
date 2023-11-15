""" Generate combination"""


def gen_comb_list(list_set):
    """generate combination list"""
    if not list_set:
        return [[]]

    first_list = list_set[0]
    rest_lists = list_set[1:]

    combinations_without_first = gen_comb_list(rest_lists)
    result = []

    for item in first_list:
        for combination in combinations_without_first:
            result.append([item] + combination)

    return result
