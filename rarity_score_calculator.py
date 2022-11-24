import numpy as np
import pandas as pd

from functools import reduce
from collections import Counter


def calculate_rarity_score(metadata_fpath):

    pd_metadata = pd.read_json(metadata_fpath)

    pd_metadata['num_attributes'] = pd_metadata['meta'].apply(lambda x: len(x['attributes']))
    pd_metadata['attributes_dict'] = pd_metadata['meta'].apply(lambda x: {attr_kv['key']: attr_kv['value'] for attr_kv in  x['attributes']})

    merged_attributes_list = pd_metadata['attributes_dict'].apply(lambda x: list(x.keys())).values.sum()
    attributes_dict = dict(Counter(merged_attributes_list))

    attribute_key_name_list = list(attributes_dict.keys())
    attribute_key_column_name_list = [f'attribute_{attribute_key}' for attribute_key in attribute_key_name_list]
    attribute_key_prob_column_name_list = [f'attribute_{attribute_key}_prob' for attribute_key in attribute_key_name_list]
    attribute_key_score_column_name_list = [f'attribute_{attribute_key}_score' for attribute_key in attribute_key_name_list]

    for attribute_key, attribute_key_column_name in zip(attribute_key_name_list, attribute_key_column_name_list):
        pd_metadata[attribute_key_column_name] = pd_metadata['attributes_dict'].apply(lambda x: x[attribute_key] if attribute_key in x else None)

    def calc_attribute_rarity_prob_and_score(attribute_key_column_name):
        attr_count_dict = pd_metadata[attribute_key_column_name].value_counts().to_dict()
        attr_score_dict = {k: 1 / (attr_count_dict[k] / len(pd_metadata)) for k,v in attr_count_dict.items()}
        attr_prob_dict = {k: (attr_count_dict[k] / len(pd_metadata)) for k,v in attr_count_dict.items()}
        pd_metadata[f'{attribute_key_column_name}_score'] = pd_metadata[attribute_key_column_name].apply(lambda x: attr_score_dict.get(x, 0))
        pd_metadata[f'{attribute_key_column_name}_prob'] = pd_metadata[attribute_key_column_name].apply(lambda x: attr_prob_dict.get(x, 0))
        
    for attribute_key_cname, attribute_key_score_cname in zip(attribute_key_column_name_list, attribute_key_score_column_name_list):
        calc_attribute_rarity_prob_and_score(attribute_key_cname)

    pd_metadata['sum_attribute_prob'] = pd_metadata[attribute_key_prob_column_name_list].sum(axis=1)
    pd_metadata['mean_attribute_prob'] = pd_metadata[attribute_key_prob_column_name_list].mean(axis=1)
    pd_metadata['mul_attribute_prob'] = pd_metadata[attribute_key_prob_column_name_list].replace(to_replace=0, value=1.).apply(lambda x: reduce(lambda m,n: m * n, x), axis=1)

    pd_metadata['sum_attribute_score'] = pd_metadata[attribute_key_score_column_name_list].sum(axis=1)
    pd_metadata['mean_attribute_score'] = pd_metadata[attribute_key_score_column_name_list].mean(axis=1)
    pd_metadata['mul_attribute_score'] = pd_metadata[attribute_key_score_column_name_list].replace(to_replace=0, value=1.).apply(lambda x: reduce(lambda m,n: m * n, x), axis=1)

    pd_metadata['sum_attribute_prob_rank'] = pd_metadata['sum_attribute_prob'].rank(method='dense', ascending=True)
    pd_metadata['mean_attribute_prob_rank'] = pd_metadata['mean_attribute_prob'].rank(method='dense', ascending=True)
    pd_metadata['mul_attribute_prob_rank'] = pd_metadata['mul_attribute_prob'].rank(method='dense', ascending=True)

    pd_metadata['sum_attribute_score_rank'] = pd_metadata['sum_attribute_score'].rank(method='dense', ascending=False)
    pd_metadata['mean_attribute_score_rank'] = pd_metadata['mean_attribute_score'].rank(method='dense', ascending=False)
    pd_metadata['mul_attribute_score_rank'] = pd_metadata['mul_attribute_score'].rank(method='dense', ascending=False)

    pd_metadata_with_rank = pd_metadata
    return pd_metadata_with_rank