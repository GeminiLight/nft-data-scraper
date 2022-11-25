import os
import json
import filetype
from collections import defaultdict


def save_json(json_obj, file_path):
    with open(file_path, 'w') as f:
        f.write(json.dumps(json_obj, indent=4))
    return file_path

def download_image(client, url, file_path):
    res = client.get(url)
    file_type = filetype.guess(res.content)
    if file_type is None:
        extension = 'svg' if url[-4:] == '.svg' else 'unknown'
    else:
        extension = file_type.extension
    with open(f'{file_path}.{extension}', 'wb') as f:
        f.write(res.content)

def extract_key_collection_info(collection_info):
    defaultdict()
    collection_key_info = {
        'id': collection_info.get('id', None),
        'blockchain': collection_info.get('blockchain', None),
        'type': collection_info.get('type', None),
        'name': collection_info.get('name', None),
        'symbol': collection_info.get('symbol', None),
        'owner': collection_info['owner'].split(':')[1] if 'owner' in collection_info else None,
        'description': collection_info['meta'].get('description', None),
        'externalUri': collection_info['meta'].get('externalUri', None),
        'ownerCount': collection_info['statistics']['ownerCount'],
        'itemCount': collection_info['statistics']['itemCount'],
        'ownerCountTotal': collection_info['statistics']['ownerCountTotal'],
        'itemCountTotal': collection_info['statistics']['itemCountTotal'],
        'highestSale': collection_info['statistics']['highestSale']['value'],
        'highestSaleUsd': collection_info['statistics']['highestSale']['valueUsd'],
        'totalVolume': collection_info['statistics']['totalVolume']['value'],
        'totalVolumeUsd': collection_info['statistics']['totalVolume']['valueUsd'],
        '1dVolume': collection_info['statistics']['volumes'][0]['value'],
        '1dVolumeUsd': collection_info['statistics']['volumes'][0]['value']['valueUsd'],
        '7dVolume': collection_info['statistics']['volumes'][1]['value'],
        '7dVolumeUsd': collection_info['statistics']['volumes'][1]['value']['valueUsd'],
        '30dVolume': collection_info['statistics']['volumes'][2]['value'],
        '30dVolumeUsd': collection_info['statistics']['volumes'][2]['value']['valueUsd'],
        'floorPrice': collection_info['statistics']['floorPrice']['value'],
        'floorPriceUsd': collection_info['statistics']['floorPrice']['valueUsd']
    }
    return collection_key_info