import os
import filetype


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
    collection_key_info = {
        'id': collection_info['id'],
        'blockchain': collection_info['blockchain'],
        'type': collection_info['type'],
        'name': collection_info['name'],
        'symbol': collection_info['symbol'],
        'owner': collection_info['owner'].split(':')[1],
        'description': collection_info['meta']['description'],
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