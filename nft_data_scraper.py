import os
import csv
import copy
import json
import time
import tqdm
import httpx
import asyncio
import threading
import pandas as pd

from utils import download_image, extract_key_collection_info



class NFTDataScraper:

    def __init__(self, num_threads=50, max_timeout=120., sleep_time=0.1, page_size=1000):
        self.rarible_api_base_url = 'https://api.rarible.org/v0.1'
        self.rarible_ethereum_api_base_url = 'https://ethereum-api.rarible.org/v0.1'
        self.main_save_dir = './nft_data'
        self.image_sub_save_dir = 'images'
        self.sleep_time = sleep_time
        self.page_size = page_size
        self.num_threads = num_threads
        timeout = httpx.Timeout(max_timeout, connect=60.0)
        transport = httpx.HTTPTransport(retries=3)
        self.client = httpx.Client(timeout=timeout, transport=transport)
        assert self.page_size <= 1000

    def ready(self, collection_contract_address, chain='ETHEREUM'):
        collection_contract_address = collection_contract_address.lower()
        chain = chain.upper()
        self.collection_save_dir = os.path.join(self.main_save_dir, chain, collection_contract_address)
        collection_image_dir = os.path.join(self.collection_save_dir, self.image_sub_save_dir)
        if not os.path.exists(collection_image_dir):
            os.makedirs(collection_image_dir)
        return chain, collection_contract_address

    def safe_request(self, url, method='get', **kwargs):
        res = self.client.get(url, **kwargs) if method == 'get' else self.client.post(url, **kwargs)
        res_status = res.status_code
        if res_status != 200:
            print(f'Status of Request is {res_status}: {res}')
            print(res.json())
            res_data = None
        else:
            res_data = res.json()
        return res_status, res_data

    def scrape_collection(self, collection_contract_address, chain='ETHEREUM', activity_types=['SELL'], if_download_image=False):
        print('-' * 20 + ' Starting ' + '-' * 20)
        collection_info, collection_key_info = self.scrape_collection_information(collection_contract_address=collection_contract_address, chain=chain)
        print()
        self.scrape_collection_items(collection_contract_address=collection_contract_address, chain=chain, collection_information=collection_info, if_download_image=if_download_image)
        print()
        self.scrape_collection_activities(collection_contract_address=collection_contract_address, chain=chain, activity_types=activity_types, collection_information=collection_info)
        print('-' * 20 + ' Finished ' + '-' * 20)

    def scrape_all_collections(self, ):
        url = f'{self.rarible_ethereum_api_base_url}/nft/collections/all?size={self.page_size}'
        continuation = None
        total_success_count = 0
        all_collection_list = []
        while True:
            new_url = url if continuation == None else url + f'&continuation={continuation}'
            res_status, res_data = self.safe_request(new_url, method='get')
            if res_data is None: break
            for collection in res_data['collections']:
                all_collection_list.append(collection)
                total_success_count += 1
            os.system('cls'); print(total_success_count)
            # continue or stop
            if 'continuation' not in res_data:
                print('Finished')
                break
            else:
                continuation = res_data['continuation']
                time.sleep(self.sleep_time)

        file_path = f'{os.path.join(self.main_save_dir)}/all_collections.csv'
        pd_collections = pd.DataFrame(all_collection_list)
        pd_collections.to_csv(file_path)
        
    def scrape_collection_information(self, collection_contract_address, chain='ETHEREUM'):
        chain, collection_contract_address = self.ready(collection_contract_address, chain)
        url = f'{self.rarible_api_base_url}/collections/{chain}:{collection_contract_address}'
        res_status, res_data = self.safe_request(url, method='get')
        collection_info = res_data
        
        print(f"   Name: {collection_info['name']}")
        print(f"  Chain: {chain}")
        print(f"Address: {collection_contract_address}")
        with open(os.path.join(self.collection_save_dir, 'information.json'), 'w') as f:
            json_collection_info = json.dumps(collection_info, indent=4)
            f.write(json_collection_info)
        collections_key_info_fpath = os.path.join(self.main_save_dir, 'collections.csv')
        if_write_header = not os.path.exists(collections_key_info_fpath)
        with open(collections_key_info_fpath, 'a+', newline='') as f:
            collection_key_info = extract_key_collection_info(collection_info)
            writer = csv.writer(f)
            if if_write_header:
                writer.writerow(collection_key_info.keys())
            writer.writerow(collection_key_info.values())
        self.written_temp_header = True
        return collection_info, collection_key_info

    def scrape_collection_activities(self, collection_contract_address, chain='ETHEREUM', activity_types=["SELL"], collection_information=None):
        if len(activity_types) == 0: return
        collection_info = self.scrape_collection_information(collection_contract_address) if collection_information is None else collection_information
        collection_name = collection_info['name'] if 'name' in collection_info else 'unknown'
        url = f'{self.rarible_api_base_url}/activities/byCollection?collection={chain}:{collection_contract_address}&type={", ".join(activity_types).upper()}&size={self.page_size}&sort=LATEST_FIRST'
        total_success_count = 0
        continue_flag_key, continue_flag = 'cursor', None
        collection_activity_list = []
        print(f'Scraping Activities of {collection_name}')
        while True:
            new_url = url if continue_flag == None else url + f'&{continue_flag_key}={continue_flag}'
            res_status, res_data = self.safe_request(new_url, method='get')
            if res_data is None: break
            for activity in res_data['activities']:
                collection_activity_list.append(activity)
                total_success_count += 1
            print(total_success_count, end=' ')
            # continue or stop
            if continue_flag_key not in res_data:
                print(f'\nFinished: Scraped Activities of {collection_name}')
                break
            else:
                continue_flag = res_data[continue_flag_key]
                time.sleep(self.sleep_time)

        file_path = os.path.join(self.collection_save_dir, f'activities_{"-".join(activity_types)}.csv')
        pd_properties = pd.DataFrame(collection_activity_list)
        pd_properties.to_csv(file_path)
        print(f'Save file to {file_path}')


    def scrape_collection_items(self, collection_contract_address, chain='ETHEREUM', collection_information=None, if_download_image=False):
        chain, collection_contract_address = self.ready(collection_contract_address, chain)
        collection_info = self.scrape_collection_information(collection_contract_address) if collection_information is None else collection_information
        collection_name = collection_info['name'] if 'name' in collection_info else 'unknown'
        total_item_count = max(int(collection_info['statistics']['itemCountTotal']), int(collection_info['statistics']['ownerCountTotal']))

        pbar = tqdm.tqdm(desc=f'Scraping Items of {collection_name}', total=total_item_count)

        total_image_count = 0
        continue_flag_key, continue_flag = 'continuation', None
        item_properties_list = []
        item_metadata_list = []
        collection_image_save_dir = os.path.join(self.main_save_dir, chain, collection_contract_address, self.image_sub_save_dir)
        downloaded_image_list = [image_file.split('.')[0] for image_file in os.listdir(collection_image_save_dir)]
        url = f'{self.rarible_api_base_url}/items/byCollection?collection={chain}:{collection_contract_address}&size={self.page_size}'
        while True:
            new_url = url if continue_flag == None else url + f'&{continue_flag_key}={continue_flag}'
            res = self.client.get(new_url)
            res_status, res_data = res.status_code, res.json()
            for item_metadata in res_data['items']:
                # nft metadata
                pbar.update()
                total_image_count += 1
                item_metadata_list.append(item_metadata)
                token_id = item_metadata['tokenId']
                token_name = item_metadata['meta']['name']
                image_url = item_metadata['meta']['content'][0]['url']

                item_properties = {'token_id': token_id, 'token_name': token_name, 'image_url': image_url}
                for p_dict in item_metadata['meta']['attributes']:
                    property_key, property_value = p_dict.values()
                    item_properties[property_key] = property_value
                item_properties_list.append(item_properties)
                # download image
                if if_download_image:
                    image_file_path = f'./{collection_image_save_dir}/{token_id}'
                    if token_id in downloaded_image_list:
                        continue
                    while threading.active_count() > self.num_threads:
                        time.sleep(self.sleep_time)
                    threading.Thread(target=download_image, args=(self.client, image_url, image_file_path)).start()
            # continue or stop
            if continue_flag_key not in res.json():
                pbar.close()
                print(f'Finished: Scraped Items of {collection_name}')
                break
            else:
                continue_flag = res_data[continue_flag_key]
                time.sleep(self.sleep_time)

        with open(os.path.join(self.collection_save_dir, 'metadata.json'), 'w') as f:
            json_item_metadata_list = json.dumps(item_metadata_list, indent=4)
            f.write(json_item_metadata_list)
        file_path = os.path.join(self.collection_save_dir, 'properties.csv')
        pd_properties = pd.DataFrame(item_properties_list)
        pd_properties.to_csv(file_path)
        print(f'Save file to {file_path}')
