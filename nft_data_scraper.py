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
from datetime import datetime

from utils import download_image, extract_key_collection_info, save_json, read_json


class NFTDataScraper:

    def __init__(
            self, 
            main_save_dir, 
            num_threads=50, 
            max_timeout=120., 
            sleep_time=0.1, 
            page_size=1000,
            image_sub_save_dir='images'
        ) -> None:
        self.rarible_api_base_url = 'https://api.rarible.org/v0.1'
        self.rarible_ethereum_api_base_url = 'https://ethereum-api.rarible.org/v0.1'
        self.main_save_dir = main_save_dir
        self.image_sub_save_dir = image_sub_save_dir
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
        res = self.client.request(method, url, **kwargs)
        res_status = res.status_code
        if res_status == 200:
            res_data = res.json()
        elif res_status == 404:
            print(f'Status of Request is {res_status}: {res}')
            print(res.json())
            res_data = None
        else:
            print(f'Status of Request is {res_status}: {res}')
            print(res.json())
            res_data = None
            raise ConnectionError
        return res_status, res_data

    def scrape_collection(self, 
            collection_contract_address, 
            chain='ETHEREUM', 
            activity_types=['SELL'], 
            if_download_image=False
        ) -> None:
        print('-' * 20 + ' Starting ' + '-' * 20)
        collection_info, collection_key_info = self.scrape_collection_information(collection_contract_address=collection_contract_address, chain=chain)
        if collection_info is None: return 
        print()
        self.scrape_collection_items(collection_contract_address=collection_contract_address, chain=chain, collection_information=collection_info, if_download_image=if_download_image)
        print()
        for activity_type in activity_types:
            activity_type_list = [activity_type]
            self.scrape_collection_activities(collection_contract_address=collection_contract_address, chain=chain, activity_types=activity_type_list, collection_information=collection_info)
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

        all_collections_file_path = f'{os.path.join(self.main_save_dir)}/all_collections.json'
        save_json(all_collection_list, all_collections_file_path)
        print(f'Save all_collections file to {all_collections_file_path}')
        
    def scrape_collection_information(
            self, 
            collection_contract_address, 
            chain='ETHEREUM',
            save=True
        ) -> tuple[dict, dict]:
        if ':' in collection_contract_address:
            chain, collection_contract_address = collection_contract_address.split(':')
        # ignore Ethereum Name Service and superrare
        if collection_contract_address in ['0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85', '0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0']:
            print(f'Not support Ethereum Name Service 0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85')
            return None, None
        chain, collection_contract_address = self.ready(collection_contract_address, chain)
        url = f'{self.rarible_api_base_url}/collections/{chain}:{collection_contract_address}'
        res_status, res_data = self.safe_request(url, method='get')
        if res_data is None: return None, None

        collection_info = res_data
        collection_key_info = extract_key_collection_info(collection_info)
        
        print(f"   Name: {collection_info['name']}")
        print(f"  Chain: {chain}")
        print(f"Address: {collection_contract_address}")

        if not save:
            return collection_info, collection_key_info

        information_fpath = os.path.join(self.collection_save_dir, 'information.json')
        save_json(collection_info, information_fpath)
        print(f'Save information file to {information_fpath}')

        collections_fpath = os.path.join(self.main_save_dir, 'collections.json')
        pd_collections_info = pd.DataFrame([collection_info])
        if os.path.exists(collections_fpath):
            old_pd_collections_info = pd.read_json(collections_fpath)
            pd_collections_info = pd.concat([old_pd_collections_info, pd_collections_info], ignore_index=True)
            pd_collections_info = pd_collections_info.drop_duplicates(subset=['id'], keep='last')
        pd_collections_info.to_json(collections_fpath, orient='records', indent=4)
        return collection_info, collection_key_info

    def scrape_collection_activities(
            self, 
            collection_contract_address, 
            chain='ETHEREUM', 
            activity_types=["SELL"], 
            collection_information=None
        ) -> None:
        if activity_types in [None, False, []]: return
        collection_info = self.scrape_collection_information(collection_contract_address)[0] if collection_information is None else collection_information
        if collection_info is None: return 
        collection_name = collection_info['name'] if 'name' in collection_info else 'unknown'
        print(f'Scraping Activities ({activity_types}) of {collection_name}')
        continue_flag_key = 'cursor'
        activities_file_path = os.path.join(self.collection_save_dir, f'activities_{"-".join(activity_types)}.json')
        continue_flag = None
        download_activities = []
        downloaded_lasted_datetime = datetime.strptime('2000-01-01T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')
        # renew with reusing previously downloaded activities
        if os.path.exists(activities_file_path):
            print(f'Reuse previously downloaded activities.')
            download_activities = read_json(activities_file_path)
            if len(download_activities) != 0:
                downloaded_lasted_datetime = datetime.strptime(download_activities[0]['date'], '%Y-%m-%dT%H:%M:%SZ')
        sort = 'LATEST_FIRST'
        url = f'{self.rarible_api_base_url}/activities/byCollection?collection={chain}:{collection_contract_address}&type={", ".join(activity_types).upper()}&size={self.page_size}&sort={sort}'
        total_success_count = 0
        collection_activity_list = []
        continue_flag = True
        while continue_flag:
            new_url = url if continue_flag == None else url + f'&{continue_flag_key}={continue_flag}'
            res_status, res_data = self.safe_request(new_url, method='get')
            for activity in res_data['activities']:
                activity_datetime = datetime.strptime(activity['date'], '%Y-%m-%dT%H:%M:%SZ')
                if activity_datetime <= downloaded_lasted_datetime:
                    continue_flag = False
                    break
                collection_activity_list.append(activity)
                total_success_count += 1
            print(total_success_count, end=' ')
            # continue or stop
            if continue_flag_key not in res_data or not continue_flag:
                print(f'\nFinished: Scraped Activities of {collection_name}')
                break
            else:
                continue_flag = res_data[continue_flag_key]
                time.sleep(self.sleep_time)
        collection_activity_list += download_activities
        print(f'Datatime range of transactions: {collection_activity_list[-1]["date"]} ~ {collection_activity_list[0]["date"]}')
        save_json(collection_activity_list, activities_file_path)
        print(f'Save activities file to {activities_file_path}')

    def scrape_collection_items(
            self, 
            collection_contract_address, 
            chain='ETHEREUM', 
            collection_information=None, 
            if_download_image=False
        ) -> None:
        metadata_fpath = os.path.join(self.collection_save_dir, 'metadata.json')
        properties_file_path = os.path.join(self.collection_save_dir, 'properties.json')
        if os.path.exists(metadata_fpath) and os.path.exists(properties_file_path):
            print(f'metadata and properties have already been downloaded.')
            return

        collection_info = self.scrape_collection_information(collection_contract_address) if collection_information is None else collection_information
        if collection_info is None: return 
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
            res_status, res_data = self.safe_request(new_url, method='get')
            for item_info in res_data['items']:
                # nft info
                pbar.update()
                total_image_count += 1
                if item_info['deleted']: continue
                if 'meta' not in item_info:
                    print(f'There is no metadata: {item_info["id"]}!')
                    continue
                item_metadata_list.append(item_info)
                token_id = item_info['tokenId']
                token_metadata = item_info['meta']
                token_name = token_metadata['name']
                
                image_url = token_metadata['content'][0]['url'] if len(token_metadata['content']) else ""

                item_properties = {'token_id': token_id, 'token_name': token_name, 'image_url': image_url}
                for p_dict in token_metadata['attributes']:
                    property_key   = p_dict.get('key', None)
                    property_value = p_dict.get('value', None)
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
            if continue_flag_key not in res_data:
                pbar.close()
                print(f'Finished: Scraped Items of {collection_name}')
                break
            else:
                continue_flag = res_data[continue_flag_key]
                time.sleep(self.sleep_time)

        save_json(item_metadata_list, metadata_fpath)
        print(f'Save metadata file to {metadata_fpath}')

        save_json(item_properties_list, properties_file_path)
        print(f'Save properties file to {properties_file_path}')
