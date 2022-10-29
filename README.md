# NFT Data Scraper

We foster a scraper for NFT (Non-fungible token) data based on public Rarible API, where the following data of NFT collections are easily acquired

- All NFT Collection List `scrape_all_collections`
- NFT Collection Information `scrape_collection_information`
- NFT Token Metadata & Image `scrape_collection_items`
- NFT Transaction Records `scrape_collection_activities`

> `scrape_collection` offers a unified way to scrape information, items and activities of the given collection.

## Installation

```shell
pip install -r requirements.txt
```

## Example

Here is a simple example of scraping all data of a given collection contract address


```python
from nft_data_scraper import NFTDataScraper

# create a nft data scraper
scraper = NFTDataScraper(num_threads=50)

# select chain & collection contract address 
chain = 'ETHEREUM'
collection_contract_address = '0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d'

# set event_types & if_download_image
event_types = ['SELL']
if_download_image = True

# start to scrape the data
scraper.scrape_collection(collection_contract_address=collection_contract_address, 
                         chain=chain, if_download_image=if_download_image
                         event_types=event_types)
```

## File Structure

```
nft-data-scraper
├───nft_data
│   ├───EHEREUM
│   │   └───COLLECTION_CONTRACT_ADDRESS
│   │       ├───activities.csv
│   │       ├───information.json
│   │       ├───metadata.json
│   │       ├───properties.csv
│   │       └───images
│   └───collections.csv
│
├───main.py
├───nft_data_scraper.py
└───utils.py
```

## Reference

- [Rarible API Document](https://api.rarible.org/v0.1/doc)