from nft_data_scraper import NFTDataScraper

# create a nft data scraper
scraper = NFTDataScraper(num_threads=50)

# select chain & collection contract address 
chain = 'ETHEREUM'
collection_contract_address = '0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d'

# set activity_types & if_download_image
activity_types = ['SELL']
if_download_image = False

# start to scrape the data
scraper.scrape_collection(
    collection_contract_address=collection_contract_address, 
    chain=chain, 
    if_download_image=if_download_image)