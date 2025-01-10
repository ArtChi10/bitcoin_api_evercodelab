import requests
import pandas as pd

cmc_url = 'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing'
cmc_params = {
    'start': 1,
    'limit': 1500,
    'sortBy': 'market_cap',
    'sortType': 'desc',
    'convert': 'USD',
    'cryptoType': 'all',
    'tagType': 'all',
    'audited': 'false',
    'aux': 'ath,atl,high24h,low24h,num_market_pairs,cmc_rank,date_added,max_supply,circulating_supply,total_supply,volume_7d,volume_30d,self_reported_circulating_supply,self_reported_market_cap'
}
cmc_response = requests.get(cmc_url, params=cmc_params)

if cmc_response.status_code != 200:
    raise Exception("Ошибка при получении данных с CoinMarketCap")

cmc_data = cmc_response.json()['data']['cryptoCurrencyList']

cmc_df = pd.DataFrame([
    {
        'symbol': coin['symbol'],
        'volume_24h': coin['quotes'][0]['volume24h']  # Извлечение объема торгов в USD
    }
    for coin in cmc_data
])

ss_url = 'https://simpleswap.io/api/v3/currencies'
ss_params = {
    'fixed': 'false',
    'includeDisabled': 'false'
}
ss_response = requests.get(ss_url, params=ss_params)

if ss_response.status_code != 200:
    raise Exception("Ошибка при получении данных с SimpleSwap")

ss_data = ss_response.json()

ss_symbols = [currency['symbol'].upper() for currency in ss_data]

missing_coins_df = cmc_df[~cmc_df['symbol'].isin(ss_symbols)]

missing_coins_df = missing_coins_df.sort_values(by='volume_24h', ascending=False)

output_file = "missing_coins.csv"
missing_coins_df.to_csv(output_file, index=False)
print(f"Результаты сохранены в файл {output_file}")
