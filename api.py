import json

import pandas

import requests
from requests import Response


def get_news():
    url = "https://investing-cryptocurrency-markets.p.rapidapi.com/coins/list-pairs"

    querystring = {"time_utc_offset": "28800", "lang_ID": "1"}

    headers = {
        'x-rapidapi-key': "4cf6cde65amshebec6aca153a547p1f5086jsn4d2c130baaad",
        'x-rapidapi-host': "investing-cryptocurrency-markets.p.rapidapi.com"
    }

    response: Response = requests.request("GET", url, headers=headers, params=querystring)

    # print(response.text)

    from data_types_and_structures import DataTypesHandler
    DataTypesHandler.print_data_recursively(
        data=response.json(), print_dict=DataTypesHandler.PRINT_DICT
    )

    #for crypto in response.json()['data'][0]['screen_data']['pairs_data']:
     #   if 'USD' in crypto['pair_name']:
      #      print(crypto['pair_name'])


def get_stock() -> dict:
    import investpy

    # # Retrieve all the available stocks as a Python list
    # stocks = investpy.get_stocks_list()
    #
    # # Retrieve the recent historical data (past month) of a stock as a pandas.DataFrame on ascending date order
    # df = investpy.get_stock_recent_data(stock='tsla', country='United States', as_json=False, order='ascending')
    #
    # # Retrieve the company profile of the introduced stock on english
    # profile = investpy.get_stock_company_profile(stock='tsla', country='United States', language='english')
    #
    # crypto: pandas.DataFrame = investpy.crypto.get_crypto_historical_data(crypto='bitcoin', as_json=False,
    #                                                                       order='ascending', from_date='05/02/2021',
    #                                                                       to_date='13/02/2021')
    # print(stocks)
    # print(df)
    # print(profile)
    # print(crypto)

    profile_tesla: dict = investpy.get_stock_company_profile(stock='tsla', country='United States', language='english')
    print(profile_tesla['desc'])
    df_tesla: pandas.DataFrame = investpy.get_stock_recent_data(stock='tsla', country='United States', as_json=False, order='ascending')
    print(df_tesla.keys().values)
    print(df_tesla.values[len(df_tesla.values)-1])
    # print(type(df_tesla.values[len(df_tesla.values)-1]))  # <class 'numpy.ndarray'>
    # print(df_tesla)

    return {
        'tsla': {
            'status': {
                'keys': df_tesla.keys().values.tolist(),
                'values': df_tesla.values[len(df_tesla.values)-1].tolist()
            },
            'description': profile_tesla['desc']
        }
    }


# get_news()
# print(pandas.json_normalize(get_stock()).to_dict())
