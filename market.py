import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получает список товаров кампании на Яндекс.Маркете.
    
    Выполняет запрос к API Яндекс.Маркета для получения товаров с пагинацией.
    Используется для получения всех товаров, размещенных в кампании продавца.
    
    Args:
        page (str): Токен страницы для пагинации. Для первой страницы - пустая строка.
        campaign_id (str): Идентификатор кампании (кабинета продавца).
        access_token (str): Токен доступа к API Яндекс.Маркета.
    
    Returns:
        dict: Словарь с результатами запроса, содержащий:
            - offerMappingEntries: список товаров
            - paging: информация для пагинации
    
    Examples:
        >>> get_product_list("", "campaign123", "token123")
        {'offerMappingEntries': [...], 'paging': {'nextPageToken': 'next123'}}
        
        >>> get_product_list("invalid_token", "campaign123", "token123")
        Traceback (most recent call last):
        requests.exceptions.HTTPError: 400 Client Error
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновляет информацию об остатках товаров на Яндекс.Маркете.
    
    Отправляет запрос к API Яндекс.Маркета для обновления количества
    доступных товаров на указанном складе.
    
    Args:
        stocks (list): Список словарей с данными об остатках.
            Каждый словарь должен содержать ключи: sku, warehouseId, items.
        campaign_id (str): Идентификатор кампании (кабинета продавца).
        access_token (str): Токен доступа к API Яндекс.Маркета.
    
    Returns:
        dict: Ответ от API Яндекс.Маркета с результатом обновления.
    
    Examples:
        >>> stocks = [{'sku': '123', 'warehouseId': '456', 'items': [...]}]
        >>> update_stocks(stocks, "campaign123", "token123")
        {'status': 'OK'}
        
        >>> update_stocks([], "campaign123", "token123")
        {'status': 'OK'}
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновляет цены товаров на Яндекс.Маркете.
    
    Отправляет запрос к API Яндекс.Маркета для обновления цен
    у указанных товаров.
    
    Args:
        prices (list): Список словарей с данными для обновления цен.
            Каждый словарь должен содержать ключи: id, price.
        campaign_id (str): Идентификатор кампании (кабинета продавца).
        access_token (str): Токен доступа к API Яндекс.Маркета.
    
    Returns:
        dict: Ответ от API Яндекс.Маркета с результатом обновления.
    
    Examples:
        >>> prices = [{'id': '123', 'price': {'value': 5990, 'currencyId': 'RUR'}}]
        >>> update_price(prices, "campaign123", "token123")
        {'status': 'OK'}
        
        >>> update_price([{'id': '123', 'price': {}}], "campaign123", "token123")
        Traceback (most recent call last):
        requests.exceptions.HTTPError: 400 Client Error
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получает артикулы товаров Яндекс.Маркета.
    
    Использует пагинацию для получения всех товаров кампании,
    затем извлекает их артикулы (shopSku).
    
    Args:
        campaign_id (str): Идентификатор кампании (кабинета продавца).
        market_token (str): Токен доступа к API Яндекс.Маркета.
    
    Returns:
        list: Список строковых артикулов (shopSku) всех товаров кампании.
    
    Examples:
        >>> get_offer_ids("campaign123", "token123")
        ['12345', '67890', '11223']
        
        >>> get_offer_ids("invalid_campaign", "token123")
        Traceback (most recent call last):
        requests.exceptions.HTTPError: 404 Client Error
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Создает список для обновления остатков на Яндекс.Маркете.
    
    Формирует структуру данных для API Яндекс.Маркета, сопоставляя товары
    из файла поставщика с товарами, размещенными на маркетплейсе.
    
    Args:
        watch_remnants (list): Список словарей с данными от поставщика.
        offer_ids (list): Список артикулов товаров, размещенных на Яндекс.Маркете.
        warehouse_id (str): Идентификатор склада (FBS или DBS).
    
    Returns:
        list: Список словарей в формате API Яндекс.Маркета для обновления остатков.
    
    Examples:
        >>> watch_data = [{'Код': '123', 'Количество': '5'}]
        >>> offer_ids = ['123', '456']
        >>> create_stocks(watch_data, offer_ids, 'warehouse123')
        [{'sku': '123', 'warehouseId': 'warehouse123', 'items': [...]}, 
         {'sku': '456', 'warehouseId': 'warehouse123', 'items': [...]}]
        
        >>> create_stocks([], ['123'], 'warehouse123')
        [{'sku': '123', 'warehouseId': 'warehouse123', 'items': [...]}]
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создает список для обновления цен на Яндекс.Маркете.
    
    Формирует структуру данных для API Яндекс.Маркета, преобразуя цены
    из формата поставщика в формат, требуемый маркетплейсом.
    
    Args:
        watch_remnants (list): Список словарей с данными от поставщика.
        offer_ids (list): Список артикулов товаров, размещенных на Яндекс.Маркете.
    
    Returns:
        list: Список словарей в формате API Яндекс.Маркета для обновления цен.
    
    Examples:
        >>> watch_data = [{'Код': '123', 'Цена': "5'990.00 руб."}]
        >>> offer_ids = ['123']
        >>> create_prices(watch_data, offer_ids)
        [{'id': '123', 'price': {'value': 5990, 'currencyId': 'RUR'}}]
        
        >>> create_prices([{'Код': '999', 'Цена': "1'000 руб."}], ['123'])
        []
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Асинхронно обновляет цены товаров на Яндекс.Маркете.
    
    Получает список товаров кампании, формирует данные для обновления цен
    и отправляет их партиями через API Яндекс.Маркета.
    
    Args:
        watch_remnants (list): Список словарей с данными от поставщика.
        campaign_id (str): Идентификатор кампании (кабинета продавца).
        market_token (str): Токен доступа к API Яндекс.Маркета.
    
    Returns:
        list: Список всех сформированных данных о ценах.
    
    Examples:
        >>> import asyncio
        >>> watch_data = [{'Код': '123', 'Цена': "5'990.00 руб."}]
        >>> asyncio.run(upload_prices(watch_data, "campaign123", "token123"))
        [{'id': '123', 'price': {'value': 5990, 'currencyId': 'RUR'}}]
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Асинхронно обновляет остатки товаров на Яндекс.Маркете.
    
    Получает список товаров кампании, формирует данные об остатках
    и отправляет их партиями через API Яндекс.Маркета.
    
    Args:
        watch_remnants (list): Список словарей с данными от поставщика.
        campaign_id (str): Идентификатор кампании (кабинета продавца).
        market_token (str): Токен доступа к API Яндекс.Маркета.
        warehouse_id (str): Идентификатор склада (FBS или DBS).
    
    Returns:
        tuple: Кортеж из двух списков:
            - not_empty: товары с ненулевым остатком
            - stocks: все товары с остатками
    
    Examples:
        >>> import asyncio
        >>> watch_data = [{'Код': '123', 'Количество': '5'}]
        >>> asyncio.run(upload_stocks(watch_data, "campaign123", "token123", "warehouse123"))
        ([{'sku': '123', ...}], [{'sku': '123', ...}])
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    """Основная функция для запуска скрипта вручную.
    
    Читает настройки из переменных окружения, загружает данные
    от поставщика и обновляет остатки и цены на Яндекс.Маркете
    отдельно для кампаний FBS и DBS.
    
    Examples:
        Для запуска требуется задать переменные окружения:
        export MARKET_TOKEN="your_token"
        export FBS_ID="fbs_campaign_id"
        export DBS_ID="dbs_campaign_id"
        export WAREHOUSE_FBS_ID="fbs_warehouse"
        export WAREHOUSE_DBS_ID="dbs_warehouse"
        python market.py
    """
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
