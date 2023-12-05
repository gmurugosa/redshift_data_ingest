import requests
from datetime import datetime

# Lista de monedas que se van a obtener
coins = ['BTC', 'ETH', 'USDT', 'BNB', 'XRP', 'SOL', 'USDC', 'ADA', 'DOGE', 'AVAX', 'LINK', 'MATIC', 'SHIB', 'LTC']

def obtener_informacion_diaria(coin, year, month, day):
    url = f'https://www.mercadobitcoin.net/api/{coin}/day-summary/{year}/{month}/{day}/'

    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Fallo al obtener información de la moneda {coin}. Status code: {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"Excepción en Request: {e}")
        return None

if __name__ == "__main__":
    # Obtengo la fecha de ayer
    current_date = datetime.now()
    year, month, day = current_date.year, current_date.month, current_date.day-1

    # Hago el api call para cada moneda
    for coin in coins:
        # Cargo el diccionario
        coin_data = obtener_informacion_diaria(coin, year, month, day)
        if coin_data:
            #Muesto la información que será guardada en la tabla que se creará en Redshift
            print(f"Información para la moneda {coin} de la fecha {year}/{month}/{day}:")
            print(coin_data)
            print("--------------------")
        else:
            print(f"No hay información disponible para la moneda {coin} de la fecha {year}/{month}/{day}")