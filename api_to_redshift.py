from datetime import datetime, timedelta, timezone
import pandas as pd
import requests
from sqlalchemy import create_engine

# Redshift credentials and connection details
dbname = 'data-engineer-database'
user = 'gabriel_muru_coderhouse'
password = '0fzrz29Pb1'
host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
port = '5439'

engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}", execution_options={"autocommit": True})

# Lista de monedas que se van a obtener
coins = ['BTC', 'ETH', 'USDT', 'XRP', 'SOL', 'USDC', 'ADA', 'DOGE', 'AVAX', 'LTC']

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

def correr_query(query):
    try:
        with engine.connect() as connection:
            connection.execute(query)
    except Exception as e:
        print(f"Error ejecutando la consulta: {e}")

if __name__ == "__main__":
    # Obtengo la fecha de ayer en UTC
    current_date_utc = datetime.now(timezone.utc)
    yesterday_utc = current_date_utc - timedelta(days=1)
    year, month, day = yesterday_utc.year, yesterday_utc.month, yesterday_utc.day

    # Borro tabla auxiliar
    correr_query("DROP TABLE IF EXISTS cryptodata_aux;")

    # Recorro la lista de monedas y descargo la información
    for coin in coins:
        coin_data = obtener_informacion_diaria(coin, year, month, day)
        if coin_data:
            df = pd.DataFrame([coin_data])
            print(f"Descargando información para la moneda {coin} de la fecha {year}/{month}/{day}:")
            display(df)
            # Convierto los tipos de datos
            df['date'] = pd.to_datetime(df['date']).dt.date
            df['volume'] = pd.to_numeric(df['volume'])
            df['quantity'] = pd.to_numeric(df['quantity'])
            df['coin'] = coin
            df.to_sql("cryptodata_aux", engine, if_exists='append', index=False)

    # Ejecuto un comando SQL MERGE entre la tabla principal y la tabla auxiliar
    merge_sql = """
                MERGE INTO cryptodata
                USING cryptodata_aux AS source
                ON cryptodata.coin = source.coin AND cryptodata.date = source.date
                WHEN MATCHED THEN
                    UPDATE SET
                        opening = source.opening,
                        closing = source.closing,
                        lowest = source.lowest,
                        highest = source.highest,
                        volume = source.volume,
                        quantity = source.quantity,
                        amount = source.amount,
                        avg_price = source.avg_price
                WHEN NOT MATCHED THEN
                    INSERT (coin, date, opening, closing, lowest, highest, volume, quantity, amount, avg_price)
                    VALUES (source.coin, source.date, source.opening, source.closing, source.lowest, source.highest,
                            source.volume, source.quantity, source.amount, source.avg_price);             
            """
    correr_query(merge_sql)