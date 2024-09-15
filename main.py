from datetime import date
from datetime import datetime
import json
import requests
import pandas as pd

API_KEY = "5c50eb410d19ac41276bc63c1885e6c99bf7b2219977e16d9904099ba4e93835"

OUT_DATASET = pd.DataFrame()


def is_correct_day_measure(measure):
    """
    Verifica se a leitura recebida é do mesmo dia em que esse código está sendo executado
    
    Args:
        measure (json): JSON com a leitura a ser verificada
        
    Returns:
        True  - Se a leitura tem data válida
        False - Se a leitura não tem data válida
    """
    if(len(measure["measurements"][0]) >0):
        today = date.today().strftime("%Y-%m-%d")
        if(today in measure["measurements"][0]["lastUpdated"]):
            return True
    return False


def save_to_parquet(df):
    """
    Converte o Pandas Dataset recebido como argumento para o formato parquet e salva no filesystem
    
    Args:
        df (pandas DataFrame): Data frame a ser salvo no formato parquet.
    """
    try:
        # Converte os dados para um DataFrame do pandas
        file_name = f'{datetime.now().strftime("%d_%m_%y")}_openaq_data.parquet'

        # Salva o DataFrame em um arquivo Parquet
        df.to_parquet(file_name, engine='pyarrow')
        return file_name
    except Exception as e:
        print(e)

def process_json_list(json_list):
    """
    Converte uma lista de JSONs com medições de diferentes parâmetros em um DataFrame do pandas.
    As medições serão extraídas e transformadas em colunas.
    
    Args:
        json_list (list): Lista de dicionários JSON.
        
    Returns:
        pd.DataFrame: DataFrame contendo os dados estruturados.
    """
    processed_data = []

    for entry in json_list:
        base_info = {
            'location': entry.get('location'),
            'city': entry.get('city'),
            'country': entry.get('country'),
            'latitude': entry.get('coordinates', {}).get('latitude'),
            'longitude': entry.get('coordinates', {}).get('longitude')
        }
        
        # Percorrer as medições e criar uma linha com os valores
        measurements = entry.get('measurements', [])
        data_row = base_info.copy()  # Copiar as informações base
        
        for measurement in measurements:
            param = measurement['parameter']
            value = measurement['value']
            unit = measurement['unit']
            # Criamos uma coluna para cada parâmetro de medição
            data_row[f'{param} ({unit})'] = value
            data_row[f'{param}_lastUpdated'] = measurement['lastUpdated']
        
        processed_data.append(data_row)

    # Criando o DataFrame final
    df = pd.DataFrame(processed_data)
    
    return df

def get_latests():
    url = "https://api.openaq.org/v2/latest?limit=5000&page=1&offset=0&sort=desc&radius=1000&order_by=country&dump_raw=false"

    headers = {"accept": "application/json", "x-api-key":API_KEY}
    response = requests.get(url, headers=headers)

    j_resp = response.json()
    #print(j_resp)

    filtered_data = []

    for j in j_resp["results"]:
        if(is_correct_day_measure(j) == True):
            filtered_data.append(j)

    if(len(filtered_data) > 0):
        out_df = process_json_list(filtered_data)
        #print(out_df.head())
        out_df.to_csv('meu_dataset.csv', index=False, encoding='utf-8',na_rep='null')
        save_to_parquet(out_df)

if __name__ == "__main__":
    get_latests()
