# Librerias
import requests
import pandas as pd
from datetime import datetime

def get_list_available_snapshots(base_url="https://api.mabrian.com/v1/spend/international/", 
                                 operation="available_snapshots", 
                                 params={'destination': "mallorca", 'key': ""}): 
    """
    return Año Mes Día
    devuelve una lista cuyo cada item es de la forma: 
        {'id': 'spd_51f2eaed950f18584eff224c', 
        'previous_period_start': '2020-04-01', 
        'previous_period_end': '2021-03-01', 
        'current_period_start': '2021-04-01', 
        'current_period_end': '2022-03-01'}
    """  
    params = params.copy()
    res = []
    r = requests.get(base_url + operation, params)
    if r.status_code != 200:
        return res

    sorted_snapshots = sorted(r.json()['data'], key=lambda x: x.get('current_period_start'))
    
    for i, snapshot in enumerate(sorted_snapshots, 1): 
        aux = snapshot.copy()
        aux['Periodo'] = f'Periodo {i}'
        res.append(aux)
    return res

def get_resource(base_url="https://api.mabrian.com/v1/spend/international/", 
                 available_snaps=[], 
                 countries=['FR', 'GR', 'DE', 'TR', 'DK', 'HR', 'IT', 'CH', 'NL', 'GB'], 
                 params={'destination': "mallorca", 'key': ""}, 
                 operation=''): 
    """
    pe: Por cada snapshot (available_snaps) y por cada country (countries), 
        pide a la API de Mabrian los datos de ese recurso (operation)
    
    Parametros:
        base_url :            "https://api.mabrian.com/v1/spend/international/"
        available_snaps :     list of snapshots from get_list_available_snapshots()
        countries :           COUNTRIES = ['FR', 'GR', 'DE', 'TR', 'DK', 'HR', 'IT', 'CH', 'NL', 'GB']
        params :              this dictionary {'destination': "mallorca", 'key': KEY}
        operation :           one item in ('avg_spend_cardholder', 'avg_transaction_value', 'countries_spend', 'avg_length_stay', 'unique_visits')
    
    se modifica el nombre me las columnas menos 'id_snapshot' y 'origin' ya que serán las PK para el join final
    """
    BASE_PARAMS = params.copy()
    dataframes = []
    for snap in available_snaps: 
        for country in countries: 
            params = BASE_PARAMS.copy()
            id = snap.get('id')
            
            params['snapshot'] = id
            params['origin'] = country

            r = requests.get(base_url+operation, params)

            if r.status_code == 200:
                data = r.json()['data']
                df = pd.json_normalize(data)
                df['id_snapshot'] = id
                df['origin'] = country
                
                dataframes.append(df)
            else:
                print(r.status_code)
                print(r.content)

    res = pd.concat(dataframes, ignore_index=True)
    res.columns = [f'{operation}_{c}' if c not in ('id_snapshot', 'origin') else c for c in res.columns]
    return res