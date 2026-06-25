# %% importando pacotes
import kaggle
import os
import sqlite3
import duckdb
from datetime import datetime
import pandas as pd
from config import KAGGLE_USERNAME, KAGGLE_KEY
import shutil
import json

# --- IMPORTING JSON ---
with open('metadata.json', "r") as f: 
    CONFIG = json.load(f)

# timestamp
timestamp = datetime.now().strftime("%Y-%m-%d_%Hh%M")

# importing from kaggle
db_imported = 'teocalvo/teomewhy-loyalty-system'

files = kaggle.api.dataset_list_files(db_imported).files

# listing csv files
csvs = [f.name 
        for f in files 
        if f.name
            .endswith('.csv')]


# moving actual to last

def move_from_actual_to_last():
    
    actual_path = "../data/actual"
    last_path = "../data/last"

    if not os.path.exists(last_path):
        os.makedirs(last_path)

    print("Movendo arquivos de ../data/actual para ../data/last...")

    for item in os.listdir(actual_path):
        source = os.path.join(actual_path, item)
        destination = os.path.join(last_path, item)
        shutil.move(source, destination)

    print("Arquivos movidos com sucesso!")

move_from_actual_to_last()

#downloading csvs from kaggle

for name in csvs:
    kaggle.api.dataset_download_file(db_imported,
                                     name,
                                     path=(f"../data/actual/"))

# converting to parquet
for file in os.listdir(f"../data/actual/"):
    
    df = pd.read_csv(f'../data/actual/{file}', sep=';')
    
    base_name = file.removesuffix('.csv')
    
    df.to_parquet(f'../data/actual/{base_name}.parquet')
    
    os.remove(f'../data/actual/{file}')

#  ---- comparing the two files ---- 

def get_update_lines(df_last, df_actual, pk, date_field):
    df_update = df_last.merge(
        df_actual,
        how="left",
        on=[pk],
        suffixes=('_x', '_y')
    )

    update_flag = df_update[date_field + '_y'] > df_update[date_field + '_x']
    ids_updated = df_update[update_flag][pk].tolist()

    df_update = df_actual[df_actual[pk].isin(ids_updated)].copy()
    df_update["op"] = "U" 
    return df_update

def get_deleted_lines(df_last, df_actual, pk, date_field):
    df_delete = df_last[~df_last[pk].isin(df_actual[pk])].copy()
    df_delete["op"] = "D"
    return df_delete

def get_insert_lines(df_last, df_actual, pk):
    
    df_insert = df_actual[~df_actual[pk].isin(df_last[pk])].copy()
    df_insert["op"] = "I"
    return df_insert

def create_cdc(df_last, df_actual, pk, date_field):
    df_update = get_update_lines(df_last, df_actual, pk, date_field)
    df_insert = get_insert_lines(df_last, df_actual, pk)
    df_delete = get_deleted_lines(df_last, df_actual, pk, date_field)
    df_cdc = pd.concat([df_update, df_insert, df_delete], ignore_index=True)
    return df_cdc

def process_cdc(tables):
    
    print("Processing CDC Tables")

    for table in tables:
        name = table["name"]
        pk = table["pk"]
        date_field = table["date_field"]
        
        df_last = pd.read_parquet(f"../data/last/{name}.parquet")
        df_actual = pd.read_parquet(f"../data/actual/{name}.parquet")
        df_cdc = create_cdc(df_last, df_actual, pk, date_field)

        if df_cdc.shape[0] == 0:
            print(f"No alteration found in table {name}.")
            continue
        
        # creates table
                
        cdc_table_path = f"../data/cdc/{name}"
        
        
        if not os.path.exists(cdc_table_path):
            os.makedirs(cdc_table_path)

        # saving arqhive into file
        
        filename = f"{cdc_table_path}/{name}_{timestamp}.parquet"

        df_cdc.to_parquet(filename, index=False)
        print(f"CDC saved: {filename}")

    print("CDC successfully processed")

process_cdc(CONFIG["tables"])
