from prefect import flow, task
import pandas as pd
import json
import mysql.connector
from pathlib import Path

# MySQL-Verbindungsdaten
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "testdb",
}


@task
def load_csv_to_mysql(csv_path):
    df = pd.read_csv(csv_path, sep=";")
    print(df)
    # insert_dataframe(df, "kunden_csv")


@task
def load_excel_to_mysql(excel_path):
    df = pd.read_excel(excel_path)
    # insert_dataframe(df, "kunden_excel")


@task
def load_json_to_mysql(json_path):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    df = pd.json_normalize(data)
    # insert_dataframe(df, "kunden_json")


def insert_dataframe(df, table_name):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

    for _, row in df.iterrows():
        cursor.execute(sql, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()


@flow
def load_all_sources():
    load_csv_to_mysql("../data/e-com_kunden.csv")
    load_excel_to_mysql("../data/bestandskunden.xlsx")
    load_json_to_mysql("../data/crm_kunden.json")


if __name__ == "__main__":
    load_all_sources()
