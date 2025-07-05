from numpy import insert
from prefect import flow, task
import pandas as pd
import json
import os
import mysql.connector
from pathlib import Path

# MySQL-Verbindungsdaten
DB_CONFIG = {
    "host": "mysql",
    "user": "root",
    "password": "root",
    "database": "testdb",
}


@task
def load_csv(path):
    df = pd.read_csv(path, sep=";", dtype={"PLZ": str})
    df = df.rename(columns={"Anrede": "Geschlecht",
                            "Email": "EMail",
                            "Straße": "Strasse"})
    df['Geschlecht'] = df['Geschlecht'].replace({"Herr": "m", "Frau": "w"})
    df['Firma'] = df['Firma'].replace({float('nan'): None})
    df['Aktiv'] = df['Aktiv'].replace({"ja": True, "nein": False})
    df['Quellsystem'] = os.path.basename(path)
    return df


@task
def load_excel(path) -> pd.DataFrame:
    df = pd.read_excel(path)
    df = df.rename(columns={"Email": "EMail", "Adresse": "Strasse"})
    df['Aktiv'] = df['Aktiv'].replace({"ja": True, "nein": False})
    splittedAddress = df['Strasse'].str.rsplit(" ", n=1, expand=True)
    df['Firma'] = df['Firma'].replace({float('nan'): None})
    df['Strasse'] = splittedAddress[0]
    df['Hausnummer'] = splittedAddress[1]
    df['Quellsystem'] = os.path.basename(path)
    return df


@task
def load_json(path) -> pd.DataFrame:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    df = pd.json_normalize(data)
    df = df.rename(columns={"kundennr": "KundenID", "vorname": "Vorname", "nachname": "Nachname",
                            "email": "EMail", "geschlecht": "Geschlecht", "registriert_am": "Registrierungsdatum",
                            "aktiv": "Aktiv", "firma": "Firma", "adresse.straße": "Strasse",
                            "adresse.plz": "PLZ", "adresse.ort": "Ort", "adresse.land": "Land"})
    df['Hausnummer'] = df['Strasse'].str.rsplit(" ", n=1, expand=True)[1]
    df['Strasse'] = df['Strasse'].str.rsplit(" ", n=1, expand=True)[0]
    df['Firma'] = df["Firma"].replace({"": None})
    df['Quellsystem'] = os.path.basename(path)
    return df


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
    csv_df = load_csv("../data/e-com_kunden.csv")
    excel_df = load_excel("../data/bestandskunden.xlsx")
    json_df = load_json("../data/crm_kunden.json")
    df = pd.concat([csv_df, excel_df, json_df], ignore_index=True)

    insert_dataframe(df, 'Kunde')


if __name__ == "__main__":
    load_all_sources()
