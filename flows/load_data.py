from prefect import flow, task
import pandas as pd
import json
import mysql.connector

# MySQL-Verbindungsdaten
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "testdb",
}


@task
def load_csv_to_mysql(csv_path) -> pd.DataFrame:
    """
    loads csv from given path, with transformations to send to mysgl
    :param csv_path: file path for csv
    :return: transformed dataframe
    """
    df = pd.read_csv(csv_path, sep=";")
    df.rename(columns={"Anrede": "Geschlecht", "Email": "EMail", }, inplace=True)
    print(df)
    df.loc[df["Geschlecht"] == "Herr", "Geschlecht"] = "m"
    df.loc[df["Geschlecht"] == "Frau", "Geschlecht"] = "w"
    df["Firma"].replace({float('nan'): None}, inplace=True)
    print(df)
    return df
    # insert_dataframe(df, "kunden_csv")


@task
def load_excel_to_mysql(excel_path) -> pd.DataFrame:
    """
    loads excel from given path, with transformations to send to mysql
    :param excel_path: file path for excel
    :return: transformed dataframe
    """
    df = pd.read_excel(excel_path)
    df = df.rename(columns={"Email": "EMail", "Adresse": "Strasse"})
    df['Aktiv'] = df["Aktiv"].replace({"ja": True, "nein": False})
    df['Hausnummer'] = df['Strasse'].str.rsplit(" ", n=1, expand=True)[1]
    df['Strasse'] = df['Strasse'].str.rsplit(" ", n=1, expand=True)[0]
    df['Firma'] = df["Firma"].replace({float('nan'): None})
    print(df)
    # insert_dataframe(df, "kunden_excel")
    return df


@task
def load_json_to_mysql(json_path) -> pd.DataFrame:
    """
    loads json from given path, with transformations to send to mysql
    :param json_path: file path for json
    :return: transformed dataframe
    """
    print("in load_json_to_mysql")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    df = pd.json_normalize(data)
    df = df.rename(columns={"kundennr": "KundenID", "vorname": "Vorname", "nachname": "Nachname",
                            "email": "EMail", "geschlecht": "Geschlecht", "registriert_am": "Registrierungsdatum",
                            "aktiv": "Aktiv", "firma": "Firma", "adresse.straße": "Strasse",
                            "adresse.plz": "PLZ", "adresse.ort": "Ort", "adresse.land": "Land"})
    df['Hausnummer'] = df['Strasse'].str.rsplit(" ", n=1, expand=True)[1]
    df['Strasse'] = df['Strasse'].str.rsplit(" ", n=1, expand=True)[0]
    df['Firma'] = df["Firma"].replace({"": None})
    print(df)
    # insert_dataframe(df, "kunden_json")
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


@task
def init_schema() -> pd.DataFrame:
    schema = [
        "KundenID",
        "Vorname",
        "Nachname",
        "Email",
        "Geschlecht",
        "Registrierungsdatum",
        "Aktiv",
        "PLZ",
        "Ort",
        "Strasse",
        "Hausnummer",
        "Land",
        "Firma"
    ]
    return pd.DataFrame(columns=schema)


@flow
def load_all_sources():
    df = init_schema()
    csv_df = load_csv_to_mysql("../data/e-com_kunden.csv")
    excel_df = load_excel_to_mysql("../data/bestandskunden.xlsx")
    json_df = load_json_to_mysql("../data/crm_kunden.json")

    combined_df = pd.concat([csv_df, excel_df, json_df], ignore_index=True)
    print(combined_df)


if __name__ == "__main__":
    load_all_sources()
