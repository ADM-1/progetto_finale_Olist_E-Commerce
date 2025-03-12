import pandas as pd
import csv
from dotenv import load_dotenv
import psycopg
import os

#miglioro la visualizzazione della tabella
pd.set_option("display.max.columns", None)
pd.set_option("display.width", None)
#pd.set_option("display.max.rows", None)

#recupero i dati di collegamento al database da .env3
load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")


def extract_products():
    #creo dataframe da file raw
    df = pd.read_csv(r"../data/raw/olistIT_products.csv")
    return df


def transform_products():
    df = extract_products()
    #stampo il dataframe
    print(df)
    print()

    #controllo che i valori siano unici
    print("--Valori Unici--")
    print(df.nunique())
    print()

    #controllo che non ci siano valori nulli
    print("--Valori nulli--")
    print(df.isnull().sum())
    print()

    # applico le stesse modifiche apportate al dataframe delle categorie
    #rimuovo "2", "_", e gli spazi, correggo gli errori di battitura
    df["category"] = df["category"].str.replace("_2", "")
    df["category"] = df["category"].str.replace("_", " ")
    df["category"] = df["category"].str.strip()
    df["category"] = df["category"].str.replace("fashio ", "fashion ")
    df["category"] = df["category"].str.replace("costruction", "construction")
    df["category"] = df["category"].str.replace("confort", "comfort")
    df["category"] = df["category"].str.replace("childrens", "children")

    #riempio i parametri vuoti con la categoria "uncategorized"
    df["category"] = df["category"].fillna("uncategorized")

    #controllo nuovamente i valori nulli
    print("--Valori nulli--")
    print(df.isnull().sum())
    print()

    # controllo il tipo di dato (object(str),float,int)
    print("--Informazioni--")
    print(df.info())
    print()

    # verifico che non ci siano valori = 0
    print(df.groupby(by="product_name_lenght").count())
    print()
    print(df.groupby(by="product_description_lenght").count())
    print()
    print(df.groupby(by="product_photos_qty").count())
    print()

    # rimpiazzo i valori nulli(nan) con "0" e li converto a valori interi
    # nelle colonne product_name_lenght, product_description_lenght, product_photos_qty

    df["product_name_lenght"] = df["product_name_lenght"].astype(str)
    df["product_name_lenght"] = df["product_name_lenght"].str.replace("nan", "0")
    df["product_name_lenght"] = df["product_name_lenght"].astype(float)
    df["product_name_lenght"] = df["product_name_lenght"].astype(int)

    df["product_description_lenght"] = df["product_description_lenght"].astype(str)
    df["product_description_lenght"] = df["product_description_lenght"].str.replace("nan", "0")
    df["product_description_lenght"] = df["product_description_lenght"].astype(float)
    df["product_description_lenght"] = df["product_description_lenght"].astype(int)

    df["product_photos_qty"] = df["product_photos_qty"].astype(str)
    df["product_photos_qty"] = df["product_photos_qty"].str.replace("nan", "0")
    df["product_photos_qty"] = df["product_photos_qty"].astype(float)
    df["product_photos_qty"] = df["product_photos_qty"].astype(int)

    # stampo il dataframe pulito
    print(df)

    #controllo i valori delle categorie
    # (il caricamento dei dati della tabella è fallito causa mancanza di dati categoria)
    pd.set_option("display.max.rows", None)
    print(df["category"].unique())

    #apro il collegamento al file in scrittura
    file_path = r"../data/clean/olistIT_products_clean.csv"
    with open (file_path, "w", encoding = "UTF-8", newline="") as file:
        writer = csv.writer(file)
        #inserisco come prima riga l'intestazione di colonna
        writer.writerow(df.columns)
        # inserisco le colonne successive
        for index, row in df.iterrows():
            writer.writerow(row)

    return df


def load_products(df):
    # recupero i dati dal file .csv
    df = pd.read_csv(r"../data/clean/olistIT_products_clean.csv")

    # stabilisco la connessione con il database
    with psycopg.connect(host=host,
                         dbname=dbname,
                         user=user,
                         password=password,
                         port=port) as conn:

        with conn.cursor() as cur:

            # scrivo ed eseguo la query per creare la tabella prodotti
            sql = """
            CREATE TABLE IF NOT EXISTS prodotti(
            pk_id_prodotto VARCHAR PRIMARY KEY,
            categoria VARCHAR,
            car_nome INTEGER,
            car_descrizione INTEGER,
            num_foto INTEGER
            );
            """

            # "FOREIGN KEY(fk_categoria) REFERENCES categorie(pk_categoria_inglese) ON DELETE CASCADE"
            cur.execute(sql)

            # scrivo ed eseguo la query per caricare i dati (il risultato del caricamento è in percentuale)
            sql = """INSERT INTO prodotti
            (pk_id_prodotto, categoria, car_nome, car_descrizione, num_foto)
            VALUES (%s, %s, %s, %s, %s);"""

            print(f"Caricamento in corso... {str(len(df))} righe da inserire.")

            perc_int = 0
            for index, row in df.iterrows():
                perc = float("%.2f" % ((index + 1) / len(df) * 100))
                if perc >= perc_int:
                    print(f"{round(perc)}% Completato")
                    perc_int += 5
                cur.execute(sql, row.to_list())
            conn.commit()

        # Caricamento completato!

def main():
    df = transform_products()
    load_products(df)

if __name__ == "__main__":
    main()