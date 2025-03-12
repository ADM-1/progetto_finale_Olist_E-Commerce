from dotenv import load_dotenv
import pandas as pd
import psycopg
import os
import csv

#miglioro la visualizzazione della tabella
pd.set_option("display.max.columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max.rows", None)

def extract_categories():
    #creo dataframe da file raw
    df = pd.read_csv(r"../data/raw/olistIT_categories.csv")
    return df

def transform_categories():
    df = extract_categories()
    #stampo il dataframe
    print(df)
    print()
# il file ha 71 righe e 2 colonne

#lavorando il file products si è notata la mancanza di queste categorie
    df.loc[len(df)] = ["pc gamer", "giochi pc"]
    df.loc[len(df)] = ["video photo", "video foto"]
    df.loc[len(df)] = ["uncategorized","senza categoria"]

#controllo che i valori siano unici
    print("--Valori Unici--")
    print(df.nunique())
    print()

#controllo che non ci siano valori nulli
    print("--Valori nulli--")
    print(df.isnull().sum())
    print()

    #controllando il dataframe si sono notati diversi errori, si risolvono di seguito:
    # elimino appendice 2 da alcuni record
    df["product_category_name_english"] = df["product_category_name_english"].str.replace("_2", "")
    df["product_category_name_italian"] = df["product_category_name_italian"].str.replace("_2", "")

    # si sostituiscono i trattini bassi con degli spazi
    df["product_category_name_english"] = df["product_category_name_english"].str.replace("_", " ")
    df["product_category_name_italian"] = df["product_category_name_italian"].str.replace("_", " ")

    # si eliminano eventuali spazi
    df["product_category_name_english"] = df["product_category_name_english"].str.strip()
    df["product_category_name_italian"] = df["product_category_name_italian"].str.strip()

    # si aggiustano i valori con errori di battitura
    df["product_category_name_english"] = df["product_category_name_english"].str.replace("fashio ", "fashion ")
    df["product_category_name_english"] = df["product_category_name_english"].str.replace("costruction", "construction")
    df["product_category_name_english"] = df["product_category_name_english"].str.replace("confort", "comfort")
    df["product_category_name_english"] = df["product_category_name_english"].str.replace("childrens", "children")

    #controllo che non ci siano duplicati
    print("--Valori Unici--")
    print(df.nunique())
    print()

    #elimino i duplicati
    df = df.drop_duplicates()

    #print(df["product_category_name_english"].unique())
    #print(df.groupby(by="product_category_name_english").count())

    #stampo il dataframe pulito
    print(df)
    print()

    #salvo il dataframe pulito su un nuovo file .csv
    #definisco il percorso del file
    file_path = r"../data/clean/olistIT_categories_clean.csv"

    #apro il collegamento al file in scrittura
    with open (file_path, "w", encoding = "UTF-8", newline="") as file:
        writer = csv.writer(file)
        #inserisco come prima riga l'intestazione di colonna
        writer.writerow(df.columns)
        #inserisco le colonne successive
        for index, row in df.iterrows():
            writer.writerow(row)
    return df

    # FINE PULIZIA DATI!

#recupero i dati di collegamento al database da .env3
load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")


def load_categories(df):
    #recupero i dati dal file .csv
    df = pd.read_csv(r"../data/clean/olistIT_categories_clean.csv")

    #stabilisco la connessione con il database
    with psycopg.connect(host = host,
                         dbname = dbname,
                         user = user,
                         password = password,
                         port = port) as conn:

        with conn.cursor() as cur:

            # scrivo ed eseguo la query per creare la tabella categorie
            sql = """
            CREATE TABLE IF NOT EXISTS categorie(
            pk_id_categoria SERIAL PRIMARY KEY,
            categoria_inglese VARCHAR,
            categoria_italiano VARCHAR);
            """

            cur.execute(sql)

            # scrivo ed eseguo la query per caricare i dati (il risultato del caricamento è in percentuale)
            sql = """INSERT INTO categorie
            (categoria_inglese,categoria_italiano)
            VALUES (%s, %s);"""

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
    df = transform_categories()
    load_categories(df)

if __name__ == "__main__":
    main()