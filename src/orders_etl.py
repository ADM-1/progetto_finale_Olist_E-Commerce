from dotenv import load_dotenv
import pandas as pd
import psycopg
import os
import csv


#miglioro la visualizzazione della tabella
pd.set_option("display.max.columns", None)
pd.set_option("display.width", None)
#pd.set_option("display.max.rows", None)


def extract_orders():
    #creo dataframe da file raw
    df = pd.read_csv(r"../data/raw/olistIT_orders.csv")
    return df

def transform_orders():
    df = extract_orders()
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

    #controllo i vari tipi di stato degli ordini
    print(df.groupby(by="order_status").count())

    #controllo i tipi di dato
    print("--Informazioni--")
    print(df.info())
    print()

    #riempio i parametri vuoti con "2000-01-01 00:00:00" per standardizzare
    df["order_delivered_customer_date"] = df["order_delivered_customer_date"].fillna("2000-01-01 00:00:00")

    #controllo nuovamente che i valori siano unici
    print("--Valori Unici--")
    print(df.nunique())
    print()

    #controllo nuovamente che non ci siano valori nulli
    print("--Valori nulli--")
    print(df.isnull().sum())
    print()



    #salvo il dataframe pulito su un nuovo file .csv
    #definisco il percorso del file
    file_path = r"../data/clean/olistIT_orders_clean.csv"

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

def load_orders(df):
    #recupero i dati dal file .csv
    df = pd.read_csv(r"../data/clean/olistIT_orders_clean.csv")

    #stabilisco la connessione con il database
    with psycopg.connect(host = host,
                        dbname = dbname,
                        user = user,
                        password = password,
                        port = port) as conn:

        with conn.cursor() as cur:

        # scrivo ed eseguo la query per creare la tabella prodotti
            sql = """
            CREATE TABLE IF NOT EXISTS ordini(
            pk_id_ordine VARCHAR PRIMARY KEY,
            fk_id_cliente VARCHAR,
            stato_ordine VARCHAR,
            t_acquisto TIMESTAMP,
            t_consegna TIMESTAMP,
            t_stima_consegna DATE,
            FOREIGN KEY(fk_id_cliente) REFERENCES clienti(pk_id_cliente)
            ON DELETE CASCADE
            );
            """
            cur.execute(sql)

            # scrivo ed eseguo la query per caricare i dati (il risultato del caricamento è in percentuale)
            sql = """INSERT INTO ordini
            (pk_id_ordine, fk_id_cliente, stato_ordine, t_acquisto, t_consegna, t_stima_consegna)
            VALUES (%s, %s, %s, %s, %s, %s);"""

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
    df = transform_orders()
    load_orders(df)

if __name__ == "__main__":
    main()