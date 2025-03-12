from dotenv import load_dotenv
import pandas as pd
import psycopg
import os
import csv

#miglioro la visualizzazione della tabella
pd.set_option("display.max.columns", None)
pd.set_option("display.width", None)
#pd.set_option("display.max.rows", None)

def extract_op():
        #creo dataframe da file raw
        df = pd.read_csv(r"../data/raw/olistIT_orders_products.csv")

        return df

def transform_op():
    df = extract_op()
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

    #controllo a cosa fa riferimento la colonna "order_item"
    print(df.groupby(by="order_item").count())
    print(df.loc[df['order_item'] == 21])
    print(df.loc[df['order_id'] == "8272b63d03f5f79c56e9e4120aec44ef"])

    return df

#Tutto ok, quindi possiamo lavorare direttamente con il file grezzo

#recupero i dati di collegamento al database da .env3
load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")

def load_op(df):
    #recupero i dati dal file .csv
    df = pd.read_csv(r"../data/raw/olistIT_orders_products.csv")

    #stabilisco la connessione con il database
    with psycopg.connect(host = host,
                         dbname = dbname,
                         user = user,
                         password = password,
                         port = port) as conn:

        with conn.cursor() as cur:

            # scrivo ed eseguo la query per creare la tabella prodotti
            sql = """
            CREATE TABLE IF NOT EXISTS prodotti_ordinati(
            pk_prodotto_ordinato SERIAL PRIMARY KEY,
            fk_id_ordine VARCHAR,
            prodotto_num INTEGER,
            fk_id_prodotto VARCHAR,
            id_venditore VARCHAR,
            prezzo FLOAT,
            p_trasporto FLOAT,
            FOREIGN KEY(fk_id_ordine) REFERENCES ordini(pk_id_ordine)
            ON DELETE CASCADE,
            FOREIGN KEY(fk_id_prodotto) REFERENCES prodotti(pk_id_prodotto)
            ON DELETE CASCADE
            );
            """
            cur.execute(sql)

            # scrivo ed eseguo la query per caricare i dati (il risultato del caricamento è in percentuale)
            sql = """INSERT INTO prodotti_ordinati
            (fk_id_ordine, prodotto_num, fk_id_prodotto, id_venditore, prezzo, p_trasporto)
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
    df = transform_op()
    load_op(df)

if __name__ == "__main__":
    main()