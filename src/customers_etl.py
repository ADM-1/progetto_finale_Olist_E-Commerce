from dotenv import load_dotenv
import pandas as pd
import psycopg
import os

def extract_customers():
    #creo dataframe da file raw
    df = pd.read_csv(r"../data/raw/olistIT_customers.csv")
    return df

def transform_customers():
    df = extract_customers()
    #stampo il dataframe
    print(df)
    print()

    #controllo se i clienti unici sono 99441 come le righe e se le regioni uniche sono massimo 20
    print("--Valori Unici--")
    print(df.nunique())
    print()
    
    #controllo che non ci siano valori nulli
    print("--Valori nulli--")
    print(df.isnull().sum())
    print()
    return df
    
    #Tutto ok, quindi possiamo lavorare direttamente con il file grezzo
    
#recupero i dati di collegamento al database da .env3
load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")

def load_customers(df):
    #recupero i dati dal file .csv
    df = pd.read_csv(r"../data/raw/olistIT_customers.csv")
    
    #stabilisco la connessione con il database
    with psycopg.connect(host = host,
                         dbname = dbname,
                         user = user,
                         password = password,
                         port = port) as conn:

        with conn.cursor() as cur:
    
            #scrivo ed eseguo la query per creare la tabella clienti
            sql = """
            CREATE TABLE IF NOT EXISTS clienti(
            pk_id_cliente VARCHAR PRIMARY KEY,
            regione VARCHAR,
            citta VARCHAR,
            cap VARCHAR);
            """
    
            cur.execute(sql)
    
            #scrivo ed eseguo la query per caricare i dati (il risultato del caricamento è in percentuale)
            sql = """INSERT INTO clienti
            (pk_id_cliente, regione, citta, cap)
            VALUES (%s, %s, %s, %s);"""
    
            print(f"Caricamento in corso... {str(len(df))} righe da inserire.")
    
            perc_int = 0
            for index, row in df.iterrows():
                perc = float("%.2f" %((index + 1) / len(df) * 100))
                if perc >= perc_int:
                    print(f"{round(perc)}% Completato")
                    perc_int += 5
                cur.execute(sql, row.to_list())
            conn.commit()

    #Caricamento completato!

def main():
    df = transform_customers()
    load_customers(df)

if __name__ == "__main__":
    main()