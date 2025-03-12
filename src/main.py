import psycopg
import customers_etl as customers
import categories_etl as categories
import products_etl as products
import orders_etl as orders
import op_etl as op


def play():
    customers.main()
    categories.main()
    products.main()
    orders.main()
    op.main()



print("Caricamento database in corso...")
print()
try:
    play()
except psycopg.OperationalError:
    print("""\n\n\n\n\n\n-/!\\-Caricamento fallito-/!\\-
            \nErrore:\n----Connessione al database fallita!----
            \nCreare il database o controllare file .env prima di riavviare il programma!\n""")
except psycopg.errors.UniqueViolation:
    print("""\n\n\n\n\n\n-/!\\-Caricamento fallito-/!\\-
                \nErrore:\n----Database sporco!----
                \nSvuotare il database prima di riavviare il programma!\n""")
else:
    print()
    print("Caricamento completato!")