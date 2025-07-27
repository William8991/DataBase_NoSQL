# Importo la libreria per Neo4j e Oracle.
import neo4j
# Importo il menu
import Menu
# Importo i due file per manipolare i database.
import Neo4jQuery
import OracleQuery
import cx_Oracle

# Connessione a Neo4j.
uri = "bolt://localhost:7687"
username = "neo4j"
password = "Password123!"
driverNeo4j = neo4j.GraphDatabase.driver(uri, auth=(username, password))
# dns per la connesione con  Oracle
dsn = cx_Oracle.makedsn('localhost', '1521', service_name='XE')
# Controllo la connessione a neo4j.
try:
    Neo4jQuery.check_connection(driverNeo4j)
    conn = cx_Oracle.connect(user='DATABASE2', password='DataBase2', dsn=dsn)
    print("Connessione effettuata (Oracle).")
    cursor = conn.cursor()
    cursor.callproc("manageBase.create_tables")
    Menu.menu(driverNeo4j, cursor, conn)
except Exception as e:
    print(f"Errore di connesione: {e}")
except cx_Oracle.DatabaseError as e:
    # Gestisce gli errori di connessione
    error, = e.args
    print(f"Errore durante la connessione al database: {error.message}")
finally:
    # Svuoto i database
    Neo4jQuery.deleteNeo4j(driverNeo4j)
    # Chiudo i driver di Neo4j
    driverNeo4j.close()
    # Chiusura Oracle
    try:
        OracleQuery.deleteOracle(cursor)
    except cx_Oracle.DatabaseError as e:
        print(f"Errore durante lo svuotamento delle tabelle Oracle: {e}")
    cursor.close()
    conn.close()
