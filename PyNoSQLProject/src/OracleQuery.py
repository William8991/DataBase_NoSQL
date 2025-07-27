import csv
import time

# Drop delle tabelle
def deleteOracle(cursor):
    cursor.callproc("manageBase.drop_tables")
    print("Oracle svuotato.")

# Crea relazioni
def creaRelazioniOracle(cursor):
        cursor.callproc("relazioni.crea_relazioni_persona_evento")
        cursor.callproc("relazioni.crea_relazioni_persona_oggetto")
        cursor.callproc("relazioni.crea_relazioni_oggetto_evento")
        cursor.callproc("relazioni.crea_relazioni_evento_luogo")
        print("Caricamento relazioni completato. (Oracle)")

# Caricamento 100%
def datiOracle(cursor, conn, numero):
    prima_parte_persona = "nodi/nodo_persona"
    prima_parte_evento = "nodi/nodo_evento"
    prima_parte_luogo = "nodi/nodo_luogo"
    prima_parte_oggetto = "nodi/nodo_oggetto"
    seconda_parte = ".csv"
    for i in range(numero):
        if i == 0 :
            percorso_persona = prima_parte_persona + seconda_parte
            percorso_evento = prima_parte_evento + seconda_parte
            percorso_luogo = prima_parte_luogo + seconda_parte
            percorso_oggetto = prima_parte_oggetto + seconda_parte
        else :
            carattere = str(i+1)
            percorso_persona = prima_parte_persona + carattere + seconda_parte
            percorso_evento = prima_parte_evento + carattere + seconda_parte
            percorso_luogo = prima_parte_luogo + carattere + seconda_parte
            percorso_oggetto = prima_parte_oggetto + carattere + seconda_parte
        try:
            with open(percorso_persona, "r") as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    try:
                        cursor.execute("""
                            INSERT INTO persona (citta, codiceFiscale, cognome, eta, nome, precedenti, ruolo, sesso)
                            VALUES (:1, :2, :3, :4, :5, :6, :7, :8)
                        """, row)
                    except Exception as e:
                        print(f"Errore nell'inserimento della riga {row}: {e}")
            with open(percorso_evento, "r") as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    try:
                        cursor.execute("""
                            INSERT INTO evento (id, data, nome)
                            VALUES (:1, TO_DATE(:2, 'YYYY-MM-DD'), :3)
                        """, row)
                    except Exception as e:
                        print(f"Errore nell'inserimento della riga {row}: {e}")
            with open(percorso_luogo, "r") as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    try:
                        cursor.execute("""
                            INSERT INTO luogo (cap, citta, nome, paese)
                            VALUES (:1, :2, :3, :4)
                        """, row)
                    except Exception as e:
                        print(f"Errore nell'inserimento della riga {row}: {e}")
            with open(percorso_oggetto, "r") as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    try:
                        cursor.execute("""
                            INSERT INTO oggetti (nome, numeroSerie)
                            VALUES (:1, :2)
                        """, row)
                    except Exception as e:
                        print(f"Errore nell'inserimento della riga {row}: {e}")
            conn.commit()
        except Exception as e:
            print(f"Errore durante il caricamento: {e}")
            conn.rollback()
    print("Caricamento dati completato. (Oracle)")

def oracleComplessita1(cursor, nome_tabella):
    lista_tempo = []
    tempo_iniziale = 0
    somma_tempo = 0
    iterazioni = 31
    query = f"SELECT * FROM {nome_tabella}"
    for i in range(iterazioni):
        start_time = time.time()
        cursor.execute(query)
        result = cursor.fetchall()
        end_time = time.time()
        time_difference = (end_time - start_time) * 1000
        if tempo_iniziale == 0:
            tempo_iniziale = time_difference
        else:
            lista_tempo.append(time_difference)
            somma_tempo += time_difference
    media_tempo = somma_tempo / 30
    return result, tempo_iniziale, media_tempo, lista_tempo

def oracleComplessita2(cursor, nome_tabella1, nome_relazione, nome_tabella2):
    lista_tempo = []
    tempo_iniziale = 0
    somma_tempo = 0
    iterazioni = 31
    query = f"""
            SELECT p.*, r.*, n.*
            FROM {nome_tabella1} p
            JOIN {nome_relazione} r ON p.codiceFiscale = r.id_persona
            JOIN {nome_tabella2} n ON r.id_evento = n.id
        """
    for i in range(iterazioni):
        start_time = time.time()
        cursor.execute(query)
        result = cursor.fetchall()
        end_time = time.time()
        time_difference = (end_time - start_time) * 1000
        if tempo_iniziale == 0:
            tempo_iniziale = time_difference
        else:
            lista_tempo.append(time_difference)
            somma_tempo += time_difference
    media_tempo = somma_tempo / 30
    return result, tempo_iniziale, media_tempo, lista_tempo

def oracleComplessita3(cursor):
    lista_tempo = []
    tempo_iniziale = 0
    somma_tempo = 0
    iterazioni = 31
    query = f"""
                SELECT *
                FROM (
                    SELECT 
                        p.codiceFiscale, 
                        p.nome, 
                        COLLECT(r.id_evento) AS relazioni, 
                        COLLECT(n.id) AS nodi_connessi, 
                        COUNT(r.id_evento) AS num_relazioni
                    FROM 
                        persona p
                    JOIN 
                        persona_evento r ON p.codiceFiscale = r.id_persona
                    JOIN 
                        evento n ON r.id_evento = n.id
                    GROUP BY 
                        p.codiceFiscale, p.nome
                    ORDER BY 
                        COUNT(r.id_evento) DESC
                )"""

    for i in range(iterazioni):
        start_time = time.time()
        cursor.execute(query)
        result = cursor.fetchall()
        end_time = time.time()
        time_difference = (end_time - start_time) * 1000
        if tempo_iniziale == 0:
            tempo_iniziale = time_difference
        else:
            lista_tempo.append(time_difference)
            somma_tempo += time_difference
    media_tempo = somma_tempo / 30
    return result, tempo_iniziale, media_tempo, lista_tempo

def oracleComplessita4(cursor):
    lista_tempo = []
    tempo_iniziale = 0
    somma_tempo = 0
    iterazioni = 31
    query = f"""
                    SELECT *
                    FROM (
                        SELECT 
                            p1.codiceFiscale AS codiceFiscale_p1,
                            p2.codiceFiscale AS codiceFiscale_p2,
                            COLLECT(DISTINCT r.tipo_relazione) AS tipi_relazioni,
                            COUNT(r.id_relazione) AS num_connessioni,
                            (
                                SELECT COUNT(*) 
                                FROM persona_evento r_sub 
                                WHERE r_sub.id_persona = p1.codiceFiscale
                            ) AS grado_medio
                        FROM 
                            persona p1
                        JOIN 
                            persona_evento r ON p1.codiceFiscale = r.id_persona
                        JOIN 
                            evento n ON r.id_evento = n.id
                        JOIN 
                            persona_evento r2 ON n.id = r2.id_evento
                        JOIN 
                            persona p2 ON r2.id_persona = p2.codiceFiscale
                        WHERE 
                            p1.codiceFiscale <> p2.codiceFiscale
                        GROUP BY 
                            p1.codiceFiscale, p2.codiceFiscale
                        ORDER BY 
                            COUNT(r.id_relazione) DESC
                    )"""

    for i in range(iterazioni):
        start_time = time.time()
        cursor.execute(query)
        result = cursor.fetchall()
        end_time = time.time()
        time_difference = (end_time - start_time) * 1000
        if tempo_iniziale == 0:
            tempo_iniziale = time_difference
        else:
            lista_tempo.append(time_difference)
            somma_tempo += time_difference
    media_tempo = somma_tempo / 30
    return result, tempo_iniziale, media_tempo, lista_tempo