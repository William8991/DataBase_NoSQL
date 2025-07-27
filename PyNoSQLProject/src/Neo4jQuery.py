import time

# Metodo per verificare la connessione al database.
def check_connection(driver):
    with driver.session() as session:
        try:
            result = session.run("RETURN 1")
            if result.single()[0] == 1:
                print("Connessione effettuata (Neo4j).")
            else:
                print("Connessione fallita (Neo4j).")
        except Exception as e:
            print("Errore durante la verifica della connessione:", e)

# Svuota database Neo4j
def deleteNeo4j(driver):
    with driver.session() as session:
        try:
            session.run("MATCH (n) DETACH DELETE n")
            print("Neo4j svuotato.")
        except Exception as e:
            print("Errore svuotamento Neo4j:", e)

# Metodo per caricare i dati
def datiNeo4j(driver, numero):
    with driver.session() as session:
        prima_parte_luogo = "LOAD CSV WITH HEADERS FROM 'file:///nodo_luogo"
        seconda_parte_luogo = (".csv' AS row MERGE (l:Luogo {cap: toInteger(row.cap)}) "
                               "SET l.citta = row.citta, l.nome = row.nome, "
                               "l.paese = row.paese;")
        prima_parte_persona = "LOAD CSV WITH HEADERS FROM 'file:///nodo_persona"
        seconda_parte_persona = (
            ".csv' AS row MERGE (p:Persona {codiceFiscale: row.codiceFiscale}) "
            "SET p.nome = row.nome, "
            "p.cognome = row.cognome, "
            "p.eta = toInteger(row.eta), "
            "p.precedenti = row.precedenti, "
            "p.citta = row.citta, "
            "p.ruolo = row.ruolo, "
            "p.sesso = row.sesso;"
        )
        prima_parte_oggetto = "LOAD CSV WITH HEADERS FROM 'file:///nodo_oggetto"
        seconda_parte_oggetto = (
            ".csv' AS row MERGE (o:Oggetto {numeroSerie: toInteger(row.numeroSerie)}) "
            "SET o.nome = row.nome;"
        )
        prima_parte_evento = "LOAD CSV WITH HEADERS FROM 'file:///nodo_evento"
        seconda_parte_evento = (
            ".csv' AS row MERGE (e:Evento {id: toInteger(row.id)}) "
            "SET e.data = date(row.data), "
            "e.nome = row.nome;"
        )
        for i in range(numero):
            if i == 0 :
                percorso_luogo = prima_parte_luogo + seconda_parte_luogo
                percorso_persona = prima_parte_persona + seconda_parte_persona
                percorso_oggetto = prima_parte_oggetto + seconda_parte_oggetto
                percorso_evento = prima_parte_evento + seconda_parte_evento
            else:
                carattere = str(i+1)
                percorso_luogo = prima_parte_luogo + carattere + seconda_parte_luogo
                percorso_persona = prima_parte_persona + carattere + seconda_parte_persona
                percorso_oggetto = prima_parte_oggetto + carattere + seconda_parte_oggetto
                percorso_evento = prima_parte_evento + carattere + seconda_parte_evento
            session.run(percorso_luogo)
            session.run(percorso_persona)
            session.run(percorso_oggetto)
            session.run(percorso_evento)
        result1 = session.run("MATCH (l:Luogo) RETURN l LIMIT 10")
        result2 = session.run("MATCH (l:Persona) RETURN l LIMIT 10")
        result3 = session.run("MATCH (l:Oggetto) RETURN l LIMIT 10")
        result4 = session.run("MATCH (l:Evento) RETURN l LIMIT 10")
        if not result1.peek() or not result2.peek() or not result3.peek() or not result4.peek():
            print("Errore caricamento. (Neo4j)")
        else:
            print("Caricamento dati completato. (Neo4j)")

# Metodo per creare le relazioni tra i nodi
def creaRelazione(driver):
    with driver.session() as session:
        # Relazione univoca Persona->Evento
        session.run("""MATCH (p:Persona) WHERE NOT (p)--() WITH collect(p) AS persone
                    MATCH (e:Evento) WHERE NOT (e)<-[:Partecipa]-() WITH persone, collect(e) AS eventi
                    UNWIND range(0, size(persone) - 1) AS index WITH persone[index]
                    AS persona, eventi[index % size(eventi)]
                    AS evento CREATE (persona)-[:Partecipa]->(evento)""")
        # Relazione 70% Persona->Evento
        session.run("""MATCH (p:Persona) WITH collect(p) AS persone WITH size(persone)
                    AS total, persone, toInteger(0.7 * size(persone)) AS limite
                    WITH persone[0..limite] AS persone_selezionate
                    MATCH (e:Evento) WITH persone_selezionate, collect(e) AS eventi
                    UNWIND persone_selezionate AS persona WITH persona, eventi
                    WITH persona, eventi, toInteger(rand() * size(eventi)) AS randomIndex
                    WITH persona, eventi[randomIndex] AS evento WHERE NOT (persona)-[:Partecipa]->(evento)
                    MERGE (persona)-[:Partecipa]->(evento) // Crea la relazione se non esiste già""")
        # Relazione Evento->Luogo
        session.run("""MATCH (e:Evento) WITH collect(e) AS eventi MATCH (l:Luogo)
                    WHERE NOT (l)<-[:Avviene_in]-() WITH eventi, collect(l) AS luoghi
                    UNWIND range(0, size(eventi) - 1) AS index WITH eventi[index]
                    AS evento, luoghi[index % size(luoghi)] AS luogo
                    MERGE (evento)-[:Avviene_in]->(luogo)""")
        # Relazione Persona->Oggetto
        session.run("""MATCH (p:Persona{ruolo:'Criminale'}) WITH collect(p) AS persone
                    MATCH (o:Oggetto) WHERE NOT (o)<-[:Usa]-() WITH persone, collect(o) AS oggetti
                    UNWIND range(0, size(persone) - 1) AS index WITH persone[index]
                    AS persona, oggetti[index % size(oggetti)] AS oggetto
                    CREATE (persona)-[:Usa]->(oggetto) // Crea la relazione""")
        # Relazione Oggetto->Evento
        session.run("""MATCH (o:Oggetto) WITH collect(o) AS oggetti MATCH (e:Evento)
                    WHERE NOT (e)<-[:Presente_in]-() WITH oggetti, collect(e) AS eventi
                    UNWIND range(0, size(oggetti) - 1) AS index WITH oggetti[index]
                    AS oggetto, eventi[index % size(eventi)] AS evento
                    CREATE (oggetto)-[:Presente_in]->(evento) // Crea la relazione""")
        # Query per il controllo delle relazioni
        relazioni = {
            "Partecipa": "MATCH (:Persona)-[:Partecipa]->(:Evento) RETURN 1 LIMIT 1",
            "Avviene_in": "MATCH (:Evento)-[:Avviene_in]->(:Luogo) RETURN 1 LIMIT 1",
            "Usa": "MATCH (:Persona)-[:Usa]->(:Oggetto) RETURN 1 LIMIT 1",
            "Presente_in": "MATCH (:Oggetto)-[:Presente_in]->(:Evento) RETURN 1 LIMIT 1"
        }
        for nome, query in relazioni.items():
            result = session.run(query).single()
            if result:
                continue
            else:
                print(f"Errore: relazione '{nome}' non creata.")
                break
        print("Caricamento relazioni completato. (Neo4j)")

# Complessità 1
def neo4jComplessita1(driver, label):
    lista_tempo = []
    somma_tempo = 0
    tempo_iniziale = 0
    iterations = 31
    last_result = None
    for i in range(iterations):
        query = f"MATCH (n:{label}) RETURN n"
        with driver.session() as session:
            start_time = time.time()
            result = session.run(query)
            end_time = time.time()
            timediff = (end_time - start_time) * 1000
            if tempo_iniziale == 0:
                tempo_iniziale = timediff
            else:
                lista_tempo.append(timediff)
                somma_tempo += timediff
            last_result = [record["n"] for record in result]
    media_tempo = somma_tempo/30
    return last_result, tempo_iniziale, media_tempo, lista_tempo

'''
# Complessità 1
def noe4jComlessita1(driver, label):
    query = f"MATCH (n:{label}) RETURN n"
    with driver.session() as session:
        start_time = time.time()
        result = session.run(query)
        end_time = time.time()
        timediff = (end_time - start_time) * 1000
        last_result = [record["n"] for record in result]
    return last_result, timediff
'''

# Complessità 2
def neo4jComplessita2(driver, label):
    lista_tempo = []
    tempo_iniziale = 0
    somma_tempo = 0
    iterations = 31
    last_result = None
    for i in range(iterations):
        query = f"MATCH (p:{label})-[r]->(n) RETURN p,r,n"
        with driver.session() as session:
            start_time = time.time()
            result = session.run(query)
            end_time = time.time()
            timediff = (end_time - start_time) * 1000
            if tempo_iniziale == 0:
                tempo_iniziale = timediff
            else:
                lista_tempo.append(timediff)
                somma_tempo += timediff
            last_result = [(record["p"], record["r"], record["n"]) for record in result]
    media_tempo = somma_tempo / 30
    return last_result, tempo_iniziale, media_tempo, lista_tempo

'''
# Complessità 2
def noe4jComlessita2(driver, label):
    query = f"MATCH (p:{label})-[r]->(n) RETURN p,r,n"
    with driver.session() as session:
        start_time = time.time()
        result = session.run(query)
        end_time = time.time()
        timediff = (end_time - start_time) * 1000
        last_result = [(record["p"], record["r"], record["n"]) for record in result]
    return last_result, timediff
'''

# Complessità 3
def neo4jComplessita3(driver, label):
    lista_tempo = []
    tempo_iniziale = 0
    somma_tempo = 0
    iterations = 31
    persone_con_relazioni = None
    for i in range(iterations):
        query = (f"MATCH (p:{label})-[r]->(n) "+
        "WITH p, collect(r) AS relazioni, collect(n) AS nodi_connessi, count(r) AS num_relazioni "+
        "RETURN p, relazioni, nodi_connessi, num_relazioni "+
        "ORDER BY num_relazioni DESC ")
        with driver.session() as session:
            start_time = time.time()
            result = session.run(query)
            end_time = time.time()
            timediff = (end_time - start_time) * 1000
            if tempo_iniziale == 0:
                tempo_iniziale = timediff
            else:
                lista_tempo.append(timediff)
                somma_tempo += timediff
            persone_con_relazioni = [
                (record["p"], record["relazioni"], record["nodi_connessi"], record["num_relazioni"])
                for record in result
            ]
    for persona, relazioni, nodi_connessi, num_relazioni in persone_con_relazioni:
        props_persona = persona._properties
        print(f"Persona ID: {persona.id}")
        for chiave, valore in props_persona.items():
            print(f"  {chiave}: {valore}")
        print(f"  Numero di connessioni: {num_relazioni}")
        print("  Relazioni:")
        for relazione in relazioni:
            print(f"    Tipo: {relazione.type}")
        print("  Nodi connessi:")
        for nodo in nodi_connessi:
            props_nodo = nodo._properties
            print(f"    Nodo ID: {nodo.id}")
            for chiave, valore in props_nodo.items():
                print(f"      {chiave}: {valore}")

        print("-" * 40)
    media_tempo = somma_tempo / 30
    return tempo_iniziale, media_tempo, lista_tempo

'''
# Complessità 3
def noe4jComlessita3(driver, label):
    query = (f"MATCH (p:{label})-[r]->(n) "+
    "WITH p, collect(r) AS relazioni, collect(n) AS nodi_connessi, count(r) AS num_relazioni "+
    "RETURN p, relazioni, nodi_connessi, num_relazioni "+
    "ORDER BY num_relazioni DESC "+
    "LIMIT 10")
    with driver.session() as session:
        start_time = time.time()
        result = session.run(query)
        end_time = time.time()
        timediff = (end_time - start_time) * 1000
        persone_con_relazioni = [
            (record["p"], record["relazioni"], record["nodi_connessi"], record["num_relazioni"])
            for record in result
        ]
    for persona, relazioni, nodi_connessi, num_relazioni in persone_con_relazioni:
        props_persona = persona._properties
        print(f"Persona ID: {persona.id}")
        for chiave, valore in props_persona.items():
            print(f"  {chiave}: {valore}")
        print(f"  Numero di connessioni: {num_relazioni}")
        print("  Relazioni:")
        for relazione in relazioni:
            print(f"    Tipo: {relazione.type}")
        print("  Nodi connessi:")
        for nodo in nodi_connessi:
            props_nodo = nodo._properties
            print(f"    Nodo ID: {nodo.id}")
            for chiave, valore in props_nodo.items():
                print(f"      {chiave}: {valore}")

        print("-" * 40)
    return timediff
'''

# Complessità 4
def neo4jComplessita4(driver, label):
    lista_tempo = []
    tempo_iniziale = 0
    somma_tempo = 0
    iterations = 31
    for i in range(iterations):
        query = f"""
            MATCH (p1:{label})-[r:Avviene_in|Usa|Partecipa|Presente_in]->(n)<-[r2]-(p2:{label})
            WHERE p1.codiceFiscale <> p2.codiceFiscale
            WITH p1, p2, collect(DISTINCT type(r)) AS tipi_relazioni, 
                count(r) AS num_connessioni
            OPTIONAL MATCH (p1)-[]->()
            WITH p1, p2, tipi_relazioni, num_connessioni, count(*) AS grado_medio
            RETURN p1, p2, tipi_relazioni, num_connessioni, grado_medio
            ORDER BY num_connessioni DESC """

        with driver.session() as session:
            start_time = time.time()
            result = session.run(query)
            end_time = time.time()
            timediff = (end_time - start_time) * 1000
            if tempo_iniziale == 0:
                tempo_iniziale = timediff
            else:
                lista_tempo.append(timediff)
                somma_tempo += timediff
            connessioni_complesse = [
                (record["p1"], record["p2"], record["tipi_relazioni"], record["num_connessioni"], record["grado_medio"])
                for record in result
            ]
    if not connessioni_complesse:
        print("Nessuna relazione complessa trovata nel database.")
        return
    print("Analisi della rete sociale complessa:")
    for p1, p2, tipi_relazioni, num_connessioni, grado_medio in connessioni_complesse:
        # Stampa dettagli delle due persone
        print(f"Persona 1 ID: {p1.id}")
        for chiave, valore in p1._properties.items():
            print(f"  {chiave}: {valore}")
        print(f"  Collegata con:")
        print(f"Persona 2 ID: {p2.id}")
        for chiave, valore in p2._properties.items():
            print(f"  {chiave}: {valore}")
        # Stampa dettagli delle relazioni
        print("  Relazioni condivise:")
        for tipo in tipi_relazioni:
            print(f"    Tipo: {tipo}")
        print(f"  Numero di connessioni: {num_connessioni}")
        print(f"  Grado medio di connessione: {grado_medio:.2f}")
        print("-" * 40)
    media_tempo = somma_tempo / 30
    return tempo_iniziale, media_tempo, lista_tempo

'''
# Complessità 4
def noe4jComlessita4(driver, label):
    query = f"""
        MATCH (p1:{label})-[r:Avviene_in|Usa|Partecipa|Presente_in]->(n)<-[r2]-(p2:{label})
        WHERE p1.codiceFiscale <> p2.codiceFiscale
        WITH p1, p2, collect(DISTINCT type(r)) AS tipi_relazioni, 
            count(r) AS num_connessioni
        OPTIONAL MATCH (p1)-[]->()
        WITH p1, p2, tipi_relazioni, num_connessioni, count(*) AS grado_medio
        RETURN p1, p2, tipi_relazioni, num_connessioni, grado_medio
        ORDER BY num_connessioni DESC
        LIMIT 10
        """
    with driver.session() as session:
        start_time = time.time()
        result = session.run(query)
        end_time = time.time()
        timediff = (end_time - start_time) * 1000
        connessioni_complesse = [
            (record["p1"], record["p2"], record["tipi_relazioni"], record["num_connessioni"], record["grado_medio"])
            for record in result
        ]
    if not connessioni_complesse:
        print("Nessuna relazione complessa trovata nel database.")
        return
    print("Analisi della rete sociale complessa:")
    for p1, p2, tipi_relazioni, num_connessioni, grado_medio in connessioni_complesse:
        # Stampa dettagli delle due persone
        print(f"Persona 1 ID: {p1.id}")
        for chiave, valore in p1._properties.items():
            print(f"  {chiave}: {valore}")
        print(f"  Collegata con:")
        print(f"Persona 2 ID: {p2.id}")
        for chiave, valore in p2._properties.items():
            print(f"  {chiave}: {valore}")
        # Stampa dettagli delle relazioni
        print("  Relazioni condivise:")
        for tipo in tipi_relazioni:
            print(f"    Tipo: {tipo}")
        print(f"  Numero di connessioni: {num_connessioni}")
        print(f"  Grado medio di connessione: {grado_medio:.2f}")
        print("-" * 40)
    return timediff
'''

# Stampa i nodi
def stampaNodi(nodi):
    for nodo in nodi:
        props = nodo._properties
        print(f"Persona ID: {nodo.id}")
        for chiave, valore in props.items():
            print(f" {chiave}: {valore}")
        print("-" * 20)

# Stampa nodi + relazioni
def stampaNodiRelazioni(nodi):
    for persona, relazione, nodo_connesso in nodi:
        props_persona = persona._properties
        print(f"Persona ID: {persona.id}")
        for chiave, valore in props_persona.items():
            print(f"  {chiave}: {valore}")
        print(f"  Relazione: {relazione.type}")
        props_nodo_connesso = nodo_connesso._properties
        print(f"  Nodo connesso ID: {nodo_connesso.id}")
        for chiave, valore in props_nodo_connesso.items():
            print(f"    {chiave}: {valore}")
        print("-" * 30)