import Neo4jQuery
import OracleQuery
import Confidenza

def menu(driverNeo4j, cursor, conn):
    menu0 = False
    menu1 = False
    # Menu principale per la scelta del dataset.
    while not menu0 and not menu1:
        print("Scegliere il dataset.")
        print("0) 100%\n1) 75%\n2) 50%\n3) 25%\n4) Esci")
        cmd = input()
        match cmd:
            case "0":
                # Caricamento dei nodi Neo4j
                Neo4jQuery.datiNeo4j(driverNeo4j,25)
                Neo4jQuery.creaRelazione(driverNeo4j)
                # Caricamento dati Oracle
                OracleQuery.datiOracle(cursor, conn, 25)
                OracleQuery.creaRelazioniOracle(cursor)
                menu1 = True
            case "1":
                # Caricamento dei nodi Neo4j
                Neo4jQuery.datiNeo4j(driverNeo4j, 19)
                Neo4jQuery.creaRelazione(driverNeo4j)
                # Caricamento dati Oracle
                OracleQuery.datiOracle(cursor, conn, 19)
                OracleQuery.creaRelazioniOracle(cursor)
                menu1 = True
            case "2":
                # Caricamento dei nodi Neo4j
                Neo4jQuery.datiNeo4j(driverNeo4j, 12)
                Neo4jQuery.creaRelazione(driverNeo4j)
                # Caricamento dati Oracle
                OracleQuery.datiOracle(cursor, conn, 12)
                OracleQuery.creaRelazioniOracle(cursor)
                menu1 = True
            case "3":
                # Caricamento dei nodi Neo4j
                Neo4jQuery.datiNeo4j(driverNeo4j, 6)
                Neo4jQuery.creaRelazione(driverNeo4j)
                # Caricamento dati Oracle
                OracleQuery.datiOracle(cursor, conn, 6)
                OracleQuery.creaRelazioniOracle(cursor)
                menu1 = True
            case "4":
                print("Arrivederci.")
                menu0 = True
                menu1 = True
            case _:
                print("Opzione non valida.")

        """ Menu per la scelta della complessità."""
        while not menu0 and menu1:
            print("Scegliere un opzione.")
            print("0) Complessità 1\n1) Complessità 2\n2) Complessità 3\n3) Complessità 4\n4) Indietro\n5) Esci")
            cmd = input()
            match cmd:
                case "0":
                    nodi, neo4jTempoIniziale, neo4jTempoMedio, neo4jListaTempo = Neo4jQuery.neo4jComplessita1(driverNeo4j, "Persona")
                    media, margine_di_errore, neo4jIntervallo = Confidenza.intervalloDiConfidenza(neo4jListaTempo)
                    Neo4jQuery.stampaNodi(nodi)
                    print(f"Tempo della prima esecuzione: {neo4jTempoIniziale:.2f} ms (Neo4j)")
                    print(f"Tempo medio delle successive 30 iterazioni: {neo4jTempoMedio:.2f} ms (Neo4j)")
                    print(f"Intervallo di confidenza al 95%: {neo4jIntervallo[0]:.2f} ms - {neo4jIntervallo[1]:.2f} ms (Neo4j)")
                    #Oracle
                    result, oracleTempoIniziale, oracleTempoMedio, oracleListaTempo = OracleQuery.oracleComplessita1(cursor, "persona")
                    media, margine_di_errore, oracleIntervallo = Confidenza.intervalloDiConfidenza(oracleListaTempo)
                    print(f"Tempo della prima esecuzione: {oracleTempoIniziale:.2f} ms (Oracle)")
                    print(f"Tempo medio delle successive 30 iterazioni: {oracleTempoMedio:.2f} ms (Oracle)")
                    print(f"Intervallo di confidenza al 95%: {oracleIntervallo[0]:.2f} ms - {oracleIntervallo[1]:.2f} ms (Oracle)")
                case "1":
                    nodi, neo4jTempoIniziale, neo4jTempoMedio, neo4jListaTempo = Neo4jQuery.neo4jComplessita2(driverNeo4j, "Persona")
                    media, margine_di_errore, neo4jIntervallo = Confidenza.intervalloDiConfidenza(neo4jListaTempo)
                    Neo4jQuery.stampaNodiRelazioni(nodi)
                    print(f"Tempo della prima esecuzione: {neo4jTempoIniziale:.2f} ms (Neo4j)")
                    print(f"Tempo medio delle successive 30 iterazioni: {neo4jTempoMedio:.2f} ms (Neo4j)")
                    print(f"Intervallo di confidenza al 95%: {neo4jIntervallo[0]:.2f} ms - {neo4jIntervallo[1]:.2f} ms (Neo4j)")
                    result, oracleTempoIniziale, oracleTempoMedio, oracleListaTempo = OracleQuery.oracleComplessita2(cursor, "persona", "persona_evento", "evento")
                    media, margine_di_errore, oracleIntervallo = Confidenza.intervalloDiConfidenza(oracleListaTempo)
                    print(f"Tempo della prima esecuzione: {oracleTempoIniziale:.2f} ms (Oracle)")
                    print(f"Tempo medio delle successive 30 iterazioni: {oracleTempoMedio:.2f} ms (Oracle)")
                    print(f"Intervallo di confidenza al 95%: {oracleIntervallo[0]:.2f} ms - {oracleIntervallo[1]:.2f} ms (Oracle)")
                case "2":
                    neo4jTempoIniziale, neo4jTempoMedio, neo4jListaTempo = Neo4jQuery.neo4jComplessita3(driverNeo4j, "Persona")
                    media, margine_di_errore, neo4jIntervallo = Confidenza.intervalloDiConfidenza(neo4jListaTempo)
                    print(f"Tempo della prima esecuzione: {neo4jTempoIniziale:.2f} ms (Neo4j)")
                    print(f"Tempo medio delle successive 30 iterazioni: {neo4jTempoMedio:.2f} ms (Neo4j)")
                    print(f"Intervallo di confidenza al 95%: {neo4jIntervallo[0]:.2f} ms - {neo4jIntervallo[1]:.2f} ms (Neo4j)")
                    result, oracleTempoIniziale, oracleTempoMedio, oracleListaTempo = OracleQuery.oracleComplessita3(cursor)
                    media, margine_di_errore, oracleIntervallo = Confidenza.intervalloDiConfidenza(oracleListaTempo)
                    print(f"Tempo della prima esecuzione: {oracleTempoIniziale:.2f} ms (Oracle)")
                    print(f"Tempo medio delle successive 30 iterazioni: {oracleTempoMedio:.2f} ms (Oracle)")
                    print(f"Intervallo di confidenza al 95%: {oracleIntervallo[0]:.2f} ms - {oracleIntervallo[1]:.2f} ms (Oracle)")
                case "3":
                    neo4jTempoIniziale, neo4jTempoMedio, neo4jListaTempo = Neo4jQuery.neo4jComplessita4(driverNeo4j, "Persona")
                    media, margine_di_errore, neo4jIntervallo = Confidenza.intervalloDiConfidenza(neo4jListaTempo)
                    print(f"Tempo della prima esecuzione: {neo4jTempoIniziale:.2f} ms (Neo4j)")
                    print(f"Tempo medio delle successive 30 iterazioni: {neo4jTempoMedio:.2f} ms (Neo4j)")
                    print(f"Intervallo di confidenza al 95%: {neo4jIntervallo[0]:.2f} ms - {neo4jIntervallo[1]:.2f} ms (Neo4j)")
                    result, oracleTempoIniziale, oracleTempoMedio, oracleListaTempo = OracleQuery.oracleComplessita3(cursor)
                    media, margine_di_errore, oracleIntervallo = Confidenza.intervalloDiConfidenza(oracleListaTempo)
                    print(f"Tempo della prima esecuzione: {oracleTempoIniziale:.2f} ms (Oracle)")
                    print(f"Tempo medio delle successive 30 iterazioni: {oracleTempoMedio:.2f} ms (Oracle)")
                    print(f"Intervallo di confidenza al 95%: {oracleIntervallo[0]:.2f} ms - {oracleIntervallo[1]:.2f} ms (Oracle)")
                case "4":
                    menu0 = False
                    menu1 = False
                    Neo4jQuery.deleteNeo4j(driverNeo4j)
                    OracleQuery.deleteOracle(cursor)
                    cursor.callproc("manageBase.create_tables")
                case "5":
                    print("Arrivederci.")
                    menu0 = True
                    menu1 = True
                case _:
                    print("Opzione non valida.")