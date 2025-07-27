import math

def intervalloDiConfidenza(lista_numeri):
    n = len(lista_numeri)

    # Calcola la media
    media = sum(lista_numeri) / n

    # Calcola la deviazione standard campionaria
    varianza = sum((x - media) ** 2 for x in lista_numeri) / (n - 1)
    deviazione_standard = math.sqrt(varianza)

    # Calcola l'errore standard della media
    errore_standard = deviazione_standard / math.sqrt(n)

    # Valore critico t (da una tabella per livello di confidenza al 95%)
    # Nota: Questi valori sono approssimati per df=1, ..., 30
    t_valori_critici = {
        1: 12.71, 2: 4.30, 3: 3.18, 4: 2.78, 5: 2.57,
        6: 2.45, 7: 2.36, 8: 2.31, 9: 2.26, 10: 2.23,
        11: 2.20, 12: 2.18, 13: 2.16, 14: 2.14, 15: 2.13,
        16: 2.12, 17: 2.11, 18: 2.10, 19: 2.09, 20: 2.09,
        21: 2.08, 22: 2.07, 23: 2.07, 24: 2.06, 25: 2.06,
        26: 2.06, 27: 2.05, 28: 2.05, 29: 2.05, 30: 2.04
    }
    df = n - 1
    t_critico = t_valori_critici.get(df, 2.00)  # Default a 2.00 per df > 30

    # Calcola il margine di errore
    margine_errore = t_critico * errore_standard

    # Calcola l'intervallo di confidenza
    limite_inferiore = media - margine_errore
    limite_superiore = media + margine_errore

    return media, margine_errore, (limite_inferiore, limite_superiore)