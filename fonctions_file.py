import requests
from bs4 import BeautifulSoup
import time
from tqdm import tqdm
import pandas as pd
import numpy as np
import itertools
import pandas as pd
from pandasql import sqldf
import random



def get_scores_for_url(list_scores, url):
    # Définir les headers, y compris un User-Agent
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    # Récupération du code HTML de la page souhaitée
    response = requests.get(url, headers=headers)

    # Vérifier le statut de la réponse
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
    else:
        print(f"Erreur: {response.status_code}")
        
    for td in soup.findAll('td', class_='zentriert hauptlink no-border-rechts no-border-links spieltagsansicht-ergebnis'):
        # Chercher l'élément 'span' avec la classe 'matchresult finished'
        span_element = td.find('span', class_='matchresult finished')

        # Vérifier que l'élément existe avant d'essayer de récupérer son texte
        if span_element is not None:
            list_scores.append(span_element.get_text(strip=True))
    
    return list_scores

def get_list_url():
    list_league_names = ['premier-league', 'laliga', 'serie-a', 'bundesliga', 'ligue-1']
    list_league_short_names = ['GB1', 'ES1', 'IT1', 'L1', 'FR1']
    
    list_url = []
    for i in range(5):
        for season in range(1990, 2024) :
            for day in range(1, 35):
                list_url.append(f'https://www.transfermarkt.fr/{list_league_names[i]}/spieltag/wettbewerb/{list_league_short_names[i]}/spieltag/{day}/saison_id/{season}')
                
    return list_url


def generate_multi_competition_calendar(x):
    df_multi_competition_calendar = generate_competition_calendar()

    #On génère une première compétition avant à laquelle on va venir ajouter les suivantes
    q=f"""
    SELECT
        1 AS id_season,
        ROW_NUMBER() OVER(ORDER BY match) AS id_match,
        match,
        substr(match, 1, instr(match, ':') - 1) AS home_team,
        substr(match, instr(match, ':') + 1) AS away_team

    FROM df_multi_competition_calendar
    """
    df_multi_competition_calendar = sqldf(q)


    for i in tqdm(range(2, x+1)) :
        df_new_competition = generate_competition_calendar()

        q=f"""

        WITH competition_to_add AS (
                SELECT
                    {i} AS id_season,
                    ROW_NUMBER() OVER(ORDER BY match) AS id_match,
                    match,
                    substr(match, 1, instr(match, ':') - 1) AS home_team,
                    substr(match, instr(match, ':') + 1) AS away_team

                FROM df_new_competition
                )

        SELECT *
        FROM df_multi_competition_calendar
        UNION 
        SELECT *
        FROM competition_to_add
        """
        df_multi_competition_calendar = sqldf(q)
    
    return df_multi_competition_calendar

def generate_36_dif_matches() :
    n_unique_matches = 0

    while n_unique_matches != 36 :

        match_calendar =[]

        list_home_team = [f"team_{i}" for i in range(1, 37)]
        list_ext_team = [f"team_{i}" for i in range(1, 37)]

        # Boucle pour générer les matchs
        while len(list_home_team) > 1:

            home_team = random.choice(list_home_team)
            ext_team = random.choice(list_ext_team)

            # Vérifier si le match est déjà programmé ou si une équipe joue contre elle-même
            while (f'{home_team}:{ext_team}' in match_calendar or 
                   f'{ext_team}:{home_team}' in match_calendar or 
                   home_team == ext_team):

                home_team = random.choice(list_home_team)
                ext_team = random.choice(list_ext_team)
            # Ajouter le match au calendrier
            match_calendar.append(f'{home_team}:{ext_team}')

            # Supprimer les équipes sélectionnées
            list_home_team.remove(f'{home_team}')
            list_ext_team.remove(f'{ext_team}')

        # Créer le dernier match manuellement
        home_team = random.choice(list_home_team)
        ext_team = random.choice(list_ext_team)

        match_calendar.append(f'{home_team}:{ext_team}')

        # Supprimer les dernières équipes
        list_home_team.remove(f'{home_team}')
        list_ext_team.remove(f'{ext_team}')

        #On compte le nombre de match unique afin de s'assurer d'avoir bien 36 affiches différentes
        df_36_dif_matches = pd.DataFrame(match_calendar, columns=['match'])
        n_unique_matches = df_36_dif_matches['match'].nunique()


    return df_36_dif_matches


def generate_competition_calendar():
    
    n_unique_matches = 0
    while n_unique_matches!= 144 :
        df_part1 = generate_36_dif_matches()
        df_part2 = generate_36_dif_matches()
        df_part3 = generate_36_dif_matches()
        df_part4 = generate_36_dif_matches()

        df_competition_calendar = pd.concat([df_part1, df_part2, df_part3, df_part4], axis=0, ignore_index=True)
        n_unique_matches = df_competition_calendar['match'].nunique()
    
    return df_competition_calendar