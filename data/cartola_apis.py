"""Cartola APIs to fecth database."""
import numpy as np
import pandas as pd
import requests

RODADA = 1

########
# Urls #
mercado_url = "https://api.cartola.globo.com/atletas/mercado"
partidas_url = f"https://api.cartola.globo.com/partidas/{RODADA}"
esquemas_url = "https://api.cartolafc.globo.com/esquemas"

##############
# Dataframes #

# Mercado
mercado_data = requests.get(mercado_url).json()

clubes_df = pd.DataFrame(mercado_data['clubes']).T[['id', 'nome_fantasia']]
posicoes_df = pd.DataFrame(mercado_data['posicoes']).T[['id', 'nome']]
status_df = pd.DataFrame(mercado_data['status']).T[['id', 'nome']]
atletas_df = pd.DataFrame(mercado_data['atletas'])

# Partidas
partidas_data = requests.get(partidas_url).json()

partidas_df = pd.DataFrame(partidas_data)