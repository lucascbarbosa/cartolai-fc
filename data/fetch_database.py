"""Cartola APIs to fecth database."""
import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

RODADA_ATUAL = 2

########
# Urls #

mercado_url = "https://api.cartola.globo.com/atletas/mercado"
gatomestre_url = "https://api.cartola.globo.com/auth/gatomestre/atletas"
partidas_url = "https://api.cartola.globo.com/partidas/{rodada}"
pontuacao_url = "https://api.cartola.globo.com/auth/mercado/atleta/{atleta_id}/pontuacao"
auth_header = {
 "Authorization": "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6Ijg5NWIwYmIwLTI4ODMtNDE3MC1hMDY2LTZkMDIwZjkzNGRlMyIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsiY2FydG9sYUBhcHBzLmdsb2JvaWQiXSwiYXpwIjoiY2FydG9sYUBhcHBzLmdsb2JvaWQiLCJlbWFpbCI6Imx1Y2FzLmJhcmJvc2EuMDg5OUBnbWFpbC5jb20iLCJleHAiOjE3NDQwNjYxNDYsImZlZGVyYXRlZF9zaWQiOiIxZDljZTJhN2Y4OTkwZWU4MGNmNDY3Yzc5MmQ0YzU1YzI1NDZmNmM1ZjM4NDUzMTMzNWE2OTY1NzQzMDZjNzAzNzRhNmUzODY5NTk0Njc4Mzg1YTZkMzY2NDMzNmE2NDRmNzY0NTY5NzU2NDM0NGU2OTU1MzI3MDc1MzkzMjRjMzM0ODMzNzE3ODU5Nzc3Mjc2NmM0MzZlNTI2NTZiNzA1NjM5NTk2ZjZlNGUzOTczNjg2MzU3NzQ0NzZhNmY1NzMzNjgzMjMwNjQ3OTY3M2QzZDNhMzAzYTZjNzU2MzYxNzMyZTYyNjE3MjJlMzIzMDMxMzQyZTM5IiwiZnNfaWQiOiJUb2xfOEUxM1ppZXQwbHA3Sm44aVlGeDhabTZkM2pkT3ZFaXVkNE5pVTJwdTkyTDNIM3F4WXdydmxDblJla3BWOVlvbk45c2hjV3RHam9XM2gyMGR5Zz09IiwiZ2xiaWQiOiIxZDljZTJhN2Y4OTkwZWU4MGNmNDY3Yzc5MmQ0YzU1YzI1NDZmNmM1ZjM4NDUzMTMzNWE2OTY1NzQzMDZjNzAzNzRhNmUzODY5NTk0Njc4Mzg1YTZkMzY2NDMzNmE2NDRmNzY0NTY5NzU2NDM0NGU2OTU1MzI3MDc1MzkzMjRjMzM0ODMzNzE3ODU5Nzc3Mjc2NmM0MzZlNTI2NTZiNzA1NjM5NTk2ZjZlNGUzOTczNjg2MzU3NzQ0NzZhNmY1NzMzNjgzMjMwNjQ3OTY3M2QzZDNhMzAzYTZjNzU2MzYxNzMyZTYyNjE3MjJlMzIzMDMxMzQyZTM5IiwiZ2xvYm9faWQiOiIwMjBjZWI5Yy04ZGE5LTQwN2QtOGMzZi05MzFiNDUyYmNmMTMiLCJpYXQiOjE3NDQwNTg2NDEsImlzcyI6Imh0dHBzOi8vZ29pZGMuZ2xvYm8uY29tL2F1dGgvcmVhbG1zL2dsb2JvLmNvbSIsImp0aSI6Ijg3OWM2M2E3LTI2Y2QtNDRjMC05OTg4LTNlYTE0YTg0YmU1NSIsInByZWZlcnJlZF91c2VybmFtZSI6Imx1Y2FzLmJhci4yMDE0LjkiLCJzY3AiOlsib3BlbmlkIiwicHJvZmlsZSJdLCJzZXNzaW9uX3N0YXRlIjoiNDJlMGE4YzAtMDlhZi00ZGE0LTllYzQtOTAyZGY1YzIxMWRmIiwic2lkIjoiNDJlMGE4YzAtMDlhZi00ZGE0LTllYzQtOTAyZGY1YzIxMWRmIiwic3ViIjoiMDIwY2ViOWMtOGRhOS00MDdkLThjM2YtOTMxYjQ1MmJjZjEzIiwidHlwIjoiQmVhcmVyIn0.S3NR4m_Y2j13UGp_wzufpkCPvm5xSi6lzmk0C-ZuXsN0RxVEfgfZrMK6mcfzqJQbbRjMGfW141HiZfIdD0y6nQ0EBDDscu2IJW5HX-GrBLG_jjaaqGCOf-t0wItHNWCR62wS_C1qH3cJtTOoY7dCL79BO5CnD5xH10IDoz4PD7UqdlWzmAH6zjEjwC54icZdZh3wi38OeppaBTg8mLYmHtGPJZJ3n7Q4JX52qAmhX8ifEdII5b-UtGfFsKC-ASG6KRocU3EAyru6OsBhImj4wWZigkypuCuZuds0tdz8jxnxRzAC1J3l1i-82bsTS6cUi1LXgfOF5Px7nKLTqb8W7w" 
}

# Requests estáticos
mercado_data = requests.get(mercado_url).json()
gatomestre_data = requests.get(gatomestre_url, headers=auth_header).json()

list__rodada_df = []

for rodada_id in tqdm(range(1, RODADA_ATUAL + 1), desc="Rodadas"):
    # Request dinâmico
    partidas_data = requests.get(partidas_url.format(rodada=rodada_id)).json()

    ##############
    # Dataframes #
    # Clubes
    clubes_df = pd.DataFrame(mercado_data['clubes']).T[['id', 'nome_fantasia']]
    clubes_df = clubes_df.rename(columns={'nome_fantasia': 'clube'})

    # Posicoes
    posicoes_df = pd.DataFrame(mercado_data['posicoes']).T[['id', 'nome']]
    posicoes_df = posicoes_df.rename(columns={'nome': 'posicao'})

    # Status
    status_df = pd.DataFrame(mercado_data['status']).T[['id', 'nome']]
    status_df = status_df.rename(columns={'nome': 'status'})

    # Atletas
    atletas_df = pd.DataFrame(mercado_data['atletas'])

    # Gato mestre
    gatomestre_df = pd.DataFrame(gatomestre_data).T

    # Partidas
    partidas_df = pd.DataFrame(partidas_data['partidas'])
    partidas_df = partidas_df[['clube_casa_id', 'clube_visitante_id']]

    # Merge atletas_df and gatomestre_df
    rodada_df = gatomestre_df.reset_index()
    rodada_df = rodada_df.rename(columns={"index": "atleta_id"})
    rodada_df['atleta_id'] = rodada_df['atleta_id'].astype(int)
    rodada_df = rodada_df.merge(atletas_df, on='atleta_id')

    # Fetch posicao, status and clube
    rodada_df = rodada_df.merge(
        status_df, left_on='status_id', right_on='id')
    rodada_df = rodada_df.merge(
        posicoes_df, left_on='posicao_id', right_on='id')
    rodada_df = rodada_df.merge(
        clubes_df, left_on='clube_id', right_on='id')

    # Set is_casa
    rodada_df['clube_id'] = rodada_df['clube_id'].astype(int)
    rodada_df['is_casa'] = rodada_df['clube_id'].isin(partidas_df['clube_casa_id'])

    # Filter columns
    rodada_df = rodada_df[[
        'apelido', 'atleta_id', 'entrou_em_campo', 'status', 'posicao',
        'clube', 'minimos_para_valorizar', 'scout', 'jogos_num', 'is_casa'
    ]]

    # Add rodada
    rodada_df['rodada_id'] = rodada_id

    list__rodada_df.append(rodada_df)

rodadas_df = pd.concat(list__rodada_df)

# Pontucao atletas
list__pontuacao_df = []
for atleta_id in tqdm(
    rodadas_df['atleta_id'].unique(), desc="Pontuação atletas"):
    pontuacao_data = requests.get(
        pontuacao_url.format(atleta_id=atleta_id), headers=auth_header).json()
    pontuacao_df = pd.DataFrame(pontuacao_data).dropna()
    list__pontuacao_df.append(pontuacao_df)

pontuacoes_df = pd.concat(list__pontuacao_df)

# Merge rodadas e pontuacoes
database = rodadas_df.merge(
    pontuacoes_df,
    on=['atleta_id', 'rodada_id'],
)

# Exportar
database.to_parquet("data/dados_bronze.parquet")
