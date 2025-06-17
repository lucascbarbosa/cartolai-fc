"""Cartola APIs to fecth database."""
import numpy as np
import pandas as pd
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

pd.set_option('future.no_silent_downcasting', True)
RODADA = 12
SCOUTS_SCORE = {
    'G': 8.0,    # Gol
    'A': 5.0,    # Assistência
    'SG': 5.0,   # Jogo Sem Sofrer Gol
    'DP': 7.0,   # Defesa de Pênalti
    'FT': 3.0,   # Finalização na Trave
    'DS': 1.5,   # Desarme
    'DE': 1.0,   # Defesa
    'FD': 1.2,   # Finalização Defendida
    'FF': 0.8,   # Finalização pra Fora
    'FS': 0.5,   # Falta Sofrida
    'PS': 1.0,   # Pênalti Sofrido
    'PP': -4.0,  # Pênalti Perdido
    'GC': -3.0,  # Gol Contra
    'CV': -3.0,  # Cartão Vermelho
    'CA': -1.0,  # Cartão Amarelo
    'GS': -1.0,  # Gol Sofrido
    'PC': -1.0,  # Pênalti Cometido
    'FC': -0.3,  # Falta Cometida
    'I': -0.1,   # Impedimento
    'PI': -0.1,  # Passe Incompleto
}


#############
# Functions #
def fetch__atletas__description() -> pd.DataFrame:
    """Prepara a base de descrição dos atletas."""
    mercado_data = requests.get(mercado_url).json()
    clubes_df = pd.DataFrame(
        mercado_data['clubes']).T[['id', 'nome_fantasia']].rename(
            columns={'nome_fantasia': 'clube', 'id': 'clube_id'})
    posicoes_df = pd.DataFrame(
        mercado_data['posicoes']).T[['id', 'nome']].rename(
            columns={'nome': 'posicao', 'id': 'posicao_id'})
    status_df = pd.DataFrame(mercado_data['status']).T[['id', 'nome']]\
        .rename(columns={'nome': 'status', 'id': 'status_id'})
    atletas_df = pd.DataFrame(mercado_data['atletas'])[[
        'apelido', 'rodada_id', 'atleta_id', 'clube_id',
        'posicao_id', 'status_id',
    ]]
    atletas_df['atleta_id'] = atletas_df['atleta_id'].astype(int)
    atletas_df['clube_id'] = atletas_df['clube_id'].astype(int)
    atletas_df['posicao_id'] = atletas_df['posicao_id'].astype(int)
    atletas_df['status_id'] = atletas_df['status_id'].astype(int)
    atletas_df = atletas_df.merge(posicoes_df, on='posicao_id')
    atletas_df = atletas_df.merge(clubes_df, on='clube_id')
    atletas_df = atletas_df.merge(status_df, on='status_id')
    atletas_df = atletas_df[[
        'atleta_id', 'apelido', 'clube_id', 'clube', 'posicao', 'status'
    ]]
    return atletas_df


def fetch__partidas_clubes__rodada(rodada_id: int) -> pd.DataFrame:
    """Prepara a base de partidas de clubes por rodada."""
    try:
        partidas_data = requests.get(
            partidas_url.format(rodada=rodada_id)).json()
        partidas_df = pd.DataFrame(
            partidas_data['partidas'])[[
                'clube_casa_id', 'clube_visitante_id',
            ]]
        casa_df = partidas_df.rename(
            columns={
            'clube_casa_id': 'clube_id',
            'clube_visitante_id': 'clube_adversario_id',
            }
        )
        casa_df['is_casa'] = True
        visitante_df = partidas_df.rename(columns={
            'clube_visitante_id': 'clube_id',
            'clube_casa_id': 'clube_adversario_id',
        })
        visitante_df['is_casa'] = False
        partidas_df = pd.concat([casa_df, visitante_df])
        partidas_df = partidas_df.drop_duplicates(subset='clube_id')
        partidas_df['rodada_id'] = rodada_id
        return partidas_df

    except Exception as e:
        print(f"Erro na rodada {rodada_id}: {e}")
        return pd.DataFrame()


def fetch__pontuacao_atletas__rodada(atleta_id: int, atleta_status: str) -> pd.DataFrame:
    """Processa base de pontuacao de atletas por rodada."""
    def _fetch_scouts(scouts_dict: dict) -> dict:
        """Format scouts."""
        for scout in SCOUTS_SCORE.keys():
            if scout not in scouts_dict:
                scouts_dict[scout] = 0.0
        return scouts_dict
    try:
        response = requests.get(
            gatomestre_url.format(atleta_id=atleta_id),
            headers=auth_header
        )
        gatomestre_data = response.json()
        rodadas_df = pd.DataFrame(gatomestre_data['rodadas']).rename(
            columns={
                'rodada': 'rodada_id',
                'status_pre': 'status'
            }
        )
        rodadas_df['entrou_em_campo'] = rodadas_df[
            'entrou_em_campo'].astype(int)

        # Rodadas completas
        if 'scouts' in rodadas_df.columns:
            rodadas_df['scouts'] = rodadas_df['scouts'].apply(_fetch_scouts)
            scouts_df = pd.json_normalize(rodadas_df['scouts']).fillna(0)
            # Aplica a pontuação de cada scout
            for scout, score in SCOUTS_SCORE.items():
                if scout in scouts_df.columns:
                    scouts_df[scout] = scouts_df[scout] * score
            scouts_df.columns = ['scout_' + c for c in scouts_df.columns]
            rodadas_df[scouts_df.columns] = scouts_df

        # Expand preco
        if 'preco' in rodadas_df.columns:
            preco = pd.json_normalize(rodadas_df['preco']).get('open', np.nan)
            rodadas_df['preco'] = preco

        # Expand pontos
        if 'pontos' in rodadas_df.columns:
            pontos = pd.json_normalize(rodadas_df['pontos']).get('num', np.nan)
            rodadas_df['pontos'] = pontos

        rodadas_df = rodadas_df[
            [
                'rodada_id', 'mpv', 'status', 'entrou_em_campo',
                'pontos', 'preco',
            ] +
            list(rodadas_df.filter(like='scout_', axis=1).columns)
        ]

        # Rodada atual
        scouts = {scout: np.nan for scout in SCOUTS_SCORE.keys()}
        atual_df = pd.DataFrame([scouts])
        atual_df.columns = ['scout_' + c for c in atual_df.columns]
        atual_df['preco'] = gatomestre_data['preco']
        atual_df['mpv'] = gatomestre_data['mpv']
        atual_df['rodada_id'] = len(rodadas_df) + 1
        atual_df['status'] = atleta_status
        atual_df['pontos'] = np.nan
        atual_df['entrou_em_campo'] = None
        pontuacoes_df = pd.concat([rodadas_df, atual_df])
        pontuacoes_df['atleta_id'] = atleta_id

        # Sort by rodada_id
        pontuacoes_df = pontuacoes_df.sort_values(
            'rodada_id').reset_index(drop=True)

        # Fetch previous values
        pontuacoes_df['pontos_prev'] = pontuacoes_df['pontos'].shift(1)
        pontuacoes_df['preco_prev'] = pontuacoes_df['preco']
        pontuacoes_df['mpv_prev'] = pontuacoes_df['mpv']

        pontuacoes_df['mpv'] = pontuacoes_df['mpv'].shift(-1)
        pontuacoes_df['preco'] = pontuacoes_df['preco'].shift(-1)

        return pontuacoes_df

    except Exception as e:
        return pd.DataFrame()


def feature_engineering(database: pd.DataFrame):
    """Create more features for database."""
    # Ordena por rodada
    database = database.sort_values(
        ['atleta_id', 'rodada_id']
    ).reset_index(drop=True)

    # Calculates historic mean
    database[
        'pontos_mean'
    ] = database['pontos'].shift(1).rolling(window=3, min_periods=1).mean()

    database[
        'mpv_mean'
    ] = database['mpv'].rolling(window=3, min_periods=1).mean()

    database[
        'preco_mean'
    ] = database['preco'].rolling(window=3, min_periods=1).mean()

    scout_cols = [
        c for c in database.columns if c.startswith('scout_')
    ]
    for col in scout_cols:
        database[
            f'{col}_mean'
        ] = database[col].shift(1).rolling(window=3, min_periods=1).mean()

    # Fetch previous values
    database['pontos_prev'] = database['pontos'].shift(1)
    database['preco_prev'] = database['preco']
    database['mpv_prev'] = database['mpv']

    database['mpv'] = database['mpv'].shift(-1)
    database['preco'] = database['preco'].shift(-1)

    # Features from club performance
    groupby_cols = scout_cols + ['rodada_id', 'clube_id', 'pontos', 'mpv', 'preco']
    clube_performance = database[groupby_cols].groupby(
        ['rodada_id', 'clube_id']
    ).mean().reset_index()
    clube_performance = clube_performance.rename(
        columns={
            c: f'{c}_clube' for c in groupby_cols})
    database = database.merge(clube_performance, on=['rodada_id', 'clube_id'])
    return database

# URLs e headers
mercado_url = "https://api.cartola.globo.com/atletas/mercado"
gatomestre_url = "https://api.gatomestre.globo.com/api/v2/atletas/{atleta_id}"
partidas_url = "https://api.cartola.globo.com/partidas/{rodada}"
auth_header = {
    "Authorization":
    "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6Ijg5NWIwYmIwLTI4ODMtNDE3MC1hMDY2LTZkMDIwZjkzNGRlMyIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsiY2FydG9sYUBhcHBzLmdsb2JvaWQiXSwiYXpwIjoiY2FydG9sYUBhcHBzLmdsb2JvaWQiLCJlbWFpbCI6Imx1Y2FzLmJhcmJvc2EuMDg5OUBnbWFpbC5jb20iLCJleHAiOjE3NDk2NzI3NTksImZlZGVyYXRlZF9zaWQiOiIxYzhjMjRkZTMxYmQyMDM4NWYyYzhlNTk2OTU2Y2Q0Yzc3MTc4NzI0NTUzNzg0ZDUwNjU0OTU4NjcyZDRiMzIzMjM2NzI0ZjY3NzY1Nzc1MzI2NjRhNTczODUzNTQzODZhNTM1OTY3NGM2ZDM5MzU0YTMyNTM0NzZmNzY2YTczNDY3Mzc4Nzk2NTc5NjY3ODcxNTA3OTc5NDM2NTZkNTM3ODdhNTE2MTYzNTM2YjY5NzY0NDYyNTA2NTQ3N2E0NDRiNjk1NzM2Mzc2ZTQxM2QzZDNhMzAzYTZjNzU2MzYxNzMyZTYyNjE3MjJlMzIzMDMxMzQyZTM5IiwiZnNfaWQiOiJxeHJFU3hNUGVJWGctSzIyNnJPZ3ZXdTJmSlc4U1Q4alNZZ0xtOTVKMlNHb3Zqc0ZzeHlleWZ4cVB5eUNlbVN4elFhY1NraXZEYlBlR3pES2lXNjduQT09IiwiZ2xiaWQiOiIxYzhjMjRkZTMxYmQyMDM4NWYyYzhlNTk2OTU2Y2Q0Yzc3MTc4NzI0NTUzNzg0ZDUwNjU0OTU4NjcyZDRiMzIzMjM2NzI0ZjY3NzY1Nzc1MzI2NjRhNTczODUzNTQzODZhNTM1OTY3NGM2ZDM5MzU0YTMyNTM0NzZmNzY2YTczNDY3Mzc4Nzk2NTc5NjY3ODcxNTA3OTc5NDM2NTZkNTM3ODdhNTE2MTYzNTM2YjY5NzY0NDYyNTA2NTQ3N2E0NDRiNjk1NzM2Mzc2ZTQxM2QzZDNhMzAzYTZjNzU2MzYxNzMyZTYyNjE3MjJlMzIzMDMxMzQyZTM5IiwiZ2xvYm9faWQiOiIwMjBjZWI5Yy04ZGE5LTQwN2QtOGMzZi05MzFiNDUyYmNmMTMiLCJpYXQiOjE3NDk1ODMwMzEsImlzcyI6Imh0dHBzOi8vZ29pZGMuZ2xvYm8uY29tL2F1dGgvcmVhbG1zL2dsb2JvLmNvbSIsImp0aSI6IjAxNzhjM2E1LWE1MTEtNDRiNC05NDFiLTdiZmQ3YTJmZDlmMyIsInByZWZlcnJlZF91c2VybmFtZSI6Imx1Y2FzLmJhci4yMDE0LjkiLCJzY3AiOlsib3BlbmlkIiwicHJvZmlsZSJdLCJzZXNzaW9uX3N0YXRlIjoiZWI5YjNkMGItZTY1OC00NTdiLWExODAtZGU5OTdhMjIwYjlmIiwic2lkIjoiZWI5YjNkMGItZTY1OC00NTdiLWExODAtZGU5OTdhMjIwYjlmIiwic3ViIjoiMDIwY2ViOWMtOGRhOS00MDdkLThjM2YtOTMxYjQ1MmJjZjEzIiwidHlwIjoiQmVhcmVyIn0.lyVuBw8p1yZ61Hx4hNuvrvI5fn0L2-2nhAMt-VWh0aBwdxqgktjk3y8TJXznYb7iM0u0sRh-eHMbU3eglckQB48pz-0ER3qlnwwolKBEqRnGV4k9M0wdvSUD6SxFfRZL1vdjz-MKTg-_HtnEtTFhukwoGyEKUCde_rxml9X95ptcE0SAPLQrFOqwWwHvOkG7kKgnVn0ieWVOyqGlOUhTFgapp6nnGR-ElPE6sqqYIc-0ivEBtNiLcApHwwYU1wHP01r1kYmYnhnlOBkYyiX42DMTU6lG7YHmkeeLQMsmhvwPsoceBKnfSdiSUTyQ3kisLGp17h3SAC8Hok9WjiZMXg"
}

# Atletas description
atletas_df = fetch__atletas__description()

# Partidas rodada
with ThreadPoolExecutor(max_workers=8) as executor:
    list__partida_df = list(
        tqdm(executor.map(fetch__partidas_clubes__rodada, range(1, RODADA + 1)),
        total=RODADA, desc="Rodadas"))
partidas_df = pd.concat(list__partida_df)

# Pontuacoes rodada
sem_tecnico = atletas_df[
    atletas_df['posicao'] != 'Técnico'
    ]
atletas_ids = sem_tecnico['atleta_id']
atletas_status = sem_tecnico['status']
atletas_df = atletas_df.drop(['status'], axis=1)
with ThreadPoolExecutor(max_workers=16) as executor:
    list__pontuacao_df = list(
        tqdm(executor.map(
            fetch__pontuacao_atletas__rodada,
            atletas_ids,
            atletas_status,
            ),
        total=len(atletas_ids), desc="Pontuação atletas"))
pontuacoes_df = pd.concat(list__pontuacao_df)

# Merge databases
database = pontuacoes_df.\
    merge(atletas_df, on='atleta_id', how='left').\
    merge(partidas_df, on=['rodada_id', 'clube_id'], how='outer')

# Feature engineering
database = database.pipe(feature_engineering)

# Export
database.to_excel(f"dados__rodada_{RODADA}.xlsx", index=False)
