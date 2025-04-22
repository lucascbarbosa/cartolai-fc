"""Cartola APIs to fecth database."""
import argparse
import numpy as np
import pandas as pd
import requests
import traceback
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

pd.set_option('future.no_silent_downcasting', True)

# Parse args
parser = argparse.ArgumentParser()
parser.add_argument('--rodada', type=int, required=True, help='Rodada atual')
args = parser.parse_args()
RODADA = args.rodada

SCOUTS = [
    'G', 'A', 'FT', 'FD', 'FF', 'FS', 'FF', 'FS', 'SG', 'DS', 'DE', 'DP',
    'PS', 'PP', 'PC', 'I', 'GC', 'GS', 'FC', 'CA', 'CV'
]


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
    def _calcular_aproveitamento(resultados):
        """Calcula aproveitamento entre 0 e 1."""
        pontos = 0
        for r in resultados:
            if r == 'v':
                pontos += 3
            elif r == 'e':
                pontos += 1
        return pontos / 15
    try:
        partidas_data = requests.get(
            partidas_url.format(rodada=rodada_id)).json()
        partidas_df = pd.DataFrame(
            partidas_data['partidas'])[[
                'clube_casa_id', 'clube_visitante_id',
                'aproveitamento_mandante', 'aproveitamento_visitante',
                'clube_casa_posicao', 'clube_visitante_posicao',
            ]]
        casa_df = partidas_df.rename(
            columns={
            'clube_casa_id': 'clube_id',
            'clube_visitante_id': 'clube_adversario_id',
            'clube_casa_posicao': 'clube_posicao',
            'clube_visitante_posicao': 'clube_adversario_posicao',
            'aproveitamento_mandante': 'clube_aproveitamento',
            'aproveitamento_visitante': 'clube_adversario_aproveitamento'
            }
        )
        casa_df['is_casa'] = True
        visitante_df = partidas_df.rename(columns={
            'clube_visitante_id': 'clube_id',
            'clube_casa_id': 'clube_adversario_id',
            'clube_visitante_posicao': 'clube_posicao',
            'clube_casa_posicao': 'clube_adversario_posicao',
            'aproveitamento_visitante': 'clube_aproveitamento',
            'aproveitamento_mandante': 'clube_adversario_aproveitamento'
        })
        visitante_df['is_casa'] = False
        partidas_df = pd.concat([casa_df, visitante_df])
        partidas_df = partidas_df.drop_duplicates(subset='clube_id')
        partidas_df['rodada_id'] = rodada_id
        partidas_df['clube_aproveitamento'] = partidas_df[
            'clube_aproveitamento'].apply(_calcular_aproveitamento)
        partidas_df[
            'clube_adversario_aproveitamento'] = partidas_df[
            'clube_adversario_aproveitamento'].apply(_calcular_aproveitamento)
        return partidas_df
    except Exception as e:
        print(f"Erro na rodada {rodada_id}: {e}")
        return pd.DataFrame()


def fetch__pontuacao_atletas__rodada(atleta_id: int, status: str) -> pd.DataFrame:
    """Processa base de pontuacao de atletas por rodada."""
    def _fetch_scouts(scouts_dict: dict) -> dict:
        """Format scouts."""
        for scout in SCOUTS:
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
        rodadas_df['entrou_em_campo'] = rodadas_df['entrou_em_campo'].astype(int)

        # Rodadas completas
        if 'scouts' in rodadas_df:
            rodadas_df['scouts'] = rodadas_df['scouts'].apply(_fetch_scouts)
            scouts_df = pd.json_normalize(rodadas_df['scouts']).fillna(0)
            scouts_df.columns = ['scout_' + c for c in scouts_df.columns]
            rodadas_df[scouts_df.columns] = scouts_df

        # Expand preco
        if 'preco' in rodadas_df:
            preco_df = pd.json_normalize(rodadas_df['preco']).rename(
                columns={'num': 'preco', 'valorizacao': 'preco_var'}
            )[['preco', 'preco_var']]
            rodadas_df[preco_df.columns] = preco_df

        # Expand pontos
        if 'pontos' in rodadas_df:
            pontos = pd.json_normalize(rodadas_df['pontos']).rename(
                columns={'num': 'pontos', 'variacao': 'pontos_var'}
            )['pontos']
            rodadas_df['pontos'] = pontos

        rodadas_df = rodadas_df[
            [
                'rodada_id', 'mpv', 'status', 'entrou_em_campo',
                'pontos', 'preco', 'preco_var'
            ] +
            list(rodadas_df.filter(like='scout_', axis=1).columns)
        ]

        # Rodada atual
        scouts = {scout: np.nan for scout in SCOUTS}
        atual_df = pd.DataFrame([scouts])
        atual_df.columns = ['scout_' + c for c in atual_df.columns]
        atual_df['preco'] = gatomestre_data['preco']
        atual_df['preco_var'] = (
            gatomestre_data['preco'] - rodadas_df.iloc[-1, 5])
        atual_df['mpv'] = gatomestre_data['mpv']
        atual_df['rodada_id'] = len(rodadas_df) + 1
        atual_df['status'] = status
        atual_df['pontos'] = np.nan
        atual_df['entrou_em_campo'] = None

        # Concatenate
        pontuacoes_df = pd.concat([rodadas_df, atual_df])
        pontuacoes_df['atleta_id'] = atleta_id

        # Calcula variações de preco e pontuacao
        pontuacoes_df = pontuacoes_df.sort_values(
            by='rodada_id').reset_index(drop=True)
        pontuacoes_df['pontos_var'] = pontuacoes_df['pontos']\
            .diff().fillna(0.0)

        # Cast colunas
        pontuacoes_df[
            list(pontuacoes_df.filter(like='scout_', axis=1).columns) +
            ['preco_var', 'preco', 'pontos_var', 'pontos',
            'entrou_em_campo', 'mpv']
        ] = pontuacoes_df[
            list(pontuacoes_df.filter(like='scout_', axis=1).columns) +
            ['preco_var', 'preco', 'pontos_var', 'pontos',
            'entrou_em_campo', 'mpv']
        ].astype(float)

        # Crie médias cumulativas
        jogos_cumsum = pontuacoes_df['entrou_em_campo'].shift(1).cumsum()
        scout_cols = pontuacoes_df.filter(like='scout_', axis=1).columns
        scout_cumsum = pontuacoes_df[scout_cols].shift(1).cumsum()
        preco_cumsum = pontuacoes_df['preco'].shift(1).cumsum()
        pontos_cumsum = pontuacoes_df['pontos'].shift(1).cumsum()
        pontuacoes_df.loc[1:, scout_cols] = scout_cumsum.div(
            jogos_cumsum, axis=0).fillna(0.0)
        pontuacoes_df['preco_mean'] = (
            preco_cumsum / len(pontuacoes_df)
        ).fillna(0.0)
        pontuacoes_df['pontos_mean'] = (
            pontos_cumsum / jogos_cumsum.replace(0, np.nan)
        ).fillna(0.0)
        return pontuacoes_df

    except Exception as e:
        return pd.DataFrame()

# URLs e headers
mercado_url = "https://api.cartola.globo.com/atletas/mercado"
gatomestre_url = "https://api.gatomestre.globo.com/api/v2/atletas/{atleta_id}"
partidas_url = "https://api.cartola.globo.com/partidas/{rodada}"
auth_header = {
    "Authorization":
    "Bearer eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJXLUppTjhfZXdyWE9uVnJFN2lfOGpIY28yU1R4dEtHZF94aW01R2N4WS1ZIn0.eyJleHAiOjE3NDUzNTgwMTksImlhdCI6MTc0NTM1NDQxOSwiYXV0aF90aW1lIjoxNzQ1MzU0NDE5LCJqdGkiOiI5M2QyZmZlNS1iZmRkLTRmMTctYWMwMy0yYjhhYTJjMDZkNTIiLCJpc3MiOiJodHRwczovL2lkLmdsb2JvLmNvbS9hdXRoL3JlYWxtcy9nbG9iby5jb20iLCJzdWIiOiJmOjNjZGVhMWZiLTAwMmYtNDg5ZS1iOWMyLWQ1N2FiYTBhZTQ5NDowMjBjZWI5Yy04ZGE5LTQwN2QtOGMzZi05MzFiNDUyYmNmMTMiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJnYXRvbWVzdHJlLXdlYkBhcHBzLmdsb2JvaWQiLCJub25jZSI6IjY2NDdkYWE3LTk0MTYtNDNjNi05NDViLTFiYjA5OGE2ZjIzYyIsInNlc3Npb25fc3RhdGUiOiI1MDkyNzE5NS0xMjA0LTRhYWUtOTc4YS00MDVlMTYzOTJiODEiLCJhY3IiOiIxIiwic2NvcGUiOiJvcGVuaWQgZW1haWwgcHJvZmlsZS1taW4gZ2xiaWQgZnMtaWQgZ2xvYm9pZCIsInNpZCI6IjUwOTI3MTk1LTEyMDQtNGFhZS05NzhhLTQwNWUxNjM5MmI4MSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJnbGJpZCI6IjE5MzZkMDA0MDVlNmRiYjNjYWFmODRlMWZkZTM5ZTBmOTRjMzA1MjY3NjI3NjQ1NTA0OTM3NjM3ODcxMmQ2ZDQxNGY1MjZmNDg3NDQ5NTM2MzM3NjI2YjZiNmM0YzU1NGU2ZDZmNDI0ZTQ2NzU3YTc3NzAzODcwMzg3ODUyNjYyZDUwNDM1OTYzNTg0YzQyNWY1NjRjNzM2NDQ2NTQzODZlMzQ0MTYyNTM1MjczMzQ0ODcyNjEzNDY2NTE2MjY4MzI0NTVhNzk3ODcyNzczZDNkM2EzMDNhNmM3NTYzNjE3MzJlNjI2MTcyMmUzMjMwMzEzNDJlMzkiLCJmc19pZCI6IkwwUmdidkVQSTdjeHEtbUFPUm9IdElTYzdia2tsTFVObW9CTkZ1endwOHA4eFJmLVBDWWNYTEJfVkxzZEZUOG40QWJTUnM0SHJhNGZRYmgyRVp5eHJ3PT0iLCJlbWFpbCI6Imx1Y2FzLmJhcmJvc2EuMDg5OUBnbWFpbC5jb20iLCJnbG9ib19pZCI6IjAyMGNlYjljLThkYTktNDA3ZC04YzNmLTkzMWI0NTJiY2YxMyJ9.b0UfMOgSLU5YVs31hORoj4f4xy-nBvj2OJE7CM7NqsHgh6TUu8JYTrLl7CEyG-F3hU22NEDmQiz8tcUoPJyNZ6GT7lih2MPLN_EbKbPnWVSJeRvlehyTDM6nxtHSbDfQuB6QK0JoZJVE-465bK76b63nKn1HDlekCOPZeJ2Ph8u_VNR5sH3ZYRT8YQoVmP7KAxO4EW5R4YSCWqPH3bcuhPv-_j4ww82Hh2E7_dJpuz4hd_suVApFEc-fFQ6MQTFiVMzD6l6yF5KZaEk4rCj1Paom5VkVxHsTPceNhKoFWg9mNLr0-cSB2NIr04jIbxLjT5BtLOnnxWCuF_mvEm6NLg"
}

# Atletas description
atletas_df = fetch__atletas__description()

# Partidas rodada
with ThreadPoolExecutor(max_workers=4) as executor:
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
with ThreadPoolExecutor(max_workers=8) as executor:
    list__pontuacao_df = list(
        tqdm(executor.map(
            fetch__pontuacao_atletas__rodada,
            atletas_ids,
            atletas_status,
            ),
        total=len(atletas_ids), desc="Pontuação atletas"))
pontuacoes_df = pd.concat(list__pontuacao_df)

# Merge final
database = pontuacoes_df.\
    merge(atletas_df, on='atleta_id', how='left').\
    merge(partidas_df, on=['rodada_id', 'clube_id'], how='outer')

# Export
database.to_excel(f"dados__rodada_{RODADA}.xlsx", index=False)
