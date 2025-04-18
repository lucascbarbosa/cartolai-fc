"""Cartola APIs to fecth database."""
import argparse
import pandas as pd
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor


# Parse args
parser = argparse.ArgumentParser()
parser.add_argument('--rodada', type=int, required=True, help='Rodada atual')
args = parser.parse_args()
RODADA = args.rodada


#############
# Functiuns #
def _fetch_rodada(rodada_id):
    """Processa base de rodada."""
    try:
        partidas_data = requests.get(
            partidas_url.format(rodada=rodada_id)).json()
        partidas_df = pd.DataFrame(
            partidas_data['partidas'])[['clube_casa_id', 'clube_visitante_id']]

        rodada_df = gatomestre_df.reset_index().rename(
            columns={"index": "atleta_id"})
        rodada_df['atleta_id'] = rodada_df['atleta_id'].astype(int)
        rodada_df = rodada_df.merge(atletas_df, on='atleta_id')
        rodada_df = rodada_df.merge(
            status_df, left_on='status_id', right_on='id')
        rodada_df = rodada_df.merge(
            posicoes_df, left_on='posicao_id', right_on='id')
        rodada_df = rodada_df.merge(
            clubes_df, left_on='clube_id', right_on='id')
        rodada_df['clube_id'] = rodada_df['clube_id'].astype(int)
        rodada_df['is_casa'] = rodada_df['clube_id'].isin(
            partidas_df['clube_casa_id'])

        rodada_df = rodada_df[[
            'apelido', 'atleta_id', 'entrou_em_campo', 'status', 'posicao',
            'clube', 'minimos_para_valorizar', 'scout', 'jogos_num', 'is_casa'
        ]]
        rodada_df['rodada_id'] = rodada_id

        return rodada_df
    except Exception as e:
        print(f"Erro na rodada {rodada_id}: {e}")
        return pd.DataFrame()


# Função para buscar pontuação
def _fetch_pontuacao(atleta_id):
    """Processa base de pontuações."""
    try:
        pontuacao_data = requests.get(
            pontuacao_url.format(
                atleta_id=atleta_id), headers=auth_header).json()
        pontuacao_df = pd.DataFrame(pontuacao_data).dropna()
        return pontuacao_df[['atleta_id', 'rodada_id', 'media', 'preco']]
    except Exception as e:
        print(f"Erro pontuação atleta {atleta_id}: {e}")
        return pd.DataFrame()


# URLs e headers
mercado_url = "https://api.cartola.globo.com/atletas/mercado"
gatomestre_url = "https://api.cartola.globo.com/auth/gatomestre/atletas"
partidas_url = "https://api.cartola.globo.com/partidas/{rodada}"
pontuacao_url = "https://api.cartola.globo.com/auth/mercado/atleta/{atleta_id}/pontuacao"
auth_header = {
 "Authorization": "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6Ijg5NWIwYmIwLTI4ODMtNDE3MC1hMDY2LTZkMDIwZjkzNGRlMyIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsiY2FydG9sYUBhcHBzLmdsb2JvaWQiXSwiYXpwIjoiY2FydG9sYUBhcHBzLmdsb2JvaWQiLCJlbWFpbCI6Imx1Y2FzLmJhcmJvc2EuMDg5OUBnbWFpbC5jb20iLCJleHAiOjE3NDQyMzI2OTgsImZlZGVyYXRlZF9zaWQiOiIxZDljZTJhN2Y4OTkwZWU4MGNmNDY3Yzc5MmQ0YzU1YzI1NDZmNmM1ZjM4NDUzMTMzNWE2OTY1NzQzMDZjNzAzNzRhNmUzODY5NTk0Njc4Mzg1YTZkMzY2NDMzNmE2NDRmNzY0NTY5NzU2NDM0NGU2OTU1MzI3MDc1MzkzMjRjMzM0ODMzNzE3ODU5Nzc3Mjc2NmM0MzZlNTI2NTZiNzA1NjM5NTk2ZjZlNGUzOTczNjg2MzU3NzQ0NzZhNmY1NzMzNjgzMjMwNjQ3OTY3M2QzZDNhMzAzYTZjNzU2MzYxNzMyZTYyNjE3MjJlMzIzMDMxMzQyZTM5IiwiZnNfaWQiOiJUb2xfOEUxM1ppZXQwbHA3Sm44aVlGeDhabTZkM2pkT3ZFaXVkNE5pVTJwdTkyTDNIM3F4WXdydmxDblJla3BWOVlvbk45c2hjV3RHam9XM2gyMGR5Zz09IiwiZ2xiaWQiOiIxZDljZTJhN2Y4OTkwZWU4MGNmNDY3Yzc5MmQ0YzU1YzI1NDZmNmM1ZjM4NDUzMTMzNWE2OTY1NzQzMDZjNzAzNzRhNmUzODY5NTk0Njc4Mzg1YTZkMzY2NDMzNmE2NDRmNzY0NTY5NzU2NDM0NGU2OTU1MzI3MDc1MzkzMjRjMzM0ODMzNzE3ODU5Nzc3Mjc2NmM0MzZlNTI2NTZiNzA1NjM5NTk2ZjZlNGUzOTczNjg2MzU3NzQ0NzZhNmY1NzMzNjgzMjMwNjQ3OTY3M2QzZDNhMzAzYTZjNzU2MzYxNzMyZTYyNjE3MjJlMzIzMDMxMzQyZTM5IiwiZ2xvYm9faWQiOiIwMjBjZWI5Yy04ZGE5LTQwN2QtOGMzZi05MzFiNDUyYmNmMTMiLCJpYXQiOjE3NDQwNTg2NDEsImlzcyI6Imh0dHBzOi8vZ29pZGMuZ2xvYm8uY29tL2F1dGgvcmVhbG1zL2dsb2JvLmNvbSIsImp0aSI6IjBjOTBhOTBjLWUwMWEtNDJlZi1hODY0LTQ1ZGRjNTY1N2RhMCIsInByZWZlcnJlZF91c2VybmFtZSI6Imx1Y2FzLmJhci4yMDE0LjkiLCJzY3AiOlsib3BlbmlkIiwicHJvZmlsZSJdLCJzZXNzaW9uX3N0YXRlIjoiNDJlMGE4YzAtMDlhZi00ZGE0LTllYzQtOTAyZGY1YzIxMWRmIiwic2lkIjoiNDJlMGE4YzAtMDlhZi00ZGE0LTllYzQtOTAyZGY1YzIxMWRmIiwic3ViIjoiMDIwY2ViOWMtOGRhOS00MDdkLThjM2YtOTMxYjQ1MmJjZjEzIiwidHlwIjoiQmVhcmVyIn0.p02dtESQuAL0OH101YPVFZn__Z__BlNACRq4R12rp-EZPZjt1yEELFzUhwAoe0UGq5VrggYlfKqG-AXHuTroH16llLNT76bEVTOtkdSIzJcAlx2VzoGXrt569g0lrN_lao9ip1_Su7eQuL3xr93msuIkDOiXofsN1mQ16kZfrB9nvQe70hX4KGqPSEyijxpSswlJ9g55k4vTsKNCLqJJadUs3hF0gGvEEc08Km3v9qZyLPDXNJSz7TL7mxZl44-vJ-fhCSXMwx31hcKB-lrKUMn0o51H4sLmJN-pWgDGdDw2D5NcPgqsxnhD5gllp5lHa9uXRX2YHssNvm182nrYbg" 
}


# Requisições iniciais
mercado_data = requests.get(mercado_url).json()
gatomestre_data = requests.get(gatomestre_url, headers=auth_header).json()

# Dataframes fixos
clubes_df = pd.DataFrame(
    mercado_data['clubes']).T[['id', 'nome_fantasia']].rename(
        columns={'nome_fantasia': 'clube'})
posicoes_df = pd.DataFrame(
    mercado_data['posicoes']).T[['id', 'nome']].rename(
        columns={'nome': 'posicao'})
status_df = pd.DataFrame(
mercado_data['status']).T[['id', 'nome']].rename(columns={'nome': 'status'})
atletas_df = pd.DataFrame(mercado_data['atletas'])
gatomestre_df = pd.DataFrame(gatomestre_data).T

with ThreadPoolExecutor(max_workers=4) as executor:
    list__rodada_df = list(
        tqdm(executor.map(_fetch_rodada, range(1, RODADA + 1)),
        total=RODADA, desc="Rodadas"))

rodadas_df = pd.concat(list__rodada_df)

atletas_ids = rodadas_df['atleta_id'].unique()
with ThreadPoolExecutor(max_workers=16) as executor:
    list__pontuacao_df = list(
        tqdm(executor.map(_fetch_pontuacao, atletas_ids),
        total=len(atletas_ids), desc="Pontuação atletas"))

pontuacoes_df = pd.concat(list__pontuacao_df)

# Merge final
database = rodadas_df.merge(pontuacoes_df, on=['atleta_id', 'rodada_id'])

# Expand scout
scout_df = pd.json_normalize(database['scout']).fillna(0)
scout_df.columns = ['scout_' + c for c in scout_df.columns]
database[scout_df.columns] = scout_df

# Expand minimos
minimos_df = pd.json_normalize(database['minimos_para_valorizar'])
minimos_df.columns = ['minval_' + c for c in minimos_df.columns]
database[minimos_df.columns] = minimos_df

# Crie score unico
database['score'] = (
    2 * database['media'] -
    (
        (
            database['minval_1'] / 3 +
            database['minval_2'] / 6 +
            database['minval_3'] / 9)
    )
) / (0.5 * database['preco'])

# Drop columns
database = database.drop(
    ['minimos_para_valorizar', 'scout'],
    axis=1
)

# Export
database.to_excel(f"dados__rodada_{RODADA}.xlsx", index=False)
