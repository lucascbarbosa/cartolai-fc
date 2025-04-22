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
# Functions #
def fetch__atletas__description():
    """Prepara a base de descrição dos atletas."""
    mercado_data = requests.get(mercado_url).json()
    clubes_df = pd.DataFrame(
        mercado_data['clubes']).T[['id', 'nome_fantasia']].rename(
            columns={'nome_fantasia': 'clube', 'id': 'clube_id'})
    posicoes_df = pd.DataFrame(
        mercado_data['posicoes']).T[['id', 'nome']].rename(
            columns={'nome': 'posicao', 'id': 'posicao_id'})
    atletas_df = pd.DataFrame(mercado_data['atletas'])[[
        'atleta_id', 'clube_id', 'posicao_id', 'apelido'
    ]]
    atletas_df['atleta_id'] = atletas_df['atleta_id'].astype(int)
    atletas_df['clube_id'] = atletas_df['clube_id'].astype(int)
    atletas_df['posicao_id'] = atletas_df['posicao_id'].astype(int)
    atletas_df = atletas_df.merge(posicoes_df, on='posicao_id')
    atletas_df = atletas_df.merge(clubes_df, on='clube_id')
    atletas_df = atletas_df[[
        'atleta_id', 'apelido', 'clube_id', 'clube', 'posicao']]
    return atletas_df


def fetch__partidas_clubes__rodada(rodada_id):
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


def fetch__pontuacao_atletas__rodada(atleta_id):
    """Processa base de pontuacao de atletas por rodada."""
    response = requests.get(
        gatomestre_url.format(atleta_id=atleta_id),
        headers=auth_header)
    try:
        gatomestre_data = response.json()
        gatomestre_df = pd.DataFrame(
            gatomestre_data['rodadas']).rename(
                columns={
                    'rodada': 'rodada_id',
                    'status_pre': 'status'
                }
            )
        gatomestre_df['atleta_id'] = atleta_id
        if 'scouts' in gatomestre_df:
            scout_df = pd.json_normalize(gatomestre_df['scouts']).fillna(0)
            scout_df.columns = ['scout_' + c for c in scout_df.columns]
            gatomestre_df[scout_df.columns] = scout_df
            gatomestre_df['atleta_id'] = atleta_id

        # Expand preco
        if 'preco' in gatomestre_df:
            preco_df = pd.json_normalize(gatomestre_df['preco']).rename(
                columns={'num': 'preco', 'valorizacao': 'preco_var'}
            )[['preco', 'preco_var']]
            gatomestre_df[preco_df.columns] = preco_df

        # Expand pontos
        if 'pontos' in gatomestre_df:
            pontos_df = pd.json_normalize(gatomestre_df['pontos']).rename(
                columns={'num': 'pontos', 'variacao': 'pontos_var'}
            )[['pontos', 'pontos_var']]
            gatomestre_df[pontos_df.columns] = pontos_df

        return gatomestre_df[
            ['atleta_id', 'rodada_id', 'mpv', 'status', 'entrou_em_campo'] +
            list(gatomestre_df.filter(like='scout_', axis=1).columns) +
            list(gatomestre_df.filter(like='preco', axis=1).columns) +
            list(gatomestre_df.filter(like='pontos', axis=1).columns)
        ]

    except:
        return pd.DataFrame()

# URLs e headers
mercado_url = "https://api.cartola.globo.com/atletas/mercado"
gatomestre_url = "https://api.gatomestre.globo.com/api/v2/atletas/{atleta_id}"
partidas_url = "https://api.cartola.globo.com/partidas/{rodada}"
auth_header = {
    "Authorization":
    "Bearer eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJXLUppTjhfZXdyWE9uVnJFN2lfOGpIY28yU1R4dEtHZF94aW01R2N4WS1ZIn0.eyJleHAiOjE3NDUzMjg3NzMsImlhdCI6MTc0NTMyNTE3MywiYXV0aF90aW1lIjoxNzQ1MzI1MTczLCJqdGkiOiJjOTAxYTdiOC03ZjA3LTRmZDAtOWI5OS04YzE3ZDExMDgwZDEiLCJpc3MiOiJodHRwczovL2lkLmdsb2JvLmNvbS9hdXRoL3JlYWxtcy9nbG9iby5jb20iLCJzdWIiOiJmOjNjZGVhMWZiLTAwMmYtNDg5ZS1iOWMyLWQ1N2FiYTBhZTQ5NDowMjBjZWI5Yy04ZGE5LTQwN2QtOGMzZi05MzFiNDUyYmNmMTMiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJnYXRvbWVzdHJlLXdlYkBhcHBzLmdsb2JvaWQiLCJub25jZSI6IjZmMmJhNDdiLTFmMDItNDVhMS04ODBjLTQzNmVmYWM0OGVjMiIsInNlc3Npb25fc3RhdGUiOiI1MDkyNzE5NS0xMjA0LTRhYWUtOTc4YS00MDVlMTYzOTJiODEiLCJhY3IiOiIxIiwic2NvcGUiOiJvcGVuaWQgZW1haWwgcHJvZmlsZS1taW4gZ2xiaWQgZnMtaWQgZ2xvYm9pZCIsInNpZCI6IjUwOTI3MTk1LTEyMDQtNGFhZS05NzhhLTQwNWUxNjM5MmI4MSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJnbGJpZCI6IjE5MzZkMDA0MDVlNmRiYjNjYWFmODRlMWZkZTM5ZTBmOTRjMzA1MjY3NjI3NjQ1NTA0OTM3NjM3ODcxMmQ2ZDQxNGY1MjZmNDg3NDQ5NTM2MzM3NjI2YjZiNmM0YzU1NGU2ZDZmNDI0ZTQ2NzU3YTc3NzAzODcwMzg3ODUyNjYyZDUwNDM1OTYzNTg0YzQyNWY1NjRjNzM2NDQ2NTQzODZlMzQ0MTYyNTM1MjczMzQ0ODcyNjEzNDY2NTE2MjY4MzI0NTVhNzk3ODcyNzczZDNkM2EzMDNhNmM3NTYzNjE3MzJlNjI2MTcyMmUzMjMwMzEzNDJlMzkiLCJmc19pZCI6IkwwUmdidkVQSTdjeHEtbUFPUm9IdElTYzdia2tsTFVObW9CTkZ1endwOHA4eFJmLVBDWWNYTEJfVkxzZEZUOG40QWJTUnM0SHJhNGZRYmgyRVp5eHJ3PT0iLCJlbWFpbCI6Imx1Y2FzLmJhcmJvc2EuMDg5OUBnbWFpbC5jb20iLCJnbG9ib19pZCI6IjAyMGNlYjljLThkYTktNDA3ZC04YzNmLTkzMWI0NTJiY2YxMyJ9.N_xhXYtGOD7EfnarzVN8SJUDZIGos2Lwx1nTa1ZUAlr8SmS0fJesoYFR4uD58XUe4fwMsYcT4WF08rjs2PG-BcWBXYw2_t1PxUuRGO2XM92KYd3NhcnvCXm3tBn-oRPguQr5ByHTry7opZthdlTiO0YQZrIqqJAvn95vFOv5ODsVZfJyHPMTCiXBwVtT_hatMfuXv_3h1H6tIO1yPhF2e53JfNIXjUuf_DTBL5uTlU5gl_ppOJBOyZQg9jdJ_3Yl15_eEXd36XOLYfro9LlAlVhnymkI1017PjaT1nY_Q3gTXPY0LJtvWM36aBQlTj38Lin0TX9ije_Gxl2GsbkAbQ"
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
atletas_ids = atletas_df[
    atletas_df['posicao'] != 'Técnico'
    ]['atleta_id'].unique()
with ThreadPoolExecutor(max_workers=8) as executor:
    list__pontuacao_df = list(
        tqdm(executor.map(fetch__pontuacao_atletas__rodada, atletas_ids),
        total=len(atletas_ids), desc="Pontuação atletas"))
pontuacoes_df = pd.concat(list__pontuacao_df)

# Fillna
pontuacoes_df[
    pontuacoes_df.filter(like='scout_', axis=1).columns
] = pontuacoes_df[
    pontuacoes_df.filter(like='scout_', axis=1).columns
].fillna(0.0)

# Merge final
database = pontuacoes_df.merge(
    atletas_df, on='atleta_id', how='left').merge(
        partidas_df, on=['rodada_id', 'clube_id'],
        how='outer'
    )

# Export
database.to_excel(f"dados__rodada_{RODADA}.xlsx", index=False)
