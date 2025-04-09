"""Script to feature engineering."""
import argparse
import ast
import pandas as pd

# parser = argparse.ArgumentParser()
# parser.add_argument('--rodada', type=int, required=True, help='Rodada atual')
# args = parser.parse_args()
# RODADA_ATUAL = args.rodada
RODADA_ATUAL = 3

dados_silver = pd.read_excel(f'dados_bronze__rodada_{RODADA_ATUAL}.xlsx')

# Expand scout
dados_silver['scout'] = dados_silver[
    'scout'].apply(ast.literal_eval)
scout_df = pd.json_normalize(dados_silver['scout']).fillna(0)
scout_df.columns = ['scout_' + c for c in scout_df.columns]
dados_silver[scout_df.columns] = scout_df

# Expand minimos
dados_silver['minimos_para_valorizar'] = dados_silver[
    'minimos_para_valorizar'].apply(ast.literal_eval)
minimos_df = pd.json_normalize(dados_silver['minimos_para_valorizar'])
minimos_df.columns = ['minval_' + c for c in minimos_df.columns]
dados_silver[minimos_df.columns] = minimos_df

# Crie score unico
dados_silver['score'] = (
    dados_silver['media'] -
    dados_silver['minval_1']
    ) / (0.5 * dados_silver['preco'])

# Drop columns
dados_silver = dados_silver.drop(
    ['minimos_para_valorizar', 'scout'],
    axis=1
)

# Export
dados_silver.to_excel(f"dados_silver__rodada_{RODADA_ATUAL}.xlsx", index=False)
