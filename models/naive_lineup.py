"""Naive lineup based on score."""
import argparse
import pandas as pd

# Parse args
parser = argparse.ArgumentParser()
parser.add_argument(
    '--esquema',
    type=str,
    required=False,
    default="4-3-3",
    choices=["4-3-3", "4-4-2", "3-5-2", "3-4-3", "4-5-1", "5-3-2", "5-4-1"],
    help='Rodada atual')
parser.add_argument('--rodada', type=int, required=True, help='Rodada atual')
args = parser.parse_args()
RODADA = args.rodada
ESQUEMA = args.esquema


#############
# Functions #
def get_position_count() -> dict:
    """Convert tactical scheme to number of players per position."""
    defesa, num_mei, num_ata = map(int, ESQUEMA.split("-"))

    if defesa == 3:
        num_zag = 3
        num_lat = 0
    elif defesa == 4:
        num_zag = 2
        num_lat = 2
    elif defesa == 5:
        num_zag = 3
        num_lat = 2

    return num_zag, num_lat, num_mei, num_ata


def split_titular_reserva(data: pd.DataFrame):
    """Seleciona jogador mais barato para reserva."""
    sorted_data = data.sort_values('preco', ascending=True)
    reserva = sorted_data.iloc[-1, :]
    titulares = sorted_data.iloc[:-1, :]
    return titulares, reserva


# Read dataframe andn filter columns
database = pd.read_excel(f"../data/dados__rodada_{RODADA}.xlsx")
database = database[[
    'apelido', 'clube', 'posicao', 'status', 'media', 'rodada_id',
    'preco', 'score', 'minval_1', 'minval_2', 'minval_3'
]]

# Get position count based on ESQUEMA
num_zag, num_lat, num_mei, num_ata = get_position_count()

##########
# Lineup #
for posicao, n in zip(
    ['Técnico', 'Goleiro', 'Zagueiro', 'Lateral', 'Meia', 'Atacante'],
    [1, 2, num_zag + 1, num_lat + 1, num_mei + 1, num_ata + 1]):
    data_posicao = database[
        (database['posicao'] == posicao) & (database['rodada_id'] == RODADA)
        ].sort_values(
        'score', ascending=False).iloc[:n, :]

    if posicao != 'Técnico':
        titulares, reserva = split_titular_reserva(data_posicao)
        print(f"""{posicao.upper()}:\nTitulares:""")
        for i in range(len(titulares)):
            titular = titulares.iloc[i, :]
            print(f"- {titular['apelido']} ({titular['clube']})")
        print("Reservas:")
        print(f"- {reserva['apelido']} ({reserva['clube']})\n")
    else:
        titulares = data_posicao.iloc[0, :]
        print(f"""{posicao.upper()}:\nTitular:""")
        print(f"- {titulares['apelido']} ({titulares['clube']})\n")
