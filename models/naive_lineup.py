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
parser.add_argument(
    '--cartoletas',
    type=float,
    required=True,
    help='Total de cartoletas disponíveis'
)
args = parser.parse_args()
RODADA = args.rodada
ESQUEMA = args.esquema
CARTOLETAS = args.cartoletas


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

    return {
        'Técnico': 2,
        'Goleiro': 2,
        'Zagueiro': num_zag + 1,
        'Lateral': num_lat + 1,
        'Meia': num_mei + 1,
        'Atacante': num_ata + 1
    }


# Get position count based on ESQUEMA
posicao_count = get_position_count()

########
# Data #
# Read dataframe andn filter columns
database = pd.read_excel(f"../data/dados__rodada_{RODADA}.xlsx")

# Filter last round
database = database[database['rodada_id'] == 2]

# Filter columns
database = database[[
    'atleta_id', 'apelido', 'clube', 'posicao', 'score', 'preco']]
