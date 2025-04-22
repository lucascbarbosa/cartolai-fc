"""Naive lineup based on score."""
import argparse
import joblib
import pandas as pd
from aux.optimizer import optimize_lineup

# Parse args
parser = argparse.ArgumentParser()
parser.add_argument(
    '--esquema',
    type=str,
    required=False,
    default="4-3-3",
    choices=["4-3-3", "4-4-2", "3-5-2", "3-4-3", "4-5-1", "5-3-2", "5-4-1"],
    help='Esquema tático')
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

# Load model
model = joblib.load(f"saved_models/modelo_valorizacao__rodada_{RODADA}.pkl")


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
        'Técnico': 1,
        'Goleiro': 1,
        'Zagueiro': num_zag,
        'Lateral': num_lat,
        'Meia': num_mei,
        'Atacante': num_ata
    }


# Get position count based on ESQUEMA
posicao_count = get_position_count()

########
# Data #
# Read dataframe andn filter columns
database = pd.read_excel(f"../data/dados__rodada_{RODADA}.xlsx")
database = database.dropna()

# Filter last round
database = database[database['rodada_id'] == RODADA]

# Filter columns
database = database.drop(['rodada_id', ''])

titulares, reservas = optimize_lineup(database, posicao_count, CARTOLETAS)

print("# TITULARES\n")
for posicao in titulares['posicao'].unique():
    grupo = titulares[titulares['posicao'] == posicao]
    print(f"\n ## {posicao}")
    for _, row in grupo.iterrows():
        nome = row['apelido']
        clube = row['clube']
        preco = row['preco']
        print(f"- {nome} ({clube}) – C$ {preco:.2f}")

print("\n\n# RESERVAS\n")
for posicao in reservas['posicao'].unique():
    grupo = reservas[reservas['posicao'] == posicao]
    print(f"\n ## {posicao}")
    for _, row in grupo.iterrows():
        nome = row['apelido']
        clube = row['clube']
        preco = row['preco']
        print(f"- {nome} ({clube}) – C$ {preco:.2f}")


total_preco = titulares['preco'].sum()
print(f"\n\n# TOTAL: C${total_preco:.2f}")
