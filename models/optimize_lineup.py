"""Naive lineup based on score."""
import pandas as pd
from aux.optimizer import optimize_lineup

# Parse args
RODADA = 12
ESQUEMA = "4-3-3"
CARTOLETAS = 137


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
# Read dataframe
database = pd.read_excel(f"../results/dados__rodada_{RODADA}.xlsx")

# Filter last round
database = database[
    (database['rodada_id'] == RODADA) &
    (database['status'] == 'Provável')
]

# Filter columns
database = database.drop(
    ['rodada_id', 'clube_id', 'entrou_em_campo', 'status'],
    axis=1)

# Escalar time
titulares, reservas = optimize_lineup(
    database, posicao_count, CARTOLETAS, 'valorizacao')

print("# TITULARES")
for posicao in titulares['posicao'].unique():
    grupo = titulares[titulares['posicao'] == posicao]
    print(f"\n ## {posicao}")
    for _, row in grupo.iterrows():
        nome = row['apelido']
        clube = row['clube']
        preco = row['preco']
        pontos = row['pontos']
        valorizacao = row['valorizacao']
        print(
            f"- {nome} ({clube}) – C${preco:.2f} - {pontos:.2f} pontos (C${valorizacao:.2f})")

print("\n\n# RESERVAS\n")
for posicao in reservas['posicao'].unique():
    grupo = reservas[reservas['posicao'] == posicao]
    print(f"\n ## {posicao}")
    for _, row in grupo.iterrows():
        nome = row['apelido']
        clube = row['clube']
        preco = row['preco']
        print(
            f"- {nome} ({clube}) – C${preco:.2f} - {pontos:.2f} pontos (C${valorizacao:.2f})")


total_preco = titulares['preco'].sum()
print(f"\n\n# TOTAL: C${total_preco:.2f}")
