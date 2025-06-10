"""Predict price valorization."""
import argparse
import pandas as pd
import joblib
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler

# Parse args
parser = argparse.ArgumentParser()
parser.add_argument('--rodada', type=int, required=True, help='Rodada atual')
args = parser.parse_args()
RODADA = args.rodada
TEST_SPLIT = 0.2
OUTPUT_COL = 'preco_var'

# Read dataframe
database = pd.read_excel(f"../data/dados__rodada_{RODADA}.xlsx")

# Filter rows
database = database[
    (database['entrou_em_campo'] == 1.0) &
    (database['rodada_id'] != 1) &
    (database['rodada_id'] != RODADA)
].dropna()

# Filter columns
database = database.drop([
    'apelido', 'atleta_id', 'entrou_em_campo',
    'status', 'clube', 'rodada_id', 'clube_id',
    'clube_adversario_id'
    ], axis=1)

# Encode posicao
database = pd.get_dummies(database, columns=['posicao'])

# Convert boolean columns to int
boolean_cols = ['is_casa'] + [
    col for col in database.columns if col.startswith('posicao_')
]
database[boolean_cols] = database[boolean_cols].astype(int)

# Replace '-' in mpv
mpv_cols = [
    col for col in database.columns if col.startswith('mpv_')
]
pd.set_option('future.no_silent_downcasting', True)
database[mpv_cols] = (
    database[mpv_cols]
    .replace('-', 0.0)
)

# Min and max vlues
y_min = database[OUTPUT_COL].min()
y_max = database[OUTPUT_COL].max()

# Normalize
scaler = MinMaxScaler()
scaled_database = pd.DataFrame(
    scaler.fit_transform(database),
    columns=database.columns,
    index=database.index
)

# save scaler
joblib.dump(scaler, f"saved_scalers/rodada_{RODADA}.pkl")

# Split X and y
X = scaled_database.drop(
    ['preco', 'preco_var', 'pontos', 'pontos_var'],
    axis=1
)
y = scaled_database[OUTPUT_COL]

# Split train and test
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=TEST_SPLIT, random_state=42)

# Random forest model
model = RandomForestRegressor()
model.fit(X_train, y_train)
y_pred = model.predict(X_test)

# Descale
y_test = y_test * (y_max - y_min)
y_pred = y_pred * (y_max - y_min)
mse = mean_squared_error(y_test, y_pred)
mae = mean_absolute_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print(f"Resultados:\n  MSE: {mse}\n  MAE: {mae}\n  R^2: {r2}")

plt.figure(figsize=(6, 6))
plt.scatter(y_test, y_pred, alpha=0.5)
plt.plot(
    [min(y_test), max(y_test)], [min(y_test), max(y_test)],
    color='red',
    linestyle='--')
plt.xlabel('C$ Real')
plt.ylabel('C$ Predita')
plt.title('Predição de C$')
plt.show()

# Save model
joblib.dump(model, f"saved_models/rodada_{RODADA}.pkl")

# Predict 