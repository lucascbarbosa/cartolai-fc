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

# Convert is_casa to int
database['is_casa'] = database['is_casa'].astype(int)

# Replace '-' in mpv
database['mpv'] = database['mpv'].replace('-', 0.0).astype(float)

# Order columns
database = database[[
    'mpv', 'preco', 'preco_var', 'scout_FS', 'scout_DS',
    'scout_G', 'scout_A', 'scout_FT', 'scout_FD', 'scout_FF', 'scout_SG',
    'scout_DE', 'scout_DP', 'scout_PS', 'scout_PP', 'scout_PC', 'scout_I',
    'scout_GC', 'scout_GS', 'scout_FC', 'scout_CA', 'scout_CV',
    'pontos_var', 'pontos_mean', 'clube_aproveitamento',
    'clube_adversario_aproveitamento', 'clube_posicao',
    'clube_adversario_posicao', 'is_casa', 'posicao_Atacante',
    'posicao_Goleiro', 'posicao_Lateral', 'posicao_Meia',
    'posicao_Zagueiro', 'pontos'
]]

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
X = scaled_database.drop(['pontos'], axis=1)
y = scaled_database['pontos']

# Split train and test
TEST_SPLIT = 0.2
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=TEST_SPLIT, random_state=42)

# Random forest model
model = RandomForestRegressor()
model.fit(X_train, y_train)
y_pred = model.predict(X_test)
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
plt.xlabel('Valorização Real')
plt.ylabel('Valorização predita')
plt.title('Predição de Valorização')
plt.grid(True)
plt.show()

# Save model
joblib.dump(model, f"saved_models/rodada_{RODADA}.pkl")
