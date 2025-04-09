"""Naive lineup based on score."""
import argparse
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler

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

# Read dataframe
database = pd.read_excel(f"../data/dados__rodada_{RODADA}.xlsx")

# Filter rows
database = database[
    (database['entrou_em_campo']) & (database['status'] == 'Provável')]

# Filter columns
database = database.drop([
    'apelido', 'atleta_id', 'entrou_em_campo',
    'status', 'clube', 'rodada_id'], axis=1)

# Encode posicao
database = pd.get_dummies(database, columns=['posicao'])

# Normalize
scaler = MinMaxScaler()
scaled_database = pd.DataFrame(
    scaler.fit_transform(database),
    columns=database.columns,
    index=database.index
)

# Split X and y
X = scaled_database.drop(['media'], axis=1)
y = scaled_database['media']

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

print(f"Resultados:\n-MSE: {mse}\n-MAE: {mae}\n-R^2: {r2}")