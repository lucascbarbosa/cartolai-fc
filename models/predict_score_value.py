"""Predict price valorization."""
import pandas as pd
import joblib
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.preprocessing import MinMaxScaler
pd.set_option('future.no_silent_downcasting', True)

RODADA = 12
PONTOS_MODEL = LinearRegression()
PRECO_MODEL = LinearRegression()


#############
# Functions #
def process_database(database: pd.DataFrame):
    """Process database."""
    # Remove jogadores que não jogaram
    database = database[
        (database['entrou_em_campo'] == 1.0) |
        (database['rodada_id'] == RODADA)
    ]

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
    database[mpv_cols] = database[mpv_cols].replace('-', 0.0)

    # Remove unused columns
    scaled_database = database.drop(
        [
            'apelido', 'atleta_id', 'entrou_em_campo', 'status',
            'clube', 'clube_id', 'clube_adversario_id', 'rodada_id'
        ],
        axis=1
    )

    # Split databases
    train_database = scaled_database[database['rodada_id'].isin(range(2, 10))]
    eval_database = scaled_database[database['rodada_id'].isin(range(10, 12))]
    test_database = scaled_database[database['rodada_id'] == 12]

    # Scale
    scaler = MinMaxScaler()
    train_database = pd.DataFrame(
        scaler.fit_transform(train_database),
        columns=train_database.columns,
        index=train_database.index
    )
    eval_database = pd.DataFrame(
        scaler.transform(eval_database),
        columns=eval_database.columns,
        index=eval_database.index
    )
    test_database = pd.DataFrame(
        scaler.transform(test_database),
        columns=test_database.columns,
        index=test_database.index
    )

    # save scaler
    joblib.dump(scaler, f"saved_scalers/rodada_{RODADA}.pkl")

    return train_database, eval_database, test_database


def split_input_output(database: pd.DataFrame):
    """Split in input and output."""
    X = database.drop(['pontos'], axis=1)
    y = database['pontos']
    return X, y


# Read dataframe
database = pd.read_excel(f"../data/dados__rodada_{RODADA}.xlsx")
y_min = database['pontos'].min()
y_max = database['pontos'].max()

# Process database
(
    train_database,
    eval_database,
    test_database
) = database.pipe(process_database)

# Split X and y
X_train, y_train = train_database.pipe(split_input_output)
X_eval, y_eval = eval_database.pipe(split_input_output)
X_test, y_test = test_database.pipe(split_input_output)

# Fit model
PONTOS_MODEL.fit(X_train, y_train)

# Evaluate
y_pred = (PONTOS_MODEL.predict(X_eval) * (y_max - y_min) + y_min).round(1)
y_eval = (y_eval * (y_max - y_min) + y_min).round(1)
mse = mean_squared_error(y_eval, y_pred)
r2 = r2_score(y_eval, y_pred)
print(f"MSE: {mse:.4f} R2: {r2:.4f}")

# Save model
joblib.dump(PONTOS_MODEL, f"saved_models/pontos__rodada_{RODADA}.pkl")

# Predict pontos
y_pred = PONTOS_MODEL.predict(X_test)
y_pred = (PONTOS_MODEL.predict(X_test) * (y_max - y_min) + y_min).round(1)
database.loc[database['rodada_id'] == RODADA, 'pontos'] = y_pred
database.to_excel(f"../data/dados__rodada_{RODADA}.xlsx", index=False)
