"""Predict price valorization."""
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.preprocessing import MinMaxScaler
pd.set_option('future.no_silent_downcasting', True)

RODADA = 12
PONTOS_MODEL = LinearRegression()
PRECO_MODEL = LinearRegression()


#############
# Functions #
def process_pontuacao_database(database: pd.DataFrame):
    """Process database for pontuacao prediction."""
    database = database.copy()
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

    return train_database, eval_database, test_database


def process_preco_database(database: pd.DataFrame):
    """Process database for preco prediction."""
    database = database.copy()
    # Remove jogadores que não jogaram
    database = database[
        (database['entrou_em_campo'] == 1.0) |
        (database['rodada_id'] == RODADA)
    ]

    # Replace '-' in mpv
    database['mpv_mean'] = database['mpv_mean'].replace('-', 0.0)
    database['mpv_prev'] = database['mpv_prev'].replace('-', 0.0)
    database['mpv'] = database['mpv'].replace('-', 0.0)

    # Create used columns
    database.loc[:, 'pontos_var'] = (
        database.loc[:, 'pontos'] - database.loc[:, 'mpv_prev']
    ).copy()
    database.loc[:, 'preco_var'] = (
        database.loc[:, 'preco'] - database.loc[:, 'preco_prev']
    )
    scaled_database = database[['pontos_var', 'preco_var']]

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

    return train_database, eval_database, test_database


def _split_pontuacao_data(database: pd.DataFrame):
    X = database.drop(
        ['mpv', 'mpv_prev', 'mpv_mean', 'preco', 'pontos'],
        axis=1
    )
    print(database.corr()['pontos'])
    y = database['pontos']
    return X, y


def _split_preco_data(database: pd.DataFrame):
    X = database['pontos_var'].to_frame()
    y = database['preco_var']
    return X, y


# Read dataframe
database = pd.read_excel(f"../data/dados__rodada_{RODADA}.xlsx")

# Process database
(
    train_database,
    eval_database,
    test_database
) = database.pipe(process_pontuacao_database)

##########
# Pontos #
# Split X and y
y_min = database['pontos'].min()
y_max = database['pontos'].max()
X_train, y_train = _split_pontuacao_data(train_database)
X_eval, y_eval = _split_pontuacao_data(eval_database)
X_test, y_test = _split_pontuacao_data(test_database)

# Fit
PONTOS_MODEL.fit(X_train, y_train)

# Evaluate
y_pred = (PONTOS_MODEL.predict(X_eval) * (y_max - y_min) + y_min).round(1)
y_eval = (y_eval * (y_max - y_min) + y_min).round(1)
mse = mean_squared_error(y_eval, y_pred)
r2 = r2_score(y_eval, y_pred)
print(f"> PONTOS: MSE: {mse:.4f} R2: {r2:.4f}")

# Predict
y_pred = PONTOS_MODEL.predict(X_test)
y_pred = (PONTOS_MODEL.predict(X_test) * (y_max - y_min) + y_min).round(1)
database.loc[database['rodada_id'] == RODADA, 'pontos'] = y_pred

# ##########
# # Preco #
# # Process database
# (
#     train_database,
#     eval_database,
#     test_database
# ) = database.pipe(process_preco_database)

# # Split X and y
# X_train, y_train = _split_preco_data(train_database)
# X_eval, y_eval = _split_preco_data(eval_database)
# X_test, y_test = _split_preco_data(test_database)

# # Fit
# PRECO_MODEL.fit(X_train, y_train)

# # Evaluate
# y_pred = (PRECO_MODEL.predict(X_eval) * (y_max - y_min) + y_min).round(1)
# y_eval = (y_eval * (y_max - y_min) + y_min).round(1)
# mse = mean_squared_error(y_eval, y_pred)
# r2 = r2_score(y_eval, y_pred)
# print(f"> PREÇO: MSE: {mse:.4f} R2: {r2:.4f}")

# # Predict
# y_pred = PRECO_MODEL.predict(X_test)
# y_pred = (PRECO_MODEL.predict(X_test) * (y_max - y_min) + y_min).round(1)
# database.loc[database['rodada_id'] == RODADA, 'preco'] = y_pred

# # Valorizacao
# database['valorizacao'] = database['preco'] - database['preco_mean']

# # Save excel
# database.to_excel(f"../results/dados__rodada_{RODADA}.xlsx", index=False)

