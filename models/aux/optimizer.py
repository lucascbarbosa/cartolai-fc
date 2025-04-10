"""Optimizer script."""
import pandas as pd
from pulp import LpMaximize, LpProblem, LpVariable, lpSum, LpBinary


def optimize_lineup(
    database: pd.DataFrame,
    posicao_count: dict,
    cartoletas: float,):
    """Function that optimize lineup."""
    database = database.set_index('atleta_id')

    # Modelo de otimização
    prob = LpProblem("lineup_optimizer", LpMaximize)

    var_dict = {
        atleta_id: LpVariable(f"{atleta_id}", cat=LpBinary)
        for atleta_id in database.index
    }

    # Objetivo: maximizar o score total
    prob += lpSum(
        var_dict[atleta_id] * database.loc[atleta_id, "score"]
        for atleta_id in database.index
    )

    # Restrição: total de cartoletas
    prob += lpSum(
        var_dict[i] * database.loc[i, "preco"] for i in database.index
        ) <= cartoletas

    # Restrições de posição
    for pos, qtd in posicao_count.items():
        prob += lpSum(
            var_dict[i] for i in database.index
            if database.loc[i, "posicao"] == pos
        ) == qtd

    # Resolver
    prob.solve()

    selecionados = database[[
        round(var_dict[i].value()) == 1 for i in database.index
    ]]

    selecionados = selecionados.sort_values(by='posicao')

    reservas = selecionados.loc[
        selecionados.groupby("posicao")["preco"].idxmin()]
    titulares = selecionados.drop(reservas.index)
    return selecionados.sort_values(by="posicao")


time_ideal = optimize_lineup()
print(time_ideal.to_string(index=False))
