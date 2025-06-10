"""Script for lineup optimization."""
import pandas as pd
from pulp import LpMaximize, LpProblem, LpVariable, lpSum, LpBinary


def optimize_lineup(
    database: pd.DataFrame,
    posicao_count: dict,
    cartoletas: float,
    output_col: str
):
    """Otimiza titulares e depois escolhe reservas mais baratos por posição."""
    opt_database = database.set_index('atleta_id')

    # Modelo para titulares
    prob = LpProblem("titulares_optimizer", LpMaximize)

    var_dict = {
        atleta_id: LpVariable(f"{atleta_id}", cat=LpBinary)
        for atleta_id in opt_database.index
    }

    # Objetivo: maximizar saida
    prob += lpSum(
        var_dict[i] * opt_database.loc[i, output_col]
        for i in opt_database.index
    )

    # Restrição de orçamento
    prob += lpSum(
        var_dict[i] * opt_database.loc[i, "preco"]
        for i in opt_database.index
    ) <= cartoletas

    # Restrição de quantidade por posição
    for pos, qtd in posicao_count.items():
        prob += lpSum(
            var_dict[i]
            for i in opt_database.index
            if opt_database.loc[i, "posicao"] == pos
        ) == qtd

    # Resolver
    prob.solve()

    # Selecionar titulares
    titulares = opt_database[[
        round(var_dict[i].value()) == 1 for i in opt_database.index
    ]]

    # Agora selecionar reservas: 1 por posição, com menor preço e que NÃO está entre os titulares
    reservas = []
    for pos in opt_database["posicao"].unique():
        if pos in posicao_count:
            candidatos = opt_database[
                (opt_database["posicao"] == pos) & (~opt_database.index.isin(titulares.index))
            ].copy()

            # Limitar aos que são mais baratos que o jogador mais barato dos titulares nessa posição
            preco_min_titular = titulares[titulares["posicao"] == pos]["preco"].min()
            candidatos = candidatos[candidatos["preco"] < preco_min_titular]

            if not candidatos.empty:
                reserva = candidatos.sort_values("preco").iloc[0]
                reservas.append(reserva)

    reservas = pd.DataFrame(reservas)

    # Organizar por posição
    titulares = titulares.sort_values("posicao")
    reservas = reservas.sort_values("posicao")

    return titulares, reservas
