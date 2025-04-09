import argparse
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument(
    '--rodada',
    type=str,
    required=False,
    default="4-3-3",
    choices=["4-3-3", "4-4-2", "3-5-2", "3-4-3", "4-5-1", "5-3-2", "5-4-1"],
    help='Rodada atual')
args = parser.parse_args()
RODADA_ATUAL = args.rodada
