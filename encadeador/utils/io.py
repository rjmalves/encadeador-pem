from typing import Dict, Any
import pandas as pd  # type: ignore
import time
import json

MAX_RETRY = 5
INTERVALO_RETRY = 0.1


def le_arquivo_json(caminho: str) -> Dict[str, Any]:
    num_retry = 0
    while num_retry < MAX_RETRY:
        try:
            with open(caminho, "r") as c:
                return json.load(c)
        except OSError:
            num_retry += 1
            time.sleep(INTERVALO_RETRY)
            continue
        except BlockingIOError:
            num_retry += 1
            time.sleep(INTERVALO_RETRY)
            continue


def escreve_arquivo_json(caminho: str, dados: dict):
    num_retry = 0
    while num_retry < MAX_RETRY:
        try:
            with open(caminho, "w") as c:
                json.dump(dados, c)
            return
        except OSError:
            num_retry += 1
            time.sleep(INTERVALO_RETRY)
            continue
        except BlockingIOError:
            num_retry += 1
            time.sleep(INTERVALO_RETRY)
            continue


def le_df_de_csv(caminho: str) -> pd.DataFrame:
    num_retry = 0
    while num_retry < MAX_RETRY:
        try:
            return pd.read_csv(caminho, index_col=0)
        except OSError:
            num_retry += 1
            time.sleep(INTERVALO_RETRY)
            continue
        except BlockingIOError:
            num_retry += 1
            time.sleep(INTERVALO_RETRY)
            continue


def escreve_df_em_csv(df: pd.DataFrame, caminho: str):
    num_retry = 0
    while num_retry < MAX_RETRY:
        try:
            df.to_csv(caminho, encoding="utf-8")
            return
        except OSError:
            num_retry += 1
            time.sleep(INTERVALO_RETRY)
            continue
        except BlockingIOError:
            num_retry += 1
            time.sleep(INTERVALO_RETRY)
            continue
