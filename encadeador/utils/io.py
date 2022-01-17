from typing import Dict, Any
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
