import subprocess
import time
from os import listdir
from os.path import isfile
from typing import List, Tuple, Optional


NUM_RETRY_DEFAULT = 3
TIMEOUT_DEFAULT = 10


def executa_terminal_retry(
    cmds: List[str], num_retry: int = NUM_RETRY_DEFAULT, timeout: float = TIMEOUT_DEFAULT
) -> Tuple[int, List[str]]:
    """
    Executa um comando no terminal e obtém as saídas e o código
    retornado pelo comando. Caso ocorram falhas, possui um
    retry de um número especificado de vezes.

    :param cmds: Partes do comando que serão unidas em uma única
        string para execução.
    :param retry: Número máximo de novas tentativas.
    :return: O código e as linhas de saída do comando.
    :rtype: Tuple[int, List[str]]
    """
    for _ in range(num_retry):
        cod, saidas = executa_terminal(cmds, timeout)
        if cod == 0:
            return cod, saidas
    return -1, []


def executa_terminal(
    cmds: List[str], timeout: float = TIMEOUT_DEFAULT
) -> Tuple[Optional[int], List[str]]:
    """
    Executa um comando no terminal e obtém as saídas e o código
    retornado pelo comando.

    :param cmds: Partes do comando que serão unidas em uma única
        string para execução.
    :param timeout: Tempo máximo para aguardar a saída do comando
        em segundos.
    :return: O código e as linhas de saída do comando.
    :rtype: Tuple[int, List[str]]
    """
    cmd = " ".join(cmds)
    processo = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, shell=True, universal_newlines=True
    )
    t_inicio = time.time()
    linhas_saida: List[str] = []
    while True:
        std = processo.stdout
        if std is None:
            raise ValueError("Erro no subprocesso")
        saida = std.readline()
        linhas_saida.append(saida.strip())
        codigo = processo.poll()
        if codigo is not None:
            for saida in std.readlines():
                linhas_saida.append(saida.strip())
            break
        t_atual = time.time()
        if t_atual - t_inicio > timeout:
            break
        time.sleep(0.5)
    processo.terminate()
    return codigo, linhas_saida


def converte_codificacao(caminho: str, script_converte: str):
    arqs = [
        a
        for a in listdir(caminho)
        if (".dat" in a or "dadger.rv" in a) and isfile(a)
    ]
    for a in arqs:
        _, out = executa_terminal([f"file -i {a}"])
        cod = out[0].split("charset=")[1].strip()
        if all([cod != "utf-8", cod != "us-ascii", cod != "binary"]):
            cod = cod.upper()
            c, _ = executa_terminal([f"{script_converte}" + f" {a} {cod}"])
