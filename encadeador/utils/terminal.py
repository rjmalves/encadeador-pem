import subprocess
import time
from typing import List, Tuple


def executa_terminal(cmds: List[str],
                     timeout: float = 10) -> Tuple[int,
                                                   List[str]]:
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
    processo = subprocess.Popen(cmd,
                                stdout=subprocess.PIPE,
                                shell=True,
                                universal_newlines=True)
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
            raise TimeoutError(f"Timeout para resposta do comando: {cmd}")
        time.sleep(0.5)
    processo.terminate()
    return codigo, linhas_saida
