from enum import Enum, auto


class TransicaoJob(Enum):
    ENTRADA_FILA = auto()
    INICIO_EXECUCAO = auto()
    FIM_EXECUCAO = auto()
    TIMEOUT_EXECUCAO = auto()
    COMANDO_DELETA_JOB = auto()
    JOB_DELETADO = auto()
    ERRO_DELECAO = auto()
