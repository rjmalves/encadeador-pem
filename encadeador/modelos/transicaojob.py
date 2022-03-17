from enum import Enum, auto


class TransicaoJob(Enum):
    ENTRADA_FILA = auto()
    COMANDO_DELETA_JOB = auto()
    JOB_DELETADO = auto()
    INICIO_EXECUCAO = auto()
    FIM_EXECUCAO = auto()
    ERRO_DELECAO = auto()
    ERRO_EXECUCAO = auto()
