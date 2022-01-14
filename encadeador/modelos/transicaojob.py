from enum import Enum, auto


class TransicaoJob(Enum):
    NAO_INICIADO = auto()
    PREPARANDO = auto()
    ENCADEANDO = auto()
    ESPERANDO = auto()
    EXECUTANDO = auto()
    FLEXIBILIZANDO = auto()
    RETRY = auto()
    SUCESSO = auto()
    ERRO_MAX_FLEX = auto()
    ERRO_DADOS = auto()
    ERRO_COMUNICACAO = auto()
