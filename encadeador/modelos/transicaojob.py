from enum import Enum, auto


class TransicaoJob(Enum):
    CRIADO = auto()
    SUBMISSAO_SOLICITADA = auto()
    SUBMISSAO_ERRO = auto()
    SUBMISSAO_SUCESSO = auto()
    DELECAO_SOLICITADA = auto()
    DELECAO_ERRO = auto()
    DELECAO_SUCESSO = auto()
    INICIO_EXECUCAO = auto()
    FIM_EXECUCAO = auto()
    TIMEOUT_EXECUCAO = auto()
