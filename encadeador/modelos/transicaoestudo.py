from enum import Enum, auto


class TransicaoEstudo(Enum):
    PREPARA_EXECUCAO_SOLICITADA = auto()
    PREPARA_EXECUCAO_SUCESSO = auto()
    PREPARA_EXECUCAO_ERRO = auto()
    INICIO_EXECUCAO_SOLICITADA = auto()
    INICIO_EXECUCAO_SUCESSO = auto()
    INICIO_EXECUCAO_ERRO = auto()
    INICIO_PROXIMO_CASO = auto()
    CONCLUIDO = auto()
    ERRO = auto()
