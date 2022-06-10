from enum import Enum, auto


class TransicaoEstudo(Enum):
    CRIADO = auto()
    INICIO_SOLICITADO = auto()
    INICIO_SUCESSO = auto()
    INICIO_ERRO = auto()
    ERRO = auto()
