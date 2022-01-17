from enum import Enum, auto


class TransicaoCaso(Enum):
    INICIOU = auto()
    SUCESSO = auto()
    ERRO = auto()
    ERRO_MAX_FLEX = auto()
    ERRO_DADOS = auto()
