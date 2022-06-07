from enum import Enum, auto


class TransicaoCaso(Enum):
    INICIOU = auto()
    AGUARDANDO_SUBMISSAO = auto()
    SUCESSO = auto()
    INVIAVEL = auto()
    ERRO = auto()
    ERRO_MAX_FLEX = auto()
    ERRO_DADOS = auto()
