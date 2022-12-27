from enum import Enum, auto


class TransicaoRodada(Enum):
    SUBMETIDA = auto()
    EXECUTANDO = auto()
    SUCESSO = auto()
    ERRO_DADOS = auto()
    ERRO_EXECUCAO = auto()
    ERRO_COMUNICACAO = auto()
    INVIAVEL = auto()
