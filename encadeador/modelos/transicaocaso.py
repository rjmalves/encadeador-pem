from enum import Enum, auto


class TransicaoCaso(Enum):
    CRIADO = auto()
    INICIALIZADO = auto()
    PREPARA_EXECUCAO_SOLICITADA = auto()
    PREPARA_EXECUCAO_SUCESSO = auto()
    PREPARA_EXECUCAO_ERRO = auto()
    INICIO_EXECUCAO_SOLICITADA = auto()
    INICIO_EXECUCAO_SUCESSO = auto()
    INICIO_EXECUCAO_ERRO = auto()
    FLEXIBILIZACAO_SOLICITADA = auto()
    FLEXIBILIZACAO_SUCESSO = auto()
    FLEXIBILIZACAO_ERRO = auto()
    CONCLUIDO = auto()
    INVIAVEL = auto()
    ERRO = auto()
    ERRO_MAX_FLEX = auto()
    ERRO_DADOS = auto()
