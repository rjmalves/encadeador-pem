from enum import Enum


class EstadoCaso(Enum):
    NAO_INICIADO = "NAO_INICIADO"
    INICIADO = "INICIADO"
    PREPARANDO = "PREPARANDO"
    ERRO_PREPARACAO = "ERRO_PREPARACAO"
    PREPARADO = "PREPARADO"
    INICIANDO_EXECUCAO = "INICIANDO_EXECUCAO"
    ERRO_EXECUCAO = "ERRO_EXECUCAO"
    ESPERANDO_FILA = "ESPERANDO_FILA"
    EXECUTANDO = "EXECUTANDO"
    CONCLUIDO = "CONCLUIDO"
    INVIAVEL = "INVIAVEL"
    FLEXIBILIZANDO = "FLEXIBILIZANDO"
    ERRO_MAX_FLEX = "ERRO_MAX_FLEX"
    ERRO_DADOS = "ERRO_DADOS"
    ERRO_COMUNICACAO = "ERRO_COMUNICACAO"
    ERRO = "ERRO"

    @staticmethod
    def factory(valor: str) -> "EstadoCaso":
        for estado in EstadoCaso:
            if estado.value == valor:
                return estado
        raise ValueError(f"Estado {valor} não suportado")
