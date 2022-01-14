from enum import Enum


class EstadoCaso(Enum):
    NAO_INICIADO = "NAO_INICIADO"
    PREPARANDO = "PREPARANDO"
    ENCADEANDO = "ENCADEANDO"
    ESPERANDO = "ESPERANDO"
    EXECUTANDO = "EXECUTANDO"
    FLEXIBILIZANDO = "FLEXIBILIZANDO"
    RETRY = "RETRY"
    SUCESSO = "SUCESSO"
    ERRO_MAX_FLEX = "ERRO_MAX_FLEX"
    ERRO_DADOS = "ERRO_DADOS"
    ERRO_COMUNICACAO = "ERRO_COMUNICACAO"

    @staticmethod
    def factory(valor: str) -> 'EstadoCaso':
        for estado in EstadoCaso:
            if estado.value == valor:
                return estado
        raise ValueError(f"Estado {valor} não suportado")
