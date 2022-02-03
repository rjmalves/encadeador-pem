from enum import Enum


class EstadoJob(Enum):
    NAO_INICIADO = "NAO_INICIADO"
    ESPERANDO = "ESPERANDO"
    EXECUTANDO = "EXECUTANDO"
    DELETANDO = "DELETANDO"
    TIMEOUT = "TIMEOUT"
    ERRO = "ERRO"
    FINALIZADO = "FINALIZADO"

    @staticmethod
    def factory(valor: str) -> "EstadoJob":
        for estado in EstadoJob:
            if estado.value == valor:
                return estado
        raise ValueError(f"Estado {valor} não suportado")
