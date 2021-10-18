from enum import Enum


class EstadoJob(Enum):
    NAO_INICIADO = "NAO INICIADO"
    ESPERANDO = "ESPERANDO"
    EXECUTANDO = "EXECUTANDO"
    DELETANDO = "DELETANDO"
    ERRO = "ERRO"
    CONCLUIDO = "CONCLUIDO"

    @staticmethod
    def factory(valor: str) -> 'EstadoJob':
        for estado in EstadoJob:
            if estado.value == valor:
                return estado
        raise ValueError(f"Estado {valor} não suportado")
