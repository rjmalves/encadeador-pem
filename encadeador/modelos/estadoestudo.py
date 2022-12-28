from enum import Enum


class EstadoEstudo(Enum):
    NAO_INICIADO = 1
    INICIADO = 2
    EXECUTANDO = 3
    CONCLUIDO = 4
    ERRO = 5

    @staticmethod
    def factory(valor: int) -> "EstadoEstudo":
        for estado in EstadoEstudo:
            if estado.value == valor:
                return estado
        raise ValueError(f"Estado {valor} não suportado")
