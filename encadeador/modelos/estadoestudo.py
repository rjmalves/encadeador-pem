from enum import Enum


class EstadoEstudo(Enum):
    NAO_INICIADO = "NAO_INICIADO"
    EXECUTANDO = "EXECUTANDO"
    CONCLUIDO = "CONCLUIDO"
    ERRO = "ERRO"

    @staticmethod
    def factory(valor: str) -> "EstadoEstudo":
        for estado in EstadoEstudo:
            if estado.value == valor:
                return estado
        raise ValueError(f"Estado {valor} não suportado")
