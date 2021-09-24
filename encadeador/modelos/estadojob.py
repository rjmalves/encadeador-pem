from enum import Enum

class EstadoJob(Enum):
    ESPERANDO = "ESPERANDO"
    EXECUTANDO = "EXECUTANDO"
    DELETANDO = "DELETANDO"
    ERRO = "ERRO"
