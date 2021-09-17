from enum import Enum, auto

class EstadoJob(Enum):
    ESPERANDO = auto()
    EXECUTANDO = auto()
    DELETANDO = auto()
    ERRO = auto()
