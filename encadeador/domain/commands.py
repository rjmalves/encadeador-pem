from dataclasses import dataclass
from typing import List
from encadeador.modelos.caso2 import Caso

from encadeador.modelos.regrareservatorio import RegraReservatorio


class Command:
    pass


@dataclass
class SubmeteJob(Command):
    gerenciador: str
    caminho: str
    numero_processadores: int
    id_caso: int


@dataclass
class MonitoraJob(Command):
    gerenciador: str
    id: int


@dataclass
class DeletaJob(Command):
    gerenciador: str
    id: int


@dataclass
class CriaCaso(Command):
    caminho: str
    id_estudo: int


@dataclass
class InicializaCaso(Command):
    id_caso: str


@dataclass
class PreparaCaso(Command):
    id_caso: str
    # TODO - não passar mais por aqui. Está ruim.
    regras_reservatorios: List[RegraReservatorio]


@dataclass
class SubmeteCaso(Command):
    id_caso: str
    numero_processadores: int
    gerenciador: str
