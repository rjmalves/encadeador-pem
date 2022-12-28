from dataclasses import dataclass
from typing import List
from encadeador.modelos.estadocaso import EstadoCaso
from encadeador.modelos.estadoestudo import EstadoEstudo
from encadeador.modelos.regrareservatorio import RegraReservatorio


class Command:
    pass


@dataclass
class CriaRodada(Command):
    programa: str
    versao: str
    caminho: str
    numero_processadores: int
    id_caso: int


@dataclass
class MonitoraRodada(Command):
    id: int


@dataclass
class DeletaRodada(Command):
    id: int


@dataclass
class CriaCaso(Command):
    caminho: str
    id_estudo: int


@dataclass
class InicializaCaso(Command):
    id_caso: int


@dataclass
class PreparaCaso(Command):
    id_caso: int
    # TODO - não passar mais por aqui. Está ruim.
    regras_reservatorios: List[RegraReservatorio]


@dataclass
class SubmeteCaso(Command):
    id_caso: int


@dataclass
class MonitoraCaso(Command):
    id_caso: int
    id_rodada: int


@dataclass
class AtualizaCaso(Command):
    id_caso: int
    estado: EstadoCaso


@dataclass
class AvaliaCaso(Command):
    id_caso: int


@dataclass
class FlexibilizaCaso(Command):
    id_caso: int
    max_flex: int


@dataclass
class CorrigeErroConvergenciaCaso(Command):
    id_caso: int


@dataclass
class FlexibilizaCriterioConvergenciaCaso(Command):
    id_caso: int


@dataclass
class SintetizaCaso(Command):
    id_caso: int
    comando: str


@dataclass
class CriaEstudo(Command):
    caminho: str
    nome: str


@dataclass
class InicializaEstudo(Command):
    id_estudo: int
    diretorios_casos: List[str]


@dataclass
class PreparaEstudo(Command):
    id_estudo: int


@dataclass
class MonitoraEstudo(Command):
    id_caso: int


@dataclass
class AtualizaEstudo(Command):
    id_estudo: int
    estado: EstadoEstudo


@dataclass
class SintetizaEstudo(Command):
    id_estudo: int
