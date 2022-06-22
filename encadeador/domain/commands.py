from dataclasses import dataclass


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
