from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from inewave.newave.caso import Caso as ArquivoCaso
from inewave.newave.arquivos import Arquivos
from inewave.newave.dger import DGer
from inewave.newave.cvar import CVAR

from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.caso2 import Caso
from encadeador.modelos.estadocaso import EstadoCaso
from encadeador.modelos.programa import Programa


class AbstractNewaveRepository(ABC):
    @abstractmethod
    @property
    def arquivos(self) -> Arquivos:
        raise NotImplementedError


class FSNewaveRepository(ABC):
    def __init__(self, path: str):
        self.__path = path
        self.__caso = ArquivoCaso.le_arquivo(self.__path)
        self.__arquivos = Arquivos.le_arquivo(
            self.__path, self.__caso.arquivos
        )

    @property
    def caminho(self) -> str:
        return self.__path

    @property
    def arquivos(self) -> Arquivos:
        return self.__arquivos

    def get_dger(self) -> DGer:
        return DGer.le_arquivo(self.__path, self.arquivos.dger)

    def set_dger(self, d: DGer):
        d.escreve_arquivo(self.__path, self.__arquivos.dger)

    def get_cvar(self) -> CVAR:
        return CVAR.le_arquivo(self.__path, self.arquivos.cvar)

    def set_cvar(self, d: CVAR):
        d.escreve_arquivo(self.__path, self.__arquivos.cvar)


# TODO
class S3NewaveRepository(ABC):
    def __init__(self, url: str):
        self.__url = url

    @property
    def arquivos(self) -> Arquivos:
        raise NotImplementedError


def factory(kind: str, *args, **kwargs) -> AbstractNewaveRepository:
    mappings: Dict[str, AbstractNewaveRepository] = {
        "FS": FSNewaveRepository,
        "S3": S3NewaveRepository,
    }
    return mappings[kind](*args, **kwargs)
