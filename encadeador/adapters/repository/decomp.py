from abc import ABC, abstractmethod
from typing import Dict, Type
import pathlib
from os.path import join
from idecomp.decomp.caso import Caso as ArquivoCaso
from idecomp.decomp.arquivos import Arquivos
from idecomp.decomp.dadger import Dadger
from idecomp.decomp.dadgnl import Dadgnl
from idecomp.decomp.hidr import Hidr
from idecomp.decomp.inviabunic import InviabUnic
from idecomp.decomp.relato import Relato
from idecomp.decomp.relgnl import Relgnl
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.utils.encoding import converte_codificacao


class AbstractDecompRepository(ABC):
    @property
    @abstractmethod
    def arquivos(self) -> Arquivos:
        raise NotImplementedError

    @abstractmethod
    async def get_dadger(self) -> Dadger:
        raise NotImplementedError

    @abstractmethod
    def set_dadger(self, d: Dadger):
        raise NotImplementedError

    @abstractmethod
    def get_dadgnl(self) -> Dadgnl:
        raise NotImplementedError

    @abstractmethod
    def set_dadgnl(self, d: Dadgnl):
        raise NotImplementedError

    @abstractmethod
    def get_inviab(self) -> InviabUnic:
        raise NotImplementedError

    @abstractmethod
    def get_relato(self) -> Relato:
        raise NotImplementedError

    @abstractmethod
    def get_relgnl(self) -> Relgnl:
        raise NotImplementedError

    @abstractmethod
    def get_hidr(self) -> Hidr:
        raise NotImplementedError


class FSDecompRepository(AbstractDecompRepository):
    def __init__(self, path: str):
        self.__path = path
        self.__caso = ArquivoCaso.read(join(self.__path, "caso.dat"))
        self.__arquivos = Arquivos.read(
            join(self.__path, self.__caso.arquivos)
        )

    @property
    def caminho(self) -> str:
        return self.__path

    @property
    def arquivos(self) -> Arquivos:
        return self.__arquivos

    async def get_dadger(self) -> Dadger:
        arq = self.arquivos.dadger
        if arq is None:
            raise FileNotFoundError("Nome do arquivo dadger não especificado")

        caminho = pathlib.Path(self.__path).joinpath(arq)
        await converte_codificacao(
            str(caminho), Configuracoes().script_converte_codificacao
        )
        return Dadger.read(str(caminho))

    def get_dadgnl(self) -> Dadgnl:
        arq = self.arquivos.dadgnl
        if arq is None:
            raise FileNotFoundError("Nome do arquivo dadgnl não especificado")

        return Dadgnl.read(join(self.__path, arq))

    def get_hidr(self) -> Hidr:
        arq = self.arquivos.hidr
        if arq is None:
            raise FileNotFoundError("Nome do arquivo hidr não especificado")

        return Hidr.read(join(self.__path, arq))

    def set_dadger(self, d: Dadger):
        arq = self.arquivos.dadger
        if arq is None:
            raise FileNotFoundError("Nome do arquivo dadger não especificado")

        d.write(join(self.__path, arq))

    def set_dadgnl(self, d: Dadgnl):
        arq = self.arquivos.dadgnl
        if arq is None:
            raise FileNotFoundError("Nome do arquivo dadgnl não especificado")

        d.write(join(self.__path, self.__arquivos.dadgnl))

    def get_inviab(self) -> InviabUnic:
        return InviabUnic.read(
            join(self.__path, f"inviab_unic.{self.__caso.arquivos}")
        )

    def get_relato(self) -> Relato:
        return Relato.read(join(self.__path, f"relato.{self.__caso.arquivos}"))

    def get_relgnl(self) -> Relgnl:
        return Relgnl.read(join(self.__path, f"relgnl.{self.__caso.arquivos}"))


def factory(kind: str, *args, **kwargs) -> AbstractDecompRepository:
    mappings: Dict[str, Type[AbstractDecompRepository]] = {
        "FS": FSDecompRepository,
    }
    return mappings[kind](*args, **kwargs)
